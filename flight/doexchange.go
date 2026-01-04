package flight

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hugr-lab/airport-go/catalog"
)

// DoExchange implements bidirectional streaming for function execution and DML operations.
// This is used by DuckDB Airport extension to execute scalar and table functions,
// as well as INSERT, UPDATE, and DELETE operations.
//
// Protocol:
// - Client sends batches of input data via stream
// - Server executes function/DML on input data
// - Server sends back result batches via stream
//
// For scalar functions, the implementation uses a pipeline with 3 stages running concurrently:
// 1. Reader goroutine: Reads input records from client stream
// 2. Processor goroutine: Executes scalar function on input records
// 3. Writer goroutine: Sends output records back to client stream
//
// For table functions (in/out), the function processes the entire input stream
// and returns an output RecordReader.
//
// For DML operations (insert, update, delete):
// - INSERT: Client sends data rows, server returns RETURNING data if requested
// - UPDATE: Client sends rowid + new column values, server returns RETURNING data if requested
// - DELETE: Client sends rowid values only, server returns RETURNING data if requested
//
// Headers:
// - airport-operation: "scalar_function", "table_function", "insert", "update", "delete"
// - airport-flight-path: "schema/table" or "schema/function"
// - return-chunks: "1" if RETURNING clause present, "0" otherwise
//
// References:
// - Scalar functions: https://airport.query.farm/scalar_functions.html
// - Table functions: https://airport.query.farm/table_returning_functions.html
// - INSERT: https://airport.query.farm/table_insert.html
// - UPDATE: https://airport.query.farm/table_update.html
// - DELETE: https://airport.query.farm/table_delete.html
func (s *Server) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	ctx := stream.Context()

	// Extract metadata from gRPC headers
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "missing metadata")
	}

	// Check for required headers
	operation := md.Get("airport-operation")
	if len(operation) == 0 {
		return status.Errorf(codes.InvalidArgument, "missing airport-operation header")
	}

	opType := operation[0]

	// Get return-chunks header (required for functions, optional for DML)
	returnChunks := md.Get("return-chunks")
	returnData := len(returnChunks) > 0 && returnChunks[0] == "1"

	// Get the flight path from gRPC metadata
	flightPath := md.Get("airport-flight-path")
	if len(flightPath) == 0 {
		return status.Errorf(codes.InvalidArgument, "missing airport-flight-path in metadata")
	}

	// Parse the flight path (format: "schema/table" or "schema/function")
	pathParts := strings.Split(flightPath[0], "/")
	if len(pathParts) != 2 {
		return status.Errorf(codes.InvalidArgument, "invalid flight path format: %s", flightPath[0])
	}

	schemaName := pathParts[0]
	targetName := pathParts[1] // table name or function name

	s.logger.Debug("DoExchange requested",
		"operation", opType,
		"return_chunks", returnData,
		"schema", schemaName,
		"target", targetName,
	)

	// Route to appropriate handler based on operation type
	switch opType {
	case "scalar_function":
		if !returnData {
			return status.Errorf(codes.InvalidArgument, "missing or invalid return-chunks header for scalar_function")
		}
		return s.handleScalarFunction(ctx, stream, schemaName, targetName)
	case "table_function", "table_function_in_out":
		if !returnData {
			return status.Errorf(codes.InvalidArgument, "missing or invalid return-chunks header for table_function")
		}
		// Both table functions and in/out table functions use the same handler
		// The handler detects the function type and handles accordingly
		return s.handleTableFunction(ctx, stream, schemaName, targetName)
	case "insert":
		return s.handleDoExchangeInsert(ctx, stream, schemaName, targetName, returnData)
	case "update":
		return s.handleDoExchangeUpdate(ctx, stream, schemaName, targetName, returnData)
	case "delete":
		return s.handleDoExchangeDelete(ctx, stream, schemaName, targetName, returnData)
	default:
		return status.Errorf(codes.InvalidArgument, "invalid airport-operation: %s (expected scalar_function, table_function, table_function_in_out, insert, update, or delete)", opType)
	}
}

// handleScalarFunction processes scalar function execution via DoExchange.
func (s *Server) handleScalarFunction(ctx context.Context, stream flight.FlightService_DoExchangeServer, schemaName, functionName string) error {
	// Look up schema
	schema, err := s.catalog.Schema(ctx, schemaName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema not found: %s", schemaName)
	}

	// Get scalar functions
	functions, err := schema.ScalarFunctions(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get scalar functions: %v", err)
	}

	// Find the requested function
	var targetFunc catalog.ScalarFunction
	for _, fn := range functions {
		if fn.Name() == functionName {
			targetFunc = fn
			break
		}
	}

	if targetFunc == nil {
		return status.Errorf(codes.NotFound, "scalar function not found: %s.%s", schemaName, functionName)
	}

	// Get the output schema from the function signature
	functionSignature := targetFunc.Signature()
	outputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: functionSignature.ReturnType},
	}, nil)

	s.logger.Debug("Scalar function output schema",
		"return_type", functionSignature.ReturnType.String(),
	)

	// read input schema message
	msg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to receive schema: %v", err)
	}

	s.logger.Debug("Received input schema for scalar function",
		"schema", msg,
	)

	// Get a record reader for input data
	reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(s.allocator))
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create record reader: %v", err)
	}

	inputSchema := reader.Schema()
	s.logger.Debug("Input schema for scalar function",
		"schema", inputSchema,
	)

	// Create a record writer for output data
	writer := NewSchemaWriter(stream, outputSchema, s.allocator, false)
	defer writer.Close()

	if err := writer.Begin(); err != nil {
		reader.Release()
		return status.Errorf(codes.Internal, "failed to send output schema: %v", err)
	}

	// Pipeline channels
	inputCh := make(chan arrow.RecordBatch, 1)
	processedCh := make(chan arrow.RecordBatch, 1)

	// Error group for managing goroutines
	eg, ctx := errgroup.WithContext(ctx)

	// Read data - run in separate goroutine not within errgroup to send errors to the client properly
	// The reader goroutine will close inputCh when done or stops when others send error status to
	// the client and connection will be closed.
	go func() error {
		defer close(inputCh)
		defer reader.Release()

		for reader.Next() {
			record := reader.RecordBatch()
			record.Retain() // Retain for passing to next stage

			s.logger.Debug("Received input batch",
				"num_rows", record.NumRows(),
				"num_cols", record.NumCols(),
			)

			select {
			case inputCh <- record:
			case <-ctx.Done():
				record.Release()
				return ctx.Err()
			}
		}
		if err := reader.Err(); err != nil && !errors.Is(err, io.EOF) {
			return status.Errorf(codes.Internal, "error reading input: %v", err)
		}
		return nil
	}()

	// Process data
	eg.Go(func() error {
		defer close(processedCh)
		batchCount := 0
		for in := range inputCh {
			batchCount++

			s.logger.Debug("Processing scalar function batch",
				"batch", batchCount,
				"rows", in.NumRows(),
				"columns", in.NumCols(),
			)

			// Save input row count before releasing
			inLen := in.NumRows()

			// Execute the scalar function (returns arrow.Array)
			res, err := targetFunc.Execute(ctx, in)
			in.Release()

			if err != nil {
				s.logger.Error("Function execution failed",
					"function", functionName,
					"batch", batchCount,
					"error", err,
				)
				return err
			}

			// Check for nil result
			if res == nil {
				return fmt.Errorf("function returned nil array")
			}

			// Validate output has same number of rows as input
			if res.Len() != int(inLen) {
				res.Release()
				return fmt.Errorf("output rows must match input rows, expected %d got %d", inLen, res.Len())
			}

			// Validate output array type matches function signature return type
			if !arrow.TypeEqual(res.DataType(), functionSignature.ReturnType) {
				actualType := res.DataType()
				res.Release()
				return fmt.Errorf("output array type mismatch: expected %s, got %s",
					functionSignature.ReturnType, actualType)
			}

			// Create RecordBatch with "result" field name
			out := array.NewRecordBatch(outputSchema, []arrow.Array{res}, inLen)
			res.Release() // RecordBatch retains the array

			// Send to writer stage
			select {
			case processedCh <- out:
			case <-ctx.Done():
				out.Release()
				return ctx.Err()
			}
		}

		s.logger.Debug("Processor completed")
		return nil
	})

	// Write data
	eg.Go(func() error {
		batchCount := 0
		for outputRecord := range processedCh {
			batchCount++

			if err := writer.Write(outputRecord); err != nil {
				outputRecord.Release()
				return fmt.Errorf("failed to write output batch: %w", err)
			}

			s.logger.Debug("Sent scalar function result",
				"batch", batchCount,
				"output_rows", outputRecord.NumRows(),
			)

			outputRecord.Release()
		}

		s.logger.Debug("Writer completed")
		return nil
	})

	// Wait for all stages to complete
	// Convert any error to gRPC status error here (only once)
	if err := eg.Wait(); err != nil {
		s.logger.Error("DoExchange pipeline failed",
			"function", functionName,
			"error", err,
		)
		return status.Errorf(codes.Internal, "scalar function `%s.%s` execution failed: %v", schemaName, functionName, err)
	}

	s.logger.Debug("DoExchange completed",
		"function", functionName,
	)

	return nil
}

// handleTableFunction processes table function execution via DoExchange.
// Table functions accept row sets as input and return transformed rows.
func (s *Server) handleTableFunction(ctx context.Context, stream flight.FlightService_DoExchangeServer, schemaName, functionName string) error {
	s.logger.Debug("Table function execution requested",
		"schema", schemaName,
		"function", functionName,
	)

	// Look up schema
	schema, err := s.catalog.Schema(ctx, schemaName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema not found: %s", schemaName)
	}

	// Get table functions with in/out support
	functions, err := schema.TableFunctionsInOut(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get table functions: %v", err)
	}

	// Find the requested function
	var targetFunc catalog.TableFunctionInOut
	for _, fn := range functions {
		if fn.Name() == functionName {
			targetFunc = fn
			break
		}
	}

	if targetFunc == nil {
		return status.Errorf(codes.NotFound, "table function not found: %s.%s", schemaName, functionName)
	}

	s.logger.Debug("Found table function",
		"function", functionName,
	)

	// Read the first message which contains the FlightDescriptor and parameters
	msg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to receive initial message: %v", err)
	}

	s.logger.Debug("Received initial message for table function",
		"message", msg,
		"has_app_metadata", msg.AppMetadata != nil,
		"app_metadata_len", len(msg.AppMetadata),
	)

	// Read the second message which contains parameters (msgpack-encoded)
	// According to Airport spec, parameters are sent as an initial metadata message
	paramMsg, err := stream.Recv()
	if err != nil {
		s.logger.Warn("Failed to receive parameter message", "error", err)
		return status.Errorf(codes.Internal, "failed to receive parameter message: %v", err)
	}

	s.logger.Debug("Received parameter message",
		"has_app_metadata", paramMsg.AppMetadata != nil,
		"app_metadata_len", len(paramMsg.AppMetadata),
		"first_bytes", fmt.Sprintf("%x", paramMsg.AppMetadata[:min(20, len(paramMsg.AppMetadata))]),
	)

	// Decode function parameters from msgpack-encoded metadata
	params := decodeTableFunctionParams(paramMsg.AppMetadata, s.logger)

	// Create input RecordReader from stream
	inputReader, err := flight.NewRecordReader(stream, ipc.WithAllocator(s.allocator))
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create input record reader: %v", err)
	}
	defer inputReader.Release()

	inputSchema := inputReader.Schema()
	s.logger.Debug("Input schema for table function",
		"schema", inputSchema,
	)

	// Get output schema from function
	outputSchema, err := targetFunc.SchemaForParameters(ctx, params, inputSchema)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get output schema: %v", err)
	}

	s.logger.Debug("Output schema for table function",
		"schema", outputSchema,
	)
	// Create writer for output data and send schema BEFORE executing function
	// This allows the client to start sending input data
	writer := NewSchemaWriter(stream, outputSchema, s.allocator, true)
	defer writer.Close()

	if err := writer.Begin(); err != nil {
		return status.Errorf(codes.Internal, "failed to send output schema: %v", err)
	}

	inputCh := make(chan arrow.RecordBatch, 1)
	// read input batches
	go func() {
		defer inputReader.Release()
		defer close(inputCh)

		for inputReader.Next() {
			record := inputReader.RecordBatch()
			record.Retain() // Retain for passing to function

			s.logger.Debug("Received input batch for table function",
				"num_rows", record.NumRows(),
				"num_cols", record.NumCols(),
			)

			select {
			case inputCh <- record:
			case <-ctx.Done():
				return
			}
		}
	}()

	eg, egCtx := errgroup.WithContext(ctx)
	processCh := make(chan array.RecordReader, 1)
	// process input batches
	eg.Go(func() error {
		defer close(processCh)

		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case batch, ok := <-inputCh:
				if !ok {
					// inputCh closed, no more data
					return nil
				}

				s.logger.Debug("Processing input batch for table function",
					"rows", batch.NumRows(),
					"columns", batch.NumCols(),
				)

				// Save input row count before releasing
				inLen := batch.NumRows()
				reader, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
				if err != nil {
					batch.Release()
					return err
				}
				batch.Release()

				// Execute the table function
				outputReader, err := targetFunc.Execute(egCtx, params, reader, &catalog.ScanOptions{})
				reader.Release()
				if err != nil {
					s.logger.Error("Table function execution failed",
						"function", functionName,
						"error", err,
					)
					return err
				}

				// sent to writer stage
				select {
				case processCh <- outputReader:
				case <-egCtx.Done():
					outputReader.Release()
					return egCtx.Err()
				}

				s.logger.Debug("Table function batch processed",
					"input_rows", inLen,
				)
			}
		}
	})

	// write output batches
	totalBatches := 0
	totalRows := int64(0)
	eg.Go(func() error {
		for outputReader := range processCh {
			// Send output batches to client
			batchCount := 0
			var rowsCount int64
			for outputReader.Next() {
				batchCount++
				batch := outputReader.RecordBatch()
				rowsCount += batch.NumRows()

				s.logger.Debug("Sending table function output batch",
					"batch", batchCount,
					"rows", batch.NumRows(),
				)

				if err := writer.Write(batch); err != nil {
					return fmt.Errorf("failed to write output batch: %w", err)
				}
			}

			if err := writer.WriteFinished(); err != nil {
				outputReader.Release()
				return fmt.Errorf("failed to finalize output batch: %w", err)
			}

			if err := outputReader.Err(); err != nil {
				outputReader.Release()
				return fmt.Errorf("error reading output: %w", err)
			}
			outputReader.Release()

			s.logger.Debug("Table function output reader completed",
				"batches_sent", batchCount,
				"rows_sent", rowsCount,
			)
			totalBatches += batchCount
			totalRows += rowsCount
		}
		return nil
	})

	// wait for all stages to complete
	if err := eg.Wait(); err != nil {
		s.logger.Error("DoExchange table function pipeline failed",
			"function", functionName,
			"error", err,
		)
		return status.Errorf(codes.Internal, "table function `%s.%s` execution failed: %v", schemaName, functionName, err)
	}

	s.logger.Debug("DoExchange table function completed",
		"function", functionName,
		"total_batches", totalBatches,
		"total_rows", totalRows,
	)

	return nil
}

type syncReader struct {
	array.RecordReader
	writer *SchemaWriter
	err    error
	isInit bool
	closed bool
}

func (sr *syncReader) Next() bool {
	if sr.isInit {
		if err := sr.writer.WriteFinished(); err != nil {
			sr.err = err
			return false
		}
	}
	if !sr.isInit {
		sr.isInit = true
	}
	if !sr.RecordReader.Next() {
		return false
	}
	return true
}

func (sr *syncReader) Err() error {
	if sr.err != nil {
		return sr.err
	}
	return sr.RecordReader.Err()
}

func (sr *syncReader) Release() {
	sr.writer.WriteFinished()
}
