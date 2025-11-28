package flight

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hugr-lab/airport-go/catalog"
)

// DoGet streams Arrow record batches for a table query.
// This is the core RPC for executing queries and returning Arrow data.
//
// The ticket must be encoded using EncodeTicket (schema/table names).
// The handler:
//  1. Decodes the ticket to get schema/table names
//  2. Looks up the table in the catalog
//  3. Calls the table's Scan function to get RecordReader
//  4. Validates the RecordReader schema matches table schema
//  5. Streams record batches using Arrow IPC format
//  6. Respects context cancellation
//  7. Propagates errors from scan function
func (s *Server) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	ctx := stream.Context()

	s.logger.Debug("DoGet called", "ticket_size", len(ticket.GetTicket()))

	// Decode ticket to get schema/table names
	ticketData, err := DecodeTicket(ticket.GetTicket())
	if err != nil {
		s.logger.Error("Failed to decode ticket", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}

	s.logger.Debug("DoGet request",
		"schema", ticketData.Schema,
		"table", ticketData.Table,
		"table_function", ticketData.TableFunction,
	)

	// Look up schema in catalog
	schema, err := s.catalog.Schema(ctx, ticketData.Schema)
	if err != nil {
		s.logger.Error("Failed to get schema from catalog",
			"schema", ticketData.Schema,
			"error", err,
		)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema not found: %s", ticketData.Schema)
	}

	// Branch based on whether this is a table or table function call
	var reader array.RecordReader
	var readerSchema *arrow.Schema

	if ticketData.TableFunction != "" {
		// Handle table function execution
		reader, readerSchema, err = s.executeTableFunction(ctx, schema, ticketData)
		if err != nil {
			return err // Error already formatted
		}
	} else {
		// Handle regular table scan
		reader, readerSchema, err = s.executeTableScan(ctx, schema, ticketData)
		if err != nil {
			return err // Error already formatted
		}
	}
	defer reader.Release()

	s.logger.Debug("Starting record streaming",
		"schema", ticketData.Schema,
		"target", fmt.Sprintf("%s%s", ticketData.Table, ticketData.TableFunction),
		"num_fields", readerSchema.NumFields(),
	)

	// Stream record batches using Arrow IPC format (T028)
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(readerSchema))
	defer writer.Close()

	batchCount := 0
	totalRows := int64(0)

	// Stream batches (T029: context cancellation handled by reader.Next())
	for reader.Next() {
		// Check context cancellation (T029)
		select {
		case <-ctx.Done():
			s.logger.Debug("DoGet cancelled by client",
				"schema", ticketData.Schema,
				"table", ticketData.Table,
				"batches_sent", batchCount,
				"rows_sent", totalRows,
			)
			return status.Error(codes.Canceled, "request cancelled")
		default:
		}

		record := reader.RecordBatch()
		batchCount++
		totalRows += record.NumRows()

		// Write batch to stream
		if err := writer.Write(record); err != nil {
			s.logger.Error("Failed to write record batch",
				"schema", ticketData.Schema,
				"table", ticketData.Table,
				"batch", batchCount,
				"error", err,
			)
			return status.Errorf(codes.Internal, "failed to write batch %d: %v", batchCount, err)
		}

		s.logger.Debug("Sent record batch",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"batch", batchCount,
			"rows_in_batch", record.NumRows(),
			"total_rows", totalRows,
		)
	}

	// Check for errors during iteration (T030: error propagation)
	if err := reader.Err(); err != nil {
		// Check if error is EOF (normal termination)
		if err == io.EOF {
			s.logger.Debug("DoGet completed (EOF)",
				"schema", ticketData.Schema,
				"table", ticketData.Table,
				"batches_sent", batchCount,
				"total_rows", totalRows,
			)
		} else {
			s.logger.Error("RecordReader error during iteration",
				"schema", ticketData.Schema,
				"table", ticketData.Table,
				"batch", batchCount,
				"error", err,
			)
			return status.Errorf(codes.Internal, "scan error after batch %d: %v", batchCount, err)
		}
	}

	s.logger.Debug("DoGet completed successfully",
		"schema", ticketData.Schema,
		"table", ticketData.Table,
		"batches_sent", batchCount,
		"total_rows", totalRows,
	)

	return nil
}

// executeTableScan handles regular table scan operations.
func (s *Server) executeTableScan(ctx context.Context, schema catalog.Schema, ticketData *TicketData) (array.RecordReader, *arrow.Schema, error) {
	// Look up table in schema
	table, err := schema.Table(ctx, ticketData.Table)
	if err != nil {
		s.logger.Error("Failed to get table from schema",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"error", err,
		)
		return nil, nil, status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		return nil, nil, status.Errorf(codes.NotFound, "table not found: %s.%s", ticketData.Schema, ticketData.Table)
	}

	// Get table's Arrow schema for validation
	tableSchema := table.ArrowSchema()
	if tableSchema == nil {
		s.logger.Error("Table returned nil Arrow schema",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
		)
		return nil, nil, status.Errorf(codes.Internal, "table %s.%s has nil Arrow schema", ticketData.Schema, ticketData.Table)
	}

	// Convert ticket data to scan options (includes time-travel parameters)
	scanOpts := ticketData.ToScanOptions()

	// Log time-travel query if timestamp parameters present
	if scanOpts.TimePoint != nil {
		s.logger.Debug("Point-in-time query",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"time_unit", scanOpts.TimePoint.Unit,
			"time_value", scanOpts.TimePoint.Value,
		)
	}

	// Call table's Scan function to get RecordReader
	reader, err := table.Scan(ctx, scanOpts)
	if err != nil {
		s.logger.Error("Table scan failed",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"error", err,
		)
		return nil, nil, status.Errorf(codes.Internal, "table scan failed: %v", err)
	}

	// Validate RecordReader schema matches table schema
	readerSchema := reader.Schema()
	if !tableSchema.Equal(readerSchema) {
		reader.Release()
		s.logger.Error("RecordReader schema does not match table schema",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"table_schema_fields", tableSchema.NumFields(),
			"reader_schema_fields", readerSchema.NumFields(),
		)
		return nil, nil, status.Errorf(codes.Internal,
			"schema mismatch: table has %d fields, reader has %d fields",
			tableSchema.NumFields(), readerSchema.NumFields())
	}

	return reader, readerSchema, nil
}

// executeTableFunction handles table function execution with dynamic schemas.
func (s *Server) executeTableFunction(ctx context.Context, schema catalog.Schema, ticketData *TicketData) (array.RecordReader, *arrow.Schema, error) {
	// Get table functions from schema
	functions, err := schema.TableFunctions(ctx)
	if err != nil {
		s.logger.Error("Failed to get table functions",
			"schema", ticketData.Schema,
			"error", err,
		)
		return nil, nil, status.Errorf(codes.Internal, "failed to get table functions: %v", err)
	}

	// Find the requested function
	var targetFunc catalog.TableFunction
	for _, fn := range functions {
		if fn.Name() == ticketData.TableFunction {
			targetFunc = fn
			break
		}
	}

	if targetFunc == nil {
		return nil, nil, status.Errorf(codes.NotFound, "table function not found: %s.%s",
			ticketData.Schema, ticketData.TableFunction)
	}

	s.logger.Debug("Executing table function",
		"schema", ticketData.Schema,
		"function", ticketData.TableFunction,
		"param_count", len(ticketData.FunctionParams),
	)

	// Get the schema for these parameters (dynamic schema based on params)
	funcSchema, err := targetFunc.SchemaForParameters(ctx, ticketData.FunctionParams)
	if err != nil {
		s.logger.Error("Failed to get function schema",
			"schema", ticketData.Schema,
			"function", ticketData.TableFunction,
			"error", err,
		)
		return nil, nil, status.Errorf(codes.Internal, "failed to get function schema: %v", err)
	}

	s.logger.Debug("Table function schema determined",
		"schema", ticketData.Schema,
		"function", ticketData.TableFunction,
		"output_fields", funcSchema.NumFields(),
	)

	// Convert ticket data to scan options
	scanOpts := ticketData.ToScanOptions()

	// Execute the table function
	reader, err := targetFunc.Execute(ctx, ticketData.FunctionParams, scanOpts)
	if err != nil {
		s.logger.Error("Table function execution failed",
			"schema", ticketData.Schema,
			"function", ticketData.TableFunction,
			"error", err,
		)
		return nil, nil, status.Errorf(codes.Internal, "table function execution failed: %v", err)
	}

	// Validate reader schema matches function's declared schema
	readerSchema := reader.Schema()
	if !funcSchema.Equal(readerSchema) {
		reader.Release()
		s.logger.Error("RecordReader schema does not match function schema",
			"schema", ticketData.Schema,
			"function", ticketData.TableFunction,
			"function_schema_fields", funcSchema.NumFields(),
			"reader_schema_fields", readerSchema.NumFields(),
		)
		return nil, nil, status.Errorf(codes.Internal,
			"schema mismatch: function declared %d fields, reader has %d fields",
			funcSchema.NumFields(), readerSchema.NumFields())
	}

	return reader, readerSchema, nil
}
