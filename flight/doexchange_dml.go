package flight

import (
	"context"
	"errors"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

// AirportChangedFinalMetadata is the msgpack response for DML operations.
// This matches the Airport extension's expected format.
type AirportChangedFinalMetadata struct {
	TotalChanged uint64 `msgpack:"total_changed"`
}

// handleDoExchangeInsert processes INSERT operations via DoExchange.
// This is called by DuckDB Airport extension when executing INSERT SQL statements.
//
// Protocol:
// - Client sends record batches with row data
// - Server inserts rows into the table
// - If return-chunks=1, server sends back RETURNING data
// - Server sends final metadata with total_changed count
//
// Implementation uses a bidirectional pipeline with concurrent goroutines:
// 1. Reader goroutine: Reads input records from client stream
// 2. Processor goroutine: Inserts data and produces RETURNING data
// 3. Writer goroutine: Sends RETURNING data back to client (if requested)
func (s *Server) handleDoExchangeInsert(ctx context.Context, stream flight.FlightService_DoExchangeServer, schemaName, tableName string, returnData bool) error {
	s.logger.Debug("DoExchange INSERT requested",
		"schema", schemaName,
		"table", tableName,
		"return_data", returnData,
	)

	// Get transaction context
	ctx = s.getTransactionContext(ctx)

	// Look up schema
	schema, err := s.catalog.Schema(ctx, schemaName)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", schemaName, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema '%s' not found", schemaName)
	}

	// Look up table
	table, err := schema.Table(ctx, tableName)
	if err != nil {
		s.logger.Error("Failed to get table", "table", tableName, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		return status.Errorf(codes.NotFound, "table '%s.%s' not found", schemaName, tableName)
	}

	// Check if table supports INSERT
	insertableTable, ok := table.(catalog.InsertableTable)
	if !ok {
		return status.Errorf(codes.FailedPrecondition, "table '%s' does not support INSERT operations", tableName)
	}

	// Create record reader from stream directly
	// The flight.NewRecordReader handles reading the schema message
	inputReader, err := flight.NewRecordReader(stream, ipc.WithAllocator(s.allocator))
	if errors.Is(err, io.EOF) {
		return s.sendDMLFinalMetadata(stream, 0)
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create input record reader: %v", err)
	}

	inputSchema := inputReader.Schema()
	s.logger.Debug("Created record reader for INSERT",
		"input_schema", inputSchema,
	)

	// Create a writer to send output schema (required for bidirectional exchange)
	// Even if not returning data, we may need to send a schema to initiate the response stream
	writer := NewSchemaWriter(stream, inputSchema, s.allocator)
	defer writer.Close()

	// Send schema to client to acknowledge and enable bidirectional data flow
	if err := writer.Begin(); err != nil {
		inputReader.Release()
		return status.Errorf(codes.Internal, "failed to send output schema: %v", err)
	}

	// Pipeline channels for bidirectional streaming
	// Use buffered channel to allow some batches to queue
	inputCh := make(chan arrow.RecordBatch, 4)
	outputCh := make(chan arrow.RecordBatch, 4)

	// Track results across pipeline
	var totalRows int64

	// Error group for managing goroutines
	eg, egCtx := errgroup.WithContext(ctx)

	// Reader goroutine: reads input batches from stream
	// Run outside errgroup to properly handle stream closure
	go func() {
		defer close(inputCh)
		defer inputReader.Release()

		for inputReader.Next() {
			record := inputReader.RecordBatch()
			record.Retain()

			s.logger.Debug("Received INSERT batch",
				"rows", record.NumRows(),
				"cols", record.NumCols(),
			)

			select {
			case inputCh <- record:
			case <-egCtx.Done():
				record.Release()
				return
			}
		}
		if err := inputReader.Err(); err != nil && !errors.Is(err, io.EOF) {
			s.logger.Error("Error reading INSERT input", "error", err)
		}
	}()

	// Processor goroutine: inserts data per-batch and produces RETURNING batches
	// This processes incrementally to avoid deadlock - each batch is inserted
	// and RETURNING data is sent before waiting for more input
	eg.Go(func() error {
		defer close(outputCh)

		for batch := range inputCh {
			totalRows += batch.NumRows()

			// Create a single-batch RecordReader for this batch
			batchReader, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
			if err != nil {
				batch.Release()
				return err
			}

			// Execute INSERT for this batch with transaction handling
			var dmlResult *catalog.DMLResult
			insertErr := s.withTransaction(egCtx, func(txCtx context.Context) error {
				var err error
				dmlResult, err = insertableTable.Insert(txCtx, batchReader)
				return err
			})

			batch.Release()

			if insertErr != nil {
				return insertErr
			}

			// Send RETURNING data through output channel if requested
			if returnData && dmlResult != nil && dmlResult.ReturningData != nil {
				for dmlResult.ReturningData.Next() {
					outBatch := dmlResult.ReturningData.RecordBatch()
					outBatch.Retain()

					select {
					case outputCh <- outBatch:
					case <-egCtx.Done():
						outBatch.Release()
						return egCtx.Err()
					}
				}
				if err := dmlResult.ReturningData.Err(); err != nil {
					return err
				}
			}
		}

		return nil
	})

	// Writer goroutine: sends RETURNING batches back to client
	eg.Go(func() error {
		for batch := range outputCh {
			if err := writer.Write(batch); err != nil {
				batch.Release()
				return err
			}
			s.logger.Debug("Sent RETURNING batch",
				"rows", batch.NumRows(),
			)
			batch.Release()
		}
		return nil
	})

	// Wait for pipeline to complete
	if err := eg.Wait(); err != nil {
		s.logger.Error("INSERT pipeline failed", "schema", schemaName, "table", tableName, "error", err)
		return status.Errorf(codes.Internal, "INSERT failed: %v", err)
	}

	s.logger.Debug("INSERT completed",
		"schema", schemaName,
		"table", tableName,
		"total_rows", totalRows,
	)

	return s.sendDMLFinalMetadata(stream, uint64(totalRows))
}

// handleDoExchangeUpdate processes UPDATE operations via DoExchange.
// This is called by DuckDB Airport extension when executing UPDATE SQL statements.
//
// Protocol:
// - Client sends record batches with rowid column and new column values
// - Server updates rows in the table
// - If return-chunks=1, server sends back RETURNING data
// - Server sends final metadata with total_changed count
//
// Implementation uses a bidirectional pipeline with concurrent goroutines.
func (s *Server) handleDoExchangeUpdate(ctx context.Context, stream flight.FlightService_DoExchangeServer, schemaName, tableName string, returnData bool) error {
	s.logger.Debug("DoExchange UPDATE requested",
		"schema", schemaName,
		"table", tableName,
		"return_data", returnData,
	)

	// Get transaction context
	ctx = s.getTransactionContext(ctx)

	// Look up schema
	schema, err := s.catalog.Schema(ctx, schemaName)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", schemaName, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema '%s' not found", schemaName)
	}

	// Look up table
	table, err := schema.Table(ctx, tableName)
	if err != nil {
		s.logger.Error("Failed to get table", "table", tableName, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		return status.Errorf(codes.NotFound, "table '%s.%s' not found", schemaName, tableName)
	}

	// Check if table supports UPDATE
	updatableTable, ok := table.(catalog.UpdatableTable)
	if !ok {
		return status.Errorf(codes.FailedPrecondition, "table '%s' does not support UPDATE operations", tableName)
	}

	// Create record reader from stream directly
	inputReader, err := flight.NewRecordReader(stream, ipc.WithAllocator(s.allocator))
	if errors.Is(err, io.EOF) {
		return s.sendDMLFinalMetadata(stream, 0)
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create input record reader: %v", err)
	}

	inputSchema := inputReader.Schema()
	s.logger.Debug("Created record reader for UPDATE",
		"input_schema", inputSchema,
	)

	// Create a writer to send output schema (required for bidirectional exchange)
	writer := NewSchemaWriter(stream, inputSchema, s.allocator)
	defer writer.Close()

	// Send schema to client to acknowledge and enable bidirectional data flow
	if err := writer.Begin(); err != nil {
		inputReader.Release()
		return status.Errorf(codes.Internal, "failed to send output schema: %v", err)
	}

	// Find the rowid column in the input schema
	rowidColIdx := -1
	for i := 0; i < inputSchema.NumFields(); i++ {
		field := inputSchema.Field(i)
		if field.Name == "rowid" {
			rowidColIdx = i
			break
		}
		if md := field.Metadata; md.Len() > 0 {
			if idx := md.FindKey("is_rowid"); idx >= 0 && md.Values()[idx] != "" {
				rowidColIdx = i
				break
			}
		}
	}

	if rowidColIdx == -1 {
		inputReader.Release()
		return status.Errorf(codes.InvalidArgument, "UPDATE requires rowid column in input schema")
	}

	// Pipeline channels for bidirectional streaming
	inputCh := make(chan arrow.RecordBatch, 1)
	outputCh := make(chan arrow.RecordBatch, 1)

	// Track results across pipeline
	var totalRows int64

	// Error group for managing goroutines
	eg, egCtx := errgroup.WithContext(ctx)

	// Reader goroutine: reads input batches from stream
	go func() {
		defer close(inputCh)
		defer inputReader.Release()

		for inputReader.Next() {
			record := inputReader.RecordBatch()
			record.Retain()

			s.logger.Debug("Received UPDATE batch",
				"rows", record.NumRows(),
				"cols", record.NumCols(),
			)

			select {
			case inputCh <- record:
			case <-egCtx.Done():
				record.Release()
				return
			}
		}
		if err := inputReader.Err(); err != nil && !errors.Is(err, io.EOF) {
			s.logger.Error("Error reading UPDATE input", "error", err)
		}
	}()

	// Processor goroutine: updates data and produces RETURNING batches
	eg.Go(func() error {
		defer close(outputCh)

		// Collect batches and extract rowids
		var records []arrow.RecordBatch
		var rowIDs []int64
		for batch := range inputCh {
			totalRows += batch.NumRows()

			// Extract rowids from this batch
			rowidCol := batch.Column(rowidColIdx)
			rowIDsFromBatch, err := extractRowIDs(rowidCol)
			if err != nil {
				batch.Release()
				for _, r := range records {
					r.Release()
				}
				return err
			}
			rowIDs = append(rowIDs, rowIDsFromBatch...)
			records = append(records, batch)
		}

		if len(records) == 0 {
			return nil
		}

		// Create RecordReader from collected batches (without rowid column)
		dataRecords := stripRowIDColumn(records, rowidColIdx)

		// Release original records
		for _, r := range records {
			r.Release()
		}

		if len(dataRecords) == 0 {
			return nil
		}

		recordReader, err := array.NewRecordReader(dataRecords[0].Schema(), dataRecords)
		if err != nil {
			for _, r := range dataRecords {
				r.Release()
			}
			return err
		}

		// Execute UPDATE with transaction handling
		var dmlResult *catalog.DMLResult
		updateErr := s.withTransaction(egCtx, func(txCtx context.Context) error {
			var err error
			dmlResult, err = updatableTable.Update(txCtx, rowIDs, recordReader)
			return err
		})

		// Release data records after update
		for _, r := range dataRecords {
			r.Release()
		}

		if updateErr != nil {
			return updateErr
		}

		// Send RETURNING data through output channel if requested
		if returnData && dmlResult != nil && dmlResult.ReturningData != nil {
			for dmlResult.ReturningData.Next() {
				outBatch := dmlResult.ReturningData.RecordBatch()
				outBatch.Retain()

				select {
				case outputCh <- outBatch:
				case <-egCtx.Done():
					outBatch.Release()
					return egCtx.Err()
				}
			}
			if err := dmlResult.ReturningData.Err(); err != nil {
				return err
			}
		}

		return nil
	})

	// Writer goroutine: sends RETURNING batches back to client
	eg.Go(func() error {
		for batch := range outputCh {
			if err := writer.Write(batch); err != nil {
				batch.Release()
				return err
			}
			s.logger.Debug("Sent UPDATE RETURNING batch",
				"rows", batch.NumRows(),
			)
			batch.Release()
		}
		return nil
	})

	// Wait for pipeline to complete
	if err := eg.Wait(); err != nil {
		s.logger.Error("UPDATE pipeline failed", "schema", schemaName, "table", tableName, "error", err)
		return status.Errorf(codes.Internal, "UPDATE failed: %v", err)
	}

	s.logger.Debug("UPDATE completed",
		"schema", schemaName,
		"table", tableName,
		"total_rows", totalRows,
	)

	return s.sendDMLFinalMetadata(stream, uint64(totalRows))
}

// handleDoExchangeDelete processes DELETE operations via DoExchange.
// This is called by DuckDB Airport extension when executing DELETE SQL statements.
//
// Protocol:
// - Client sends record batches with only the rowid column
// - Server deletes rows from the table
// - If return-chunks=1, server sends back RETURNING data
// - Server sends final metadata with total_changed count
//
// Implementation uses a bidirectional pipeline with concurrent goroutines.
func (s *Server) handleDoExchangeDelete(ctx context.Context, stream flight.FlightService_DoExchangeServer, schemaName, tableName string, returnData bool) error {
	s.logger.Debug("DoExchange DELETE requested",
		"schema", schemaName,
		"table", tableName,
		"return_data", returnData,
	)

	// Get transaction context
	ctx = s.getTransactionContext(ctx)

	// Look up schema
	schema, err := s.catalog.Schema(ctx, schemaName)
	if err != nil {
		s.logger.Error("Failed to get schema", "schema", schemaName, "error", err)
		return status.Errorf(codes.Internal, "failed to get schema: %v", err)
	}
	if schema == nil {
		return status.Errorf(codes.NotFound, "schema '%s' not found", schemaName)
	}

	// Look up table
	table, err := schema.Table(ctx, tableName)
	if err != nil {
		s.logger.Error("Failed to get table", "table", tableName, "error", err)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		return status.Errorf(codes.NotFound, "table '%s.%s' not found", schemaName, tableName)
	}

	// Check if table supports DELETE
	deletableTable, ok := table.(catalog.DeletableTable)
	if !ok {
		return status.Errorf(codes.FailedPrecondition, "table '%s' does not support DELETE operations", tableName)
	}

	// Create record reader from stream directly
	inputReader, err := flight.NewRecordReader(stream, ipc.WithAllocator(s.allocator))
	if errors.Is(err, io.EOF) {
		return s.sendDMLFinalMetadata(stream, 0)
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create input record reader: %v", err)
	}

	inputSchema := inputReader.Schema()
	s.logger.Debug("Created record reader for DELETE",
		"input_schema", inputSchema,
	)

	// Create a writer to send output schema (required for bidirectional exchange)
	writer := NewSchemaWriter(stream, inputSchema, s.allocator)
	defer writer.Close()

	// Send schema to client to acknowledge and enable bidirectional data flow
	if err := writer.Begin(); err != nil {
		inputReader.Release()
		return status.Errorf(codes.Internal, "failed to send output schema: %v", err)
	}

	// Pipeline channels for bidirectional streaming
	inputCh := make(chan arrow.RecordBatch, 1)
	outputCh := make(chan arrow.RecordBatch, 1)

	// Track results across pipeline
	var totalRows int64

	// Error group for managing goroutines
	eg, egCtx := errgroup.WithContext(ctx)

	// Reader goroutine: reads input batches from stream
	go func() {
		defer close(inputCh)
		defer inputReader.Release()

		for inputReader.Next() {
			record := inputReader.RecordBatch()
			record.Retain()

			s.logger.Debug("Received DELETE batch",
				"rows", record.NumRows(),
				"cols", record.NumCols(),
			)

			select {
			case inputCh <- record:
			case <-egCtx.Done():
				record.Release()
				return
			}
		}
		if err := inputReader.Err(); err != nil && !errors.Is(err, io.EOF) {
			s.logger.Error("Error reading DELETE input", "error", err)
		}
	}()

	// Processor goroutine: deletes data and produces RETURNING batches
	eg.Go(func() error {
		defer close(outputCh)

		// Collect rowids from all batches
		var rowIDs []int64
		for batch := range inputCh {
			totalRows += batch.NumRows()

			if batch.NumCols() > 0 {
				// The first column should be rowid
				rowidCol := batch.Column(0)
				rowIDsFromBatch, err := extractRowIDs(rowidCol)
				if err != nil {
					batch.Release()
					return err
				}
				rowIDs = append(rowIDs, rowIDsFromBatch...)
			}
			batch.Release()
		}

		if len(rowIDs) == 0 {
			return nil
		}

		// Execute DELETE with transaction handling
		var dmlResult *catalog.DMLResult
		deleteErr := s.withTransaction(egCtx, func(txCtx context.Context) error {
			var err error
			dmlResult, err = deletableTable.Delete(txCtx, rowIDs)
			return err
		})

		if deleteErr != nil {
			return deleteErr
		}

		// Send RETURNING data through output channel if requested
		if returnData && dmlResult != nil && dmlResult.ReturningData != nil {
			for dmlResult.ReturningData.Next() {
				outBatch := dmlResult.ReturningData.RecordBatch()
				outBatch.Retain()

				select {
				case outputCh <- outBatch:
				case <-egCtx.Done():
					outBatch.Release()
					return egCtx.Err()
				}
			}
			if err := dmlResult.ReturningData.Err(); err != nil {
				return err
			}
		}

		return nil
	})

	// Writer goroutine: sends RETURNING batches back to client
	eg.Go(func() error {
		for batch := range outputCh {
			if err := writer.Write(batch); err != nil {
				batch.Release()
				return err
			}
			s.logger.Debug("Sent DELETE RETURNING batch",
				"rows", batch.NumRows(),
			)
			batch.Release()
		}
		return nil
	})

	// Wait for pipeline to complete
	if err := eg.Wait(); err != nil {
		s.logger.Error("DELETE pipeline failed", "schema", schemaName, "table", tableName, "error", err)
		return status.Errorf(codes.Internal, "DELETE failed: %v", err)
	}

	s.logger.Debug("DELETE completed",
		"schema", schemaName,
		"table", tableName,
		"total_rows", totalRows,
	)

	return s.sendDMLFinalMetadata(stream, uint64(totalRows))
}

// sendDMLFinalMetadata sends the final metadata message for DML operations.
// This is the msgpack-encoded AirportChangedFinalMetadata struct.
func (s *Server) sendDMLFinalMetadata(stream flight.FlightService_DoExchangeServer, totalChanged uint64) error {
	metadata := AirportChangedFinalMetadata{
		TotalChanged: totalChanged,
	}

	metadataBytes, err := msgpack.Encode(metadata)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to encode final metadata: %v", err)
	}

	// Send as FlightData with app_metadata
	return stream.Send(&flight.FlightData{
		AppMetadata: metadataBytes,
	})
}

// extractRowIDs extracts row IDs from an Arrow array.
// Supports Int64 and Int32 array types.
func extractRowIDs(arr arrow.Array) ([]int64, error) {
	rowIDs := make([]int64, arr.Len())

	switch typedArr := arr.(type) {
	case *array.Int64:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue // Skip null rowids
			}
			rowIDs[i] = typedArr.Value(i)
		}
	case *array.Int32:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			rowIDs[i] = int64(typedArr.Value(i))
		}
	case *array.Uint64:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			rowIDs[i] = int64(typedArr.Value(i))
		}
	default:
		return nil, &DMLError{
			Code:    "INVALID_ROWID_TYPE",
			Message: "rowid column must be Int64, Int32, or Uint64",
		}
	}

	return rowIDs, nil
}

// stripRowIDColumn removes the rowid column from records for UPDATE operations.
// The Update method expects only data columns, not the rowid.
func stripRowIDColumn(records []arrow.RecordBatch, rowidColIdx int) []arrow.RecordBatch {
	if len(records) == 0 {
		return nil
	}

	// Build new schema without rowid column
	origSchema := records[0].Schema()
	newFields := make([]arrow.Field, 0, origSchema.NumFields()-1)
	for i := 0; i < origSchema.NumFields(); i++ {
		if i != rowidColIdx {
			newFields = append(newFields, origSchema.Field(i))
		}
	}
	newSchema := arrow.NewSchema(newFields, nil)

	// Build new records without rowid column
	result := make([]arrow.RecordBatch, 0, len(records))
	for _, record := range records {
		newCols := make([]arrow.Array, 0, record.NumCols()-1)
		for i := 0; i < int(record.NumCols()); i++ {
			if i != rowidColIdx {
				col := record.Column(i)
				col.Retain()
				newCols = append(newCols, col)
			}
		}

		newRecord := array.NewRecordBatch(newSchema, newCols, record.NumRows())
		// Release the retained columns since NewRecordBatch retains them
		for _, col := range newCols {
			col.Release()
		}
		result = append(result, newRecord)
	}

	return result
}
