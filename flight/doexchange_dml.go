package flight

import (
	"context"
	"errors"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

	// Determine output schema for RETURNING data
	// For RETURNING, use table schema projected to returning columns (all columns except rowid)
	// This allows returning auto-generated columns like 'id' that aren't in the input
	var outputSchema *arrow.Schema
	if returnData {
		outputSchema = catalog.ProjectSchema(table.ArrowSchema(nil), getTableColumnNames(table))
	} else {
		outputSchema = inputSchema
	}

	// Create a writer to send output schema (required for bidirectional exchange)
	writer := NewSchemaWriter(stream, outputSchema, s.allocator)
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

	// Create DMLOptions with RETURNING information
	// Note: DuckDB Airport extension does not communicate which specific columns
	// are in the RETURNING clause. When RETURNING is requested, we populate
	// ReturningColumns with all table columns (excluding pseudo-columns like rowid).
	// DuckDB handles column projection client-side after receiving server response.
	opts := &catalog.DMLOptions{
		Returning: returnData,
	}
	if returnData {
		opts.ReturningColumns = getTableColumnNames(table)
	}

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
				dmlResult, err = insertableTable.Insert(txCtx, batchReader, opts)
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

	// For UPDATE, the output schema must match the input schema that DuckDB expects
	// DuckDB sends [updated_cols..., id, rowid] and expects the same schema back
	// The transformBatchSchema handles adapting table's RETURNING data to this schema
	outputSchema := inputSchema

	// Create a writer to send output schema (required for bidirectional exchange)
	writer := NewSchemaWriter(stream, outputSchema, s.allocator)
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

	// Create DMLOptions with RETURNING information
	// Note: DuckDB Airport extension does not communicate which specific columns
	// are in the RETURNING clause. When RETURNING is requested, we populate
	// ReturningColumns with all table columns (excluding pseudo-columns like rowid).
	// DuckDB handles column projection client-side after receiving server response.
	opts := &catalog.DMLOptions{
		Returning: returnData,
	}
	if returnData {
		opts.ReturningColumns = getTableColumnNames(table)
	}

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

	// Processor goroutine: updates data per-batch and produces RETURNING batches
	// This processes incrementally to avoid deadlock - each batch is updated
	// and RETURNING data is sent before waiting for more input
	eg.Go(func() error {
		defer close(outputCh)

		for batch := range inputCh {
			totalRows += batch.NumRows()

			// Extract rowids from this batch
			rowidCol := batch.Column(rowidColIdx)
			rowIDsFromBatch, err := extractRowIDs(rowidCol)
			if err != nil {
				batch.Release()
				return err
			}

			// Strip rowid column from batch to create data record
			dataRecords := stripRowIDColumn([]arrow.RecordBatch{batch}, rowidColIdx)
			batch.Release()

			if len(dataRecords) == 0 {
				continue
			}

			// Create RecordReader for this single batch
			recordReader, err := array.NewRecordReader(dataRecords[0].Schema(), dataRecords)
			if err != nil {
				for _, r := range dataRecords {
					r.Release()
				}
				return err
			}

			// Execute UPDATE for this batch with transaction handling
			var dmlResult *catalog.DMLResult
			updateErr := s.withTransaction(egCtx, func(txCtx context.Context) error {
				var err error
				dmlResult, err = updatableTable.Update(txCtx, rowIDsFromBatch, recordReader, opts)
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
				s.logger.Debug("Processing UPDATE RETURNING data")
				for dmlResult.ReturningData.Next() {
					outBatch := dmlResult.ReturningData.RecordBatch()
					s.logger.Debug("Sending UPDATE RETURNING batch to channel",
						"rows", outBatch.NumRows(),
					)
					outBatch.Retain()

					select {
					case outputCh <- outBatch:
						s.logger.Debug("Sent UPDATE RETURNING batch to channel")
					case <-egCtx.Done():
						outBatch.Release()
						return egCtx.Err()
					}
				}
				if err := dmlResult.ReturningData.Err(); err != nil {
					s.logger.Error("RETURNING data reader error", "error", err)
					return err
				}
				s.logger.Debug("Finished processing UPDATE RETURNING data")
			}
			s.logger.Debug("UPDATE batch processing complete, waiting for next batch")
		}

		s.logger.Debug("UPDATE processor completed")
		return nil
	})

	// Writer goroutine: sends RETURNING batches back to client
	eg.Go(func() error {
		for batch := range outputCh {
			s.logger.Debug("Writing UPDATE RETURNING batch to stream",
				"rows", batch.NumRows(),
			)
			// Transform batch to match writer's expected schema (outputSchema)
			// The table may return data with slightly different schema
			transformedBatch := transformBatchSchema(batch, outputSchema, s.allocator)
			if transformedBatch != batch {
				batch.Release() // Release original if transformed
			}
			if err := writer.Write(transformedBatch); err != nil {
				s.logger.Error("Failed to write UPDATE RETURNING batch", "error", err)
				transformedBatch.Release()
				return err
			}
			s.logger.Debug("Sent UPDATE RETURNING batch",
				"rows", transformedBatch.NumRows(),
			)
			transformedBatch.Release()
		}
		s.logger.Debug("Writer goroutine completed")
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

	// Determine output schema for RETURNING data
	// For RETURNING, use table schema projected to returning columns (all columns except rowid)
	// because inputSchema only contains rowid, but client expects full table data
	var outputSchema *arrow.Schema
	if returnData {
		outputSchema = catalog.ProjectSchema(table.ArrowSchema(nil), getTableColumnNames(table))
	} else {
		outputSchema = inputSchema
	}

	// Create a writer to send output schema (required for bidirectional exchange)
	writer := NewSchemaWriter(stream, outputSchema, s.allocator)
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

	// Create DMLOptions with RETURNING information
	// Note: DuckDB Airport extension does not communicate which specific columns
	// are in the RETURNING clause. When RETURNING is requested, we populate
	// ReturningColumns with all table columns (excluding pseudo-columns like rowid).
	// DuckDB handles column projection client-side after receiving server response.
	opts := &catalog.DMLOptions{
		Returning: returnData,
	}
	if returnData {
		opts.ReturningColumns = getTableColumnNames(table)
	}

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

	// Processor goroutine: deletes data per-batch and produces RETURNING batches
	// This processes incrementally to avoid deadlock - each batch is deleted
	// and RETURNING data is sent before waiting for more input
	eg.Go(func() error {
		defer close(outputCh)

		for batch := range inputCh {
			totalRows += batch.NumRows()

			if batch.NumCols() == 0 {
				batch.Release()
				continue
			}

			// The first column should be rowid
			rowidCol := batch.Column(0)
			rowIDsFromBatch, err := extractRowIDs(rowidCol)
			batch.Release()
			if err != nil {
				return err
			}

			if len(rowIDsFromBatch) == 0 {
				continue
			}

			// Execute DELETE for this batch with transaction handling
			var dmlResult *catalog.DMLResult
			deleteErr := s.withTransaction(egCtx, func(txCtx context.Context) error {
				var err error
				dmlResult, err = deletableTable.Delete(txCtx, rowIDsFromBatch, opts)
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
		}

		return nil
	})

	// Writer goroutine: sends RETURNING batches back to client
	eg.Go(func() error {
		for batch := range outputCh {
			// Transform batch to match writer's expected schema (outputSchema)
			// The table may return data with slightly different schema (e.g., different metadata)
			transformedBatch := transformBatchSchema(batch, outputSchema, s.allocator)
			if transformedBatch != batch {
				batch.Release() // Release original if transformed
			}
			if err := writer.Write(transformedBatch); err != nil {
				transformedBatch.Release()
				return err
			}
			s.logger.Debug("Sent DELETE RETURNING batch",
				"rows", transformedBatch.NumRows(),
			)
			transformedBatch.Release()
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

// getTableColumnNames returns all column names from the table schema,
// excluding pseudo-columns like rowid (identified by is_rowid metadata).
// This is used to populate ReturningColumns for DML operations.
func getTableColumnNames(table catalog.Table) []string {
	schema := table.ArrowSchema(nil) // Get full schema
	if schema == nil {
		return nil
	}

	columns := make([]string, 0, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		// Skip rowid pseudo-column
		if field.Name == "rowid" {
			continue
		}
		if md := field.Metadata; md.Len() > 0 {
			if idx := md.FindKey("is_rowid"); idx >= 0 && md.Values()[idx] == "true" {
				continue
			}
		}
		columns = append(columns, field.Name)
	}
	return columns
}

// transformBatchSchema creates a new record batch with the target schema.
// This is needed when the table returns data with a slightly different schema
// (e.g., different nullable flags or metadata) than what the writer expects.
// If schemas match, returns the original batch unchanged.
func transformBatchSchema(batch arrow.RecordBatch, targetSchema *arrow.Schema, alloc memory.Allocator) arrow.RecordBatch {
	batchSchema := batch.Schema()
	if batchSchema.Equal(targetSchema) {
		return batch // No transformation needed
	}

	// Build column name to batch column index mapping
	colNameToIdx := make(map[string]int)
	for i := 0; i < batchSchema.NumFields(); i++ {
		colNameToIdx[batchSchema.Field(i).Name] = i
	}

	// Create arrays for each target schema column
	cols := make([]arrow.Array, targetSchema.NumFields())
	numRows := batch.NumRows()
	for i := 0; i < targetSchema.NumFields(); i++ {
		targetField := targetSchema.Field(i)
		if batchIdx, ok := colNameToIdx[targetField.Name]; ok {
			// Column exists in batch - use it directly
			// The data is the same, just the schema metadata differs
			cols[i] = batch.Column(batchIdx)
			cols[i].Retain()
		} else {
			// Column doesn't exist in batch - create null array
			// This happens when RETURNING data doesn't include pseudo-columns like rowid
			cols[i] = makeNullArray(alloc, targetField.Type, int(numRows))
		}
	}

	// Create new record batch with target schema
	newBatch := array.NewRecordBatch(targetSchema, cols, numRows)

	// Release our references to the columns
	for _, col := range cols {
		if col != nil {
			col.Release()
		}
	}

	return newBatch
}

// makeNullArray creates an array of null values for the given Arrow type.
func makeNullArray(alloc memory.Allocator, dt arrow.DataType, numRows int) arrow.Array {
	bldr := array.NewBuilder(alloc, dt)
	defer bldr.Release()
	bldr.AppendNulls(numRows)
	return bldr.NewArray()
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
