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
	ctx := EnrichContextMetadata(stream.Context())

	s.logger.Debug("DoGet called", "ticket_size", len(ticket.GetTicket()))

	// Decode ticket to get schema/table names
	ticketData, err := DecodeTicket(ticket.GetTicket())
	if err != nil {
		s.logger.Error("Failed to decode ticket", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}

	s.logger.Debug("DoGet request",
		"catalog", ticketData.Catalog,
		"schema", ticketData.Schema,
		"table", ticketData.Table,
		"table_function", ticketData.TableFunction,
	)

	if ticketData.Catalog != s.CatalogName() {
		s.logger.Error("Catalog name mismatch", "expected", s.CatalogName(), "got", ticketData.Catalog)
		return status.Errorf(codes.InvalidArgument, "catalog name mismatch: expected %q, got %q", s.CatalogName(), ticketData.Catalog)
	}

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
	} else {
		// Handle regular table scan
		reader, readerSchema, err = s.executeTableScan(ctx, schema, ticketData)
	}
	if err != nil {
		return err // Error already formatted
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

	// Convert ticket data to scan options (includes time-travel parameters and columns)
	scanOpts := ticketData.ToScanOptions()

	// Get table's full Arrow schema (nil = no projection, DuckDB expects full schema in DoGet)
	fullSchema := table.ArrowSchema(nil)
	if fullSchema == nil {
		s.logger.Error("Table returned nil Arrow schema",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
		)
		return nil, nil, status.Errorf(codes.Internal, "table %s.%s has nil Arrow schema", ticketData.Schema, ticketData.Table)
	}

	// Log column projection hint (passed to table for optimization)
	if len(scanOpts.Columns) > 0 {
		s.logger.Debug("Column projection hint",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"columns", scanOpts.Columns,
		)
	}

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
	// Table can use scanOpts.Columns to optimize (e.g., only fetch needed columns from DB)
	// but must return full schema - DuckDB handles projection client-side
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
	// Skip validation for time-travel queries (schema may differ from current schema)
	readerSchema := reader.Schema()
	if scanOpts.TimePoint == nil && !fullSchema.Equal(readerSchema) {
		reader.Release()
		s.logger.Error("RecordReader schema does not match table schema",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"table_schema_fields", fullSchema.NumFields(),
			"reader_schema_fields", readerSchema.NumFields(),
		)
		return nil, nil, status.Errorf(codes.Internal,
			"schema mismatch: table has %d fields, reader has %d fields",
			fullSchema.NumFields(), readerSchema.NumFields())
	}

	// For time-travel queries, use the reader's schema (which reflects historical state)
	if scanOpts.TimePoint != nil {
		s.logger.Debug("Time-travel query: using reader schema",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"reader_fields", readerSchema.NumFields(),
			"current_table_fields", fullSchema.NumFields(),
		)
		return reader, readerSchema, nil
	}

	return reader, fullSchema, nil
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

	// Get the full schema for these parameters (dynamic schema based on params)
	fullSchema, err := targetFunc.SchemaForParameters(ctx, ticketData.FunctionParams)
	if err != nil {
		s.logger.Error("Failed to get function schema",
			"schema", ticketData.Schema,
			"function", ticketData.TableFunction,
			"error", err,
		)
		return nil, nil, status.Errorf(codes.Internal, "failed to get function schema: %v", err)
	}

	// Convert ticket data to scan options (includes column projection hint)
	scanOpts := ticketData.ToScanOptions()

	// Log column projection hint (passed to function for optimization)
	if len(scanOpts.Columns) > 0 {
		s.logger.Debug("Table function column projection hint",
			"schema", ticketData.Schema,
			"function", ticketData.TableFunction,
			"columns", scanOpts.Columns,
		)
	}

	s.logger.Debug("Table function schema determined",
		"schema", ticketData.Schema,
		"function", ticketData.TableFunction,
		"output_fields", fullSchema.NumFields(),
	)

	// Execute the table function
	// Function can use scanOpts.Columns to optimize (e.g., skip computing unused columns)
	// but must return full schema - DuckDB handles projection client-side
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
	if !fullSchema.Equal(readerSchema) {
		reader.Release()
		s.logger.Error("RecordReader schema does not match function schema",
			"schema", ticketData.Schema,
			"function", ticketData.TableFunction,
			"function_schema_fields", fullSchema.NumFields(),
			"reader_schema_fields", readerSchema.NumFields(),
		)
		return nil, nil, status.Errorf(codes.Internal,
			"schema mismatch: function declared %d fields, reader has %d fields",
			fullSchema.NumFields(), readerSchema.NumFields())
	}

	return reader, fullSchema, nil
}
