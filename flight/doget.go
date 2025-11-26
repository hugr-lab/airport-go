package flight

import (
	"io"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
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

	s.logger.Info("DoGet called", "ticket_size", len(ticket.GetTicket()))

	// Decode ticket to get schema/table names
	ticketData, err := DecodeTicket(ticket.GetTicket())
	if err != nil {
		s.logger.Error("Failed to decode ticket", "error", err)
		return status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}

	s.logger.Info("DoGet request",
		"schema", ticketData.Schema,
		"table", ticketData.Table,
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

	// Look up table in schema
	table, err := schema.Table(ctx, ticketData.Table)
	if err != nil {
		s.logger.Error("Failed to get table from schema",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"error", err,
		)
		return status.Errorf(codes.Internal, "failed to get table: %v", err)
	}
	if table == nil {
		return status.Errorf(codes.NotFound, "table not found: %s.%s", ticketData.Schema, ticketData.Table)
	}

	// Get table's Arrow schema for validation
	tableSchema := table.ArrowSchema()
	if tableSchema == nil {
		s.logger.Error("Table returned nil Arrow schema",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
		)
		return status.Errorf(codes.Internal, "table %s.%s has nil Arrow schema", ticketData.Schema, ticketData.Table)
	}

	// Call table's Scan function to get RecordReader
	reader, err := table.Scan(ctx, &catalog.ScanOptions{})
	if err != nil {
		s.logger.Error("Table scan failed",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"error", err,
		)
		return status.Errorf(codes.Internal, "table scan failed: %v", err)
	}
	defer reader.Release()

	// Validate RecordReader schema matches table schema (T031)
	readerSchema := reader.Schema()
	if !tableSchema.Equal(readerSchema) {
		s.logger.Error("RecordReader schema does not match table schema",
			"schema", ticketData.Schema,
			"table", ticketData.Table,
			"table_schema_fields", tableSchema.NumFields(),
			"reader_schema_fields", readerSchema.NumFields(),
		)
		return status.Errorf(codes.Internal,
			"schema mismatch: table has %d fields, reader has %d fields",
			tableSchema.NumFields(), readerSchema.NumFields())
	}

	s.logger.Info("Starting record streaming",
		"schema", ticketData.Schema,
		"table", ticketData.Table,
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
			s.logger.Info("DoGet cancelled by client",
				"schema", ticketData.Schema,
				"table", ticketData.Table,
				"batches_sent", batchCount,
				"rows_sent", totalRows,
			)
			return status.Error(codes.Canceled, "request cancelled")
		default:
		}

		record := reader.Record()
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
			s.logger.Info("DoGet completed (EOF)",
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

	s.logger.Info("DoGet completed successfully",
		"schema", ticketData.Schema,
		"table", ticketData.Table,
		"batches_sent", batchCount,
		"total_rows", totalRows,
	)

	return nil
}
