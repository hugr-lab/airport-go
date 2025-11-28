package flight

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// handleDMLAction routes DML action requests (currently only DELETE).
// INSERT and UPDATE use DoPut instead of DoAction.
func (s *Server) handleDMLAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {
	case "Delete":
		return s.handleDelete(action.Body, stream)
	default:
		return status.Errorf(codes.Unimplemented, "unknown DML action: %s", action.Type)
	}
}

// handleDelete implements DELETE operation via DoAction.
// DELETE uses DoAction (not DoPut) since it only needs rowids, not data payload.
func (s *Server) handleDelete(body []byte, stream flight.FlightService_DoActionServer) error {
	var req DeleteAction
	if err := json.Unmarshal(body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid Delete payload: %v", err)
	}

	if req.SchemaName == "" {
		return status.Errorf(codes.InvalidArgument, "schema_name is required")
	}
	if req.TableName == "" {
		return status.Errorf(codes.InvalidArgument, "table_name is required")
	}
	if len(req.RowIds) == 0 {
		return status.Errorf(codes.InvalidArgument, "row_ids is required and must not be empty")
	}

	// Placeholder implementation
	// In a real implementation:
	// 1. Look up schema and table in catalog
	// 2. Validate rowids exist
	// 3. Delete specified rows
	// 4. Return affected row count

	result := DMLResult{
		Status:       "success",
		AffectedRows: int64(len(req.RowIds)),
		Message:      fmt.Sprintf("Deleted %d rows from %s.%s (implementation pending)", len(req.RowIds), req.SchemaName, req.TableName),
	}

	respBytes, _ := json.Marshal(result)
	return stream.Send(&flight.Result{Body: respBytes})
}

// handleInsert implements INSERT operation via DoPut.
// This is called from DoPut handler when descriptor indicates an insert.
func (s *Server) handleInsert(descriptor *InsertDescriptor, stream flight.FlightService_DoPutServer) error {
	if descriptor.SchemaName == "" {
		return status.Errorf(codes.InvalidArgument, "schema_name is required")
	}
	if descriptor.TableName == "" {
		return status.Errorf(codes.InvalidArgument, "table_name is required")
	}

	// Placeholder implementation
	// In a real implementation:
	// 1. Validate table exists and get schema
	// 2. Read RecordBatch stream from client
	// 3. Validate each batch matches table schema
	// 4. Insert rows into table
	// 5. Return affected row count

	affectedRows := int64(0)

	// Read record batches from stream
	for {
		msg, err := stream.Recv()
		if err != nil {
			// End of stream or error
			break
		}

		// In real implementation, would decode RecordBatch from msg.DataBody
		// and insert into catalog
		_ = msg
		affectedRows++ // Placeholder - would count actual rows
	}

	result := &flight.PutResult{
		// AppMetadata could contain affected row count
		// For now, return empty result
	}

	return stream.Send(result)
}

// handleUpdate implements UPDATE operation via DoPut.
// This is called from DoPut handler when descriptor indicates an update.
func (s *Server) handleUpdate(descriptor *UpdateDescriptor, stream flight.FlightService_DoPutServer) error {
	if descriptor.SchemaName == "" {
		return status.Errorf(codes.InvalidArgument, "schema_name is required")
	}
	if descriptor.TableName == "" {
		return status.Errorf(codes.InvalidArgument, "table_name is required")
	}
	if len(descriptor.RowIds) == 0 {
		return status.Errorf(codes.InvalidArgument, "row_ids is required and must not be empty")
	}

	// Placeholder implementation
	// In a real implementation:
	// 1. Validate table exists and get schema
	// 2. Validate rowids exist
	// 3. Read RecordBatch stream with updated values
	// 4. Validate batch schema is compatible with table
	// 5. Update specified rows
	// 6. Return affected row count

	affectedRows := int64(0)

	// Read record batches from stream
	for {
		msg, err := stream.Recv()
		if err != nil {
			// End of stream or error
			break
		}

		// In real implementation, would decode RecordBatch and update rows
		_ = msg
		affectedRows++ // Placeholder
	}

	result := &flight.PutResult{
		// AppMetadata could contain affected row count
	}

	return stream.Send(result)
}

// validateSchemaCompatibility checks if a RecordBatch schema matches the table schema.
// validateSchemaCompatibility validates that a batch schema is compatible with a table schema.
// This is used to validate INSERT and UPDATE operations.
// Currently unused but kept for future implementation.
// nolint:unused
func validateSchemaCompatibility(batchSchema, tableSchema interface{}) error {
	// Placeholder - would implement actual schema validation:
	// 1. Check all batch columns exist in table
	// 2. Check types are compatible
	// 3. Check nullability constraints
	// 4. Return detailed error if mismatch
	return nil
}
