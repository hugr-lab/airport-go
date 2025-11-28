package flight

// DML operation descriptor types for Flight DoPut and DoAction RPCs.
// These structures represent operation metadata sent with data streams.

// InsertDescriptor represents metadata for an INSERT operation via DoPut.
// The actual data is sent as Arrow RecordBatch stream.
type InsertDescriptor struct {
	SchemaName string `json:"schema_name"` // Target schema
	TableName  string `json:"table_name"`  // Target table
}

// UpdateDescriptor represents metadata for an UPDATE operation via DoPut.
// The RecordBatch must include updated column values.
// RowIds identify which rows to modify using the rowid pseudocolumn.
type UpdateDescriptor struct {
	SchemaName string   `json:"schema_name"` // Target schema
	TableName  string   `json:"table_name"`  // Target table
	RowIds     []int64  `json:"row_ids"`     // Rows to update (via rowid pseudocolumn)
}

// DeleteAction represents a DELETE operation via DoAction.
// This uses DoAction (not DoPut) since no data payload is needed.
type DeleteAction struct {
	SchemaName string   `json:"schema_name"` // Target schema
	TableName  string   `json:"table_name"`  // Target table
	RowIds     []int64  `json:"row_ids"`     // Rows to delete (via rowid pseudocolumn)
}

// DMLResult represents the result of a DML operation.
type DMLResult struct {
	Status       string `json:"status"`        // "success" or "error"
	AffectedRows int64  `json:"affected_rows"` // Number of rows affected
	Message      string `json:"message,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
}

// DMLError represents a DML operation error.
type DMLError struct {
	Code    string // Error code (e.g., "SCHEMA_MISMATCH", "INVALID_ROWID")
	Message string // Human-readable error message
}

func (e *DMLError) Error() string {
	return e.Message
}
