package flight

// DML operation descriptor types for Flight DoPut and DoAction RPCs.
// These structures represent operation metadata sent with data streams.

// InsertDescriptor represents metadata for an INSERT operation via DoPut.
// The actual data is sent as Arrow RecordBatch stream.
type InsertDescriptor struct {
	Operation  string `json:"operation"`            // Always "insert"
	SchemaName string `json:"schema_name"`          // Target schema
	TableName  string `json:"table_name"`           // Target table
	Returning  bool   `json:"returning,omitempty"`  // Request RETURNING data
}

// UpdateDescriptor represents metadata for an UPDATE operation via DoPut.
// The RecordBatch must include updated column values.
// RowIds identify which rows to modify using the rowid pseudocolumn.
type UpdateDescriptor struct {
	Operation  string  `json:"operation"`           // Always "update"
	SchemaName string  `json:"schema_name"`         // Target schema
	TableName  string  `json:"table_name"`          // Target table
	RowIds     []int64 `json:"row_ids"`             // Rows to update (via rowid pseudocolumn)
	Returning  bool    `json:"returning,omitempty"` // Request RETURNING data
}

// DeleteAction represents a DELETE operation via DoAction.
// This uses DoAction (not DoPut) since no data payload is needed.
type DeleteAction struct {
	SchemaName string  `json:"schema_name"`         // Target schema
	TableName  string  `json:"table_name"`          // Target table
	RowIds     []int64 `json:"row_ids"`             // Rows to delete (via rowid pseudocolumn)
	Returning  bool    `json:"returning,omitempty"` // Request RETURNING data
}

// DMLResultJSON represents the JSON-encoded result of a DML operation.
// Used for DoAction responses (DELETE).
type DMLResultJSON struct {
	Status       string `json:"status"`                  // "success" or "error"
	AffectedRows int64  `json:"affected_rows"`           // Number of rows affected
	Message      string `json:"message,omitempty"`       // Optional status message
	ErrorCode    string `json:"error_code,omitempty"`    // Error code if status="error"
	ReturningData []byte `json:"returning_data,omitempty"` // Arrow IPC bytes if returning=true
}

// DMLResponse is the MessagePack-encoded result of DML operations via DoPut.
// Used for INSERT and UPDATE responses in PutResult.AppMetadata.
type DMLResponse struct {
	Status        string `msgpack:"status"`                   // "success" or "error"
	AffectedRows  int64  `msgpack:"affected_rows"`            // Count of affected rows
	ReturningData []byte `msgpack:"returning_data,omitempty"` // Arrow IPC bytes
	ErrorMessage  string `msgpack:"error_message,omitempty"`  // Error details
}

// DMLError represents a DML operation error.
type DMLError struct {
	Code    string // Error code (e.g., "SCHEMA_MISMATCH", "INVALID_ROWID")
	Message string // Human-readable error message
}

func (e *DMLError) Error() string {
	return e.Message
}
