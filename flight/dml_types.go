package flight

// DMLError represents a DML operation error.
type DMLError struct {
	Code    string // Error code (e.g., "SCHEMA_MISMATCH", "INVALID_ROWID")
	Message string // Human-readable error message
}

func (e *DMLError) Error() string {
	return e.Message
}
