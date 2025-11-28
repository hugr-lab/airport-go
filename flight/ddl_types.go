package flight

import "github.com/apache/arrow-go/v18/arrow"

// DDL action payload types for Flight DoAction RPC.
// These structures represent JSON payloads sent in action.Body.

// CreateSchemaAction represents a CREATE SCHEMA DDL command.
type CreateSchemaAction struct {
	SchemaName  string `json:"schema_name"`       // Name of schema to create
	IfNotExists bool   `json:"if_not_exists"`     // If true, succeed silently if schema exists
	Comment     string `json:"comment,omitempty"` // Optional schema comment
}

// DropSchemaAction represents a DROP SCHEMA DDL command.
type DropSchemaAction struct {
	SchemaName string `json:"schema_name"` // Name of schema to drop
	IfExists   bool   `json:"if_exists"`   // If true, succeed silently if schema doesn't exist
	Cascade    bool   `json:"cascade"`     // If true, drop all tables in schema
}

// CreateTableAction represents a CREATE TABLE DDL command.
type CreateTableAction struct {
	SchemaName  string              `json:"schema_name"`       // Parent schema
	TableName   string              `json:"table_name"`        // Name of table to create
	IfNotExists bool                `json:"if_not_exists"`     // Idempotent flag
	Schema      *ArrowSchemaPayload `json:"schema"`            // Arrow schema definition
	Comment     string              `json:"comment,omitempty"` // Optional table comment
}

// ArrowSchemaPayload represents an Arrow schema in JSON format.
type ArrowSchemaPayload struct {
	Fields   []ArrowFieldPayload `json:"fields"`
	Metadata map[string]string   `json:"metadata,omitempty"`
}

// ArrowFieldPayload represents an Arrow field in JSON format.
type ArrowFieldPayload struct {
	Name     string            `json:"name"`
	Type     string            `json:"type"` // Arrow type string (e.g., "int64", "utf8", "extension<geoarrow.wkb>")
	Nullable bool              `json:"nullable"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// DropTableAction represents a DROP TABLE DDL command.
type DropTableAction struct {
	SchemaName string `json:"schema_name"` // Parent schema
	TableName  string `json:"table_name"`  // Name of table to drop
	IfExists   bool   `json:"if_exists"`   // Idempotent flag
}

// AlterTableAddColumnAction represents an ALTER TABLE ADD COLUMN DDL command.
type AlterTableAddColumnAction struct {
	SchemaName string            `json:"schema_name"` // Parent schema
	TableName  string            `json:"table_name"`  // Table to alter
	IfExists   bool              `json:"if_exists"`   // Idempotent flag (for table existence)
	Column     ArrowFieldPayload `json:"column"`      // Column to add
}

// AlterTableDropColumnAction represents an ALTER TABLE DROP COLUMN DDL command.
type AlterTableDropColumnAction struct {
	SchemaName string `json:"schema_name"` // Parent schema
	TableName  string `json:"table_name"`  // Table to alter
	IfExists   bool   `json:"if_exists"`   // Idempotent flag
	ColumnName string `json:"column_name"` // Column to drop
}

// parseArrowType converts a type string to an Arrow DataType.
// Supports common types and extension types like "extension<geoarrow.wkb>".
func parseArrowType(typeStr string) (arrow.DataType, error) {
	// Handle extension types
	if len(typeStr) > 10 && typeStr[:10] == "extension<" && typeStr[len(typeStr)-1] == '>' {
		extName := typeStr[10 : len(typeStr)-1]

		// Handle geometry extension type
		if extName == "geoarrow.wkb" || extName == "airport.geometry" {
			// Return a binary type for WKB storage
			// The extension metadata should be set separately via field metadata
			return arrow.BinaryTypes.Binary, nil
		}

		// Unknown extension type
		return nil, &DDLError{
			Code:    "INVALID_TYPE",
			Message: "unknown extension type: " + extName,
		}
	}

	// Handle standard Arrow types
	switch typeStr {
	// Numeric types
	case "int8":
		return arrow.PrimitiveTypes.Int8, nil
	case "int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "uint16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "uint32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "uint64":
		return arrow.PrimitiveTypes.Uint64, nil
	case "float32", "float":
		return arrow.PrimitiveTypes.Float32, nil
	case "float64", "double":
		return arrow.PrimitiveTypes.Float64, nil

	// String types
	case "utf8", "string":
		return arrow.BinaryTypes.String, nil
	case "large_utf8", "large_string":
		return arrow.BinaryTypes.LargeString, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "large_binary":
		return arrow.BinaryTypes.LargeBinary, nil

	// Boolean
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean, nil

	// Temporal types
	case "date32":
		return arrow.FixedWidthTypes.Date32, nil
	case "date64":
		return arrow.FixedWidthTypes.Date64, nil
	case "timestamp":
		// Default to microsecond precision, UTC
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case "time32":
		return arrow.FixedWidthTypes.Time32ms, nil
	case "time64":
		return arrow.FixedWidthTypes.Time64us, nil

	default:
		return nil, &DDLError{
			Code:    "INVALID_TYPE",
			Message: "unsupported Arrow type: " + typeStr,
		}
	}
}

// DDLError represents a DDL operation error.
type DDLError struct {
	Code    string // Error code (e.g., "SCHEMA_EXISTS", "INVALID_TYPE")
	Message string // Human-readable error message
}

func (e *DDLError) Error() string {
	return e.Message
}
