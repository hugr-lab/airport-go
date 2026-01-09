package flight

import (
	"encoding/json"
	"fmt"

	"github.com/hugr-lab/airport-go/catalog"
)

// TicketData represents the decoded content of a Flight ticket.
// Tickets are opaque byte slices encoding schema/table/function names for query routing,
// plus optional time-travel parameters for point-in-time queries and function parameters.
type TicketData struct {
	// Catalog is the catalog name (e.g., "default", "analytics") (optional)
	// If empty, default catalog is used
	Catalog string `json:"catalog,omitempty"`

	// Schema is the schema name (e.g., "main", "staging")
	Schema string `json:"schema"`

	// Table is the table name (e.g., "users", "orders")
	// Either Table or TableFunction must be set, but not both
	Table string `json:"table,omitempty"`

	// TableFunction is the table function name (e.g., "read_parquet")
	// Either Table or TableFunction must be set, but not both
	TableFunction string `json:"table_function,omitempty"`

	// FunctionParams are the parameters for table function execution (optional)
	// Only valid when TableFunction is set
	// Parameters are serialized as JSON-compatible values
	FunctionParams []any `json:"function_params,omitempty"`

	// TimePointUnit specifies time granularity for time-travel queries (optional)
	// Valid values: "timestamp", "timestamp_ns", "version", etc.
	// If empty, query returns current data
	TimePointUnit string `json:"time_point_unit,omitempty"`

	// TimePointValue is the time point value (optional)
	// Format depends on TimePointUnit (e.g., "2024-01-01 00:00:00", "1704067200")
	// Only valid when TimePointUnit is set
	TimePointValue string `json:"time_point_value,omitempty"`

	// Columns to project (optional, nil means all columns)
	Columns []string `json:"columns,omitempty"`

	// Filters to apply (optional)
	Filters []byte `json:"filters,omitempty"`
}

// EncodeTicket creates an opaque ticket from schema and table names.
// The ticket is JSON-encoded for simplicity and transparency.
// Returns error if encoding fails.
func EncodeTicket(catalog, schema, table string) ([]byte, error) {
	if schema == "" {
		return nil, fmt.Errorf("schema name cannot be empty")
	}
	if table == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	ticket := TicketData{
		Schema: schema,
		Table:  table,
	}

	data, err := json.Marshal(ticket)
	if err != nil {
		return nil, fmt.Errorf("failed to encode ticket: %w", err)
	}

	return data, nil
}

// DecodeTicket parses an opaque ticket to extract schema and table/function names,
// plus optional time-travel parameters and function parameters.
// Returns error if ticket is invalid or cannot be decoded.
func DecodeTicket(ticketBytes []byte) (*TicketData, error) {
	if len(ticketBytes) == 0 {
		return nil, fmt.Errorf("ticket cannot be empty")
	}

	var ticket TicketData
	if err := json.Unmarshal(ticketBytes, &ticket); err != nil {
		return nil, fmt.Errorf("failed to decode ticket: %w", err)
	}

	if ticket.Schema == "" {
		return nil, fmt.Errorf("decoded ticket has empty schema name")
	}

	// Either Table or TableFunction must be set, but not both
	if ticket.Table == "" && ticket.TableFunction == "" {
		return nil, fmt.Errorf("ticket must have either table or table_function set")
	}
	if ticket.Table != "" && ticket.TableFunction != "" {
		return nil, fmt.Errorf("ticket cannot have both table and table_function set")
	}

	// Function params only valid with table functions
	if ticket.FunctionParams != nil && ticket.TableFunction == "" {
		return nil, fmt.Errorf("function_params only valid with table_function")
	}

	// Validate time point parameters
	if ticket.TimePointUnit != "" && ticket.TimePointValue == "" {
		return nil, fmt.Errorf("time_point_value must be set when time_point_unit is specified")
	}
	if ticket.TimePointValue != "" && ticket.TimePointUnit == "" {
		return nil, fmt.Errorf("time_point_unit must be set when time_point_value is specified")
	}

	return &ticket, nil
}

// ToScanOptions converts TicketData to catalog.ScanOptions with time-travel support.
// This extracts time point parameters and converts them to TimePoint for the catalog layer.
func (td *TicketData) ToScanOptions() *catalog.ScanOptions {
	opts := &catalog.ScanOptions{
		Columns: td.Columns,
		Filter:  td.Filters,
	}

	// Convert time point parameters to TimePoint
	if td.TimePointUnit != "" && td.TimePointValue != "" {
		opts.TimePoint = &catalog.TimePoint{
			Unit:  td.TimePointUnit,
			Value: td.TimePointValue,
		}
	}

	return opts
}
