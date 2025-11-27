package flight

import (
	"encoding/json"
	"fmt"

	"github.com/hugr-lab/airport-go/catalog"
)

// TicketData represents the decoded content of a Flight ticket.
// Tickets are opaque byte slices encoding schema/table names for query routing,
// plus optional time-travel parameters for point-in-time queries.
type TicketData struct {
	// Schema is the schema name (e.g., "main", "staging")
	Schema string `json:"schema"`

	// Table is the table name (e.g., "users", "orders")
	Table string `json:"table"`

	// Ts is Unix timestamp in seconds for point-in-time queries (optional)
	// If both Ts and TsNs are nil, query returns current data
	Ts *int64 `json:"ts,omitempty"`

	// TsNs is Unix timestamp in nanoseconds for point-in-time queries (optional)
	// At most one of Ts or TsNs can be set
	TsNs *int64 `json:"ts_ns,omitempty"`

	// Columns to project (optional, nil means all columns)
	Columns []string `json:"columns,omitempty"`
}

// EncodeTicket creates an opaque ticket from schema and table names.
// The ticket is JSON-encoded for simplicity and transparency.
// Returns error if encoding fails.
func EncodeTicket(schema, table string) ([]byte, error) {
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

// DecodeTicket parses an opaque ticket to extract schema and table names,
// plus optional time-travel parameters.
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
	if ticket.Table == "" {
		return nil, fmt.Errorf("decoded ticket has empty table name")
	}

	// Validate timestamp parameters
	if ticket.Ts != nil && ticket.TsNs != nil {
		return nil, fmt.Errorf("at most one of ts or ts_ns can be set")
	}

	if ticket.Ts != nil && *ticket.Ts < 0 {
		return nil, fmt.Errorf("ts must be non-negative, got %d", *ticket.Ts)
	}

	if ticket.TsNs != nil && *ticket.TsNs < 0 {
		return nil, fmt.Errorf("ts_ns must be non-negative, got %d", *ticket.TsNs)
	}

	return &ticket, nil
}

// ToScanOptions converts TicketData to catalog.ScanOptions with time-travel support.
// This extracts timestamp parameters and converts them to TimePoint for the catalog layer.
func (td *TicketData) ToScanOptions() *catalog.ScanOptions {
	opts := &catalog.ScanOptions{
		Columns: td.Columns,
	}

	// Convert timestamp parameters to TimePoint
	if td.Ts != nil {
		opts.TimePoint = &catalog.TimePoint{
			Unit:  "timestamp",
			Value: fmt.Sprintf("%d", *td.Ts),
		}
	} else if td.TsNs != nil {
		opts.TimePoint = &catalog.TimePoint{
			Unit:  "timestamp_ns",
			Value: fmt.Sprintf("%d", *td.TsNs),
		}
	}

	return opts
}
