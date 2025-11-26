package flight

import (
	"encoding/json"
	"fmt"
)

// TicketData represents the decoded content of a Flight ticket.
// Tickets are opaque byte slices encoding schema/table names for query routing.
type TicketData struct {
	// Schema is the schema name (e.g., "main", "staging")
	Schema string `json:"schema"`

	// Table is the table name (e.g., "users", "orders")
	Table string `json:"table"`
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

// DecodeTicket parses an opaque ticket to extract schema and table names.
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

	return &ticket, nil
}
