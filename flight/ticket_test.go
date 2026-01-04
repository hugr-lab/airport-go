package flight

import (
	"encoding/json"
	"testing"
)

func TestEncodeDecodeTicket(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		table  string
	}{
		{
			name:   "simple table",
			schema: "main",
			table:  "users",
		},
		{
			name:   "schema with underscore",
			schema: "my_schema",
			table:  "my_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := EncodeTicket("", tt.schema, tt.table)
			if err != nil {
				t.Fatalf("EncodeTicket() error = %v", err)
			}

			// Decode
			decoded, err := DecodeTicket(encoded)
			if err != nil {
				t.Fatalf("DecodeTicket() error = %v", err)
			}

			// Verify
			if decoded.Schema != tt.schema {
				t.Errorf("Schema = %v, want %v", decoded.Schema, tt.schema)
			}
			if decoded.Table != tt.table {
				t.Errorf("Table = %v, want %v", decoded.Table, tt.table)
			}
		})
	}
}

func TestDecodeTicketWithTimePoint(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		wantUnit  string
		wantValue string
		wantError bool
	}{
		{
			name:      "no time point",
			json:      `{"schema":"main","table":"users"}`,
			wantUnit:  "",
			wantValue: "",
			wantError: false,
		},
		{
			name:      "with timestamp",
			json:      `{"schema":"main","table":"users","time_point_unit":"timestamp","time_point_value":"1704067200"}`,
			wantUnit:  "timestamp",
			wantValue: "1704067200",
			wantError: false,
		},
		{
			name:      "with timestamp_ns",
			json:      `{"schema":"main","table":"users","time_point_unit":"timestamp_ns","time_point_value":"1704067200000000000"}`,
			wantUnit:  "timestamp_ns",
			wantValue: "1704067200000000000",
			wantError: false,
		},
		{
			name:      "with version",
			json:      `{"schema":"main","table":"users","time_point_unit":"version","time_point_value":"42"}`,
			wantUnit:  "version",
			wantValue: "42",
			wantError: false,
		},
		{
			name:      "unit without value - error",
			json:      `{"schema":"main","table":"users","time_point_unit":"timestamp"}`,
			wantError: true,
		},
		{
			name:      "value without unit - error",
			json:      `{"schema":"main","table":"users","time_point_value":"1704067200"}`,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := DecodeTicket([]byte(tt.json))

			if (err != nil) != tt.wantError {
				t.Errorf("DecodeTicket() error = %v, wantError %v", err, tt.wantError)
				return
			}

			if tt.wantError {
				return
			}

			// Verify time point fields
			if decoded.TimePointUnit != tt.wantUnit {
				t.Errorf("TimePointUnit = %v, want %v", decoded.TimePointUnit, tt.wantUnit)
			}
			if decoded.TimePointValue != tt.wantValue {
				t.Errorf("TimePointValue = %v, want %v", decoded.TimePointValue, tt.wantValue)
			}
		})
	}
}

func TestToScanOptions(t *testing.T) {
	tests := []struct {
		name             string
		ticket           TicketData
		wantTimeUnit     string
		wantTimeValue    string
		wantTimePointNil bool
	}{
		{
			name: "no timestamp",
			ticket: TicketData{
				Schema: "main",
				Table:  "users",
			},
			wantTimePointNil: true,
		},
		{
			name: "with timestamp",
			ticket: TicketData{
				Schema:         "main",
				Table:          "users",
				TimePointUnit:  "timestamp",
				TimePointValue: "1704067200",
			},
			wantTimeUnit:     "timestamp",
			wantTimeValue:    "1704067200",
			wantTimePointNil: false,
		},
		{
			name: "with timestamp_ns",
			ticket: TicketData{
				Schema:         "main",
				Table:          "users",
				TimePointUnit:  "timestamp_ns",
				TimePointValue: "1704067200000000000",
			},
			wantTimeUnit:     "timestamp_ns",
			wantTimeValue:    "1704067200000000000",
			wantTimePointNil: false,
		},
		{
			name: "with version",
			ticket: TicketData{
				Schema:         "main",
				Table:          "users",
				TimePointUnit:  "version",
				TimePointValue: "42",
			},
			wantTimeUnit:     "version",
			wantTimeValue:    "42",
			wantTimePointNil: false,
		},
		{
			name: "with columns",
			ticket: TicketData{
				Schema:  "main",
				Table:   "users",
				Columns: []string{"id", "name"},
			},
			wantTimePointNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.ticket.ToScanOptions()

			if opts == nil {
				t.Fatal("ToScanOptions() returned nil")
			}

			// Check TimePoint
			if tt.wantTimePointNil {
				if opts.TimePoint != nil {
					t.Errorf("TimePoint = %v, want nil", opts.TimePoint)
				}
			} else {
				if opts.TimePoint == nil {
					t.Fatal("TimePoint is nil, want non-nil")
				}
				if opts.TimePoint.Unit != tt.wantTimeUnit {
					t.Errorf("TimePoint.Unit = %v, want %v", opts.TimePoint.Unit, tt.wantTimeUnit)
				}
				if opts.TimePoint.Value != tt.wantTimeValue {
					t.Errorf("TimePoint.Value = %v, want %v", opts.TimePoint.Value, tt.wantTimeValue)
				}
			}

			// Check columns
			if len(tt.ticket.Columns) > 0 {
				if len(opts.Columns) != len(tt.ticket.Columns) {
					t.Errorf("Columns length = %v, want %v", len(opts.Columns), len(tt.ticket.Columns))
				}
			}
		})
	}
}

func TestEncodeTicketValidation(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		table     string
		wantError bool
	}{
		{
			name:      "valid",
			schema:    "main",
			table:     "users",
			wantError: false,
		},
		{
			name:      "empty schema",
			schema:    "",
			table:     "users",
			wantError: true,
		},
		{
			name:      "empty table",
			schema:    "main",
			table:     "",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := EncodeTicket("", tt.schema, tt.table)

			if (err != nil) != tt.wantError {
				t.Errorf("EncodeTicket() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestDecodeTicketValidation(t *testing.T) {
	tests := []struct {
		name      string
		ticket    []byte
		wantError bool
	}{
		{
			name:      "valid ticket",
			ticket:    []byte(`{"schema":"main","table":"users"}`),
			wantError: false,
		},
		{
			name:      "empty ticket",
			ticket:    []byte{},
			wantError: true,
		},
		{
			name:      "invalid JSON",
			ticket:    []byte(`{"schema":"main"`),
			wantError: true,
		},
		{
			name:      "missing schema",
			ticket:    []byte(`{"table":"users"}`),
			wantError: true,
		},
		{
			name:      "missing table",
			ticket:    []byte(`{"schema":"main"}`),
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeTicket(tt.ticket)

			if (err != nil) != tt.wantError {
				t.Errorf("DecodeTicket() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestTicketDataJSON(t *testing.T) {
	// Test round-trip JSON encoding
	original := TicketData{
		Schema:         "main",
		Table:          "users",
		TimePointUnit:  "timestamp",
		TimePointValue: "1704067200",
		Columns:        []string{"id", "name"},
	}

	// Marshal
	jsonBytes, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	// Unmarshal
	var decoded TicketData
	if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	// Verify
	if decoded.Schema != original.Schema {
		t.Errorf("Schema = %v, want %v", decoded.Schema, original.Schema)
	}
	if decoded.Table != original.Table {
		t.Errorf("Table = %v, want %v", decoded.Table, original.Table)
	}
	if decoded.TimePointUnit != original.TimePointUnit {
		t.Errorf("TimePointUnit = %v, want %v", decoded.TimePointUnit, original.TimePointUnit)
	}
	if decoded.TimePointValue != original.TimePointValue {
		t.Errorf("TimePointValue = %v, want %v", decoded.TimePointValue, original.TimePointValue)
	}
}
