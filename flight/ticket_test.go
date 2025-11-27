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
			encoded, err := EncodeTicket(tt.schema, tt.table)
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

func TestDecodeTicketWithTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		wantTs    *int64
		wantTsNs  *int64
		wantError bool
	}{
		{
			name:      "no timestamp",
			json:      `{"schema":"main","table":"users"}`,
			wantTs:    nil,
			wantTsNs:  nil,
			wantError: false,
		},
		{
			name:      "with ts",
			json:      `{"schema":"main","table":"users","ts":1704067200}`,
			wantTs:    int64Ptr(1704067200),
			wantTsNs:  nil,
			wantError: false,
		},
		{
			name:      "with ts_ns",
			json:      `{"schema":"main","table":"users","ts_ns":1704067200000000000}`,
			wantTs:    nil,
			wantTsNs:  int64Ptr(1704067200000000000),
			wantError: false,
		},
		{
			name:      "both ts and ts_ns - error",
			json:      `{"schema":"main","table":"users","ts":1704067200,"ts_ns":1704067200000000000}`,
			wantError: true,
		},
		{
			name:      "negative ts",
			json:      `{"schema":"main","table":"users","ts":-1}`,
			wantError: true,
		},
		{
			name:      "negative ts_ns",
			json:      `{"schema":"main","table":"users","ts_ns":-1}`,
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

			// Verify timestamp fields
			if !int64PtrEqual(decoded.Ts, tt.wantTs) {
				t.Errorf("Ts = %v, want %v", ptrValue(decoded.Ts), ptrValue(tt.wantTs))
			}
			if !int64PtrEqual(decoded.TsNs, tt.wantTsNs) {
				t.Errorf("TsNs = %v, want %v", ptrValue(decoded.TsNs), ptrValue(tt.wantTsNs))
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
			name: "with ts",
			ticket: TicketData{
				Schema: "main",
				Table:  "users",
				Ts:     int64Ptr(1704067200),
			},
			wantTimeUnit:     "timestamp",
			wantTimeValue:    "1704067200",
			wantTimePointNil: false,
		},
		{
			name: "with ts_ns",
			ticket: TicketData{
				Schema: "main",
				Table:  "users",
				TsNs:   int64Ptr(1704067200000000000),
			},
			wantTimeUnit:     "timestamp_ns",
			wantTimeValue:    "1704067200000000000",
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
			_, err := EncodeTicket(tt.schema, tt.table)

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
		Schema:  "main",
		Table:   "users",
		Ts:      int64Ptr(1704067200),
		Columns: []string{"id", "name"},
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
	if !int64PtrEqual(decoded.Ts, original.Ts) {
		t.Errorf("Ts = %v, want %v", ptrValue(decoded.Ts), ptrValue(original.Ts))
	}
}

// Helper functions
func int64Ptr(v int64) *int64 {
	return &v
}

func int64PtrEqual(a, b *int64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func ptrValue(p *int64) interface{} {
	if p == nil {
		return nil
	}
	return *p
}
