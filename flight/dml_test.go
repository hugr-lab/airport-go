package flight

import "testing"

func TestDMLError(t *testing.T) {
	tests := []struct {
		name    string
		err     *DMLError
		wantMsg string
	}{
		{
			name: "schema mismatch",
			err: &DMLError{
				Code:    "SCHEMA_MISMATCH",
				Message: "column types do not match",
			},
			wantMsg: "column types do not match",
		},
		{
			name: "invalid rowid",
			err: &DMLError{
				Code:    "INVALID_ROWID_TYPE",
				Message: "rowid column must be Int64, Int32, or Uint64",
			},
			wantMsg: "rowid column must be Int64, Int32, or Uint64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.wantMsg {
				t.Errorf("DMLError.Error() = %v, want %v", got, tt.wantMsg)
			}
		})
	}
}
