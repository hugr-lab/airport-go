package flight

import (
	"encoding/json"
	"testing"
)

func TestInsertDescriptorJSON(t *testing.T) {
	tests := []struct {
		name       string
		descriptor InsertDescriptor
		wantJSON   string
	}{
		{
			name: "basic insert",
			descriptor: InsertDescriptor{
				Operation:  "insert",
				SchemaName: "main",
				TableName:  "users",
			},
			wantJSON: `{"operation":"insert","schema_name":"main","table_name":"users"}`,
		},
		{
			name: "with special characters",
			descriptor: InsertDescriptor{
				Operation:  "insert",
				SchemaName: "my_schema",
				TableName:  "my_table_2024",
			},
			wantJSON: `{"operation":"insert","schema_name":"my_schema","table_name":"my_table_2024"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			gotJSON, err := json.Marshal(tt.descriptor)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}

			if string(gotJSON) != tt.wantJSON {
				t.Errorf("JSON = %s, want %s", string(gotJSON), tt.wantJSON)
			}

			// Unmarshal back
			var got InsertDescriptor
			if err := json.Unmarshal([]byte(tt.wantJSON), &got); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}

			if got.SchemaName != tt.descriptor.SchemaName {
				t.Errorf("SchemaName = %v, want %v", got.SchemaName, tt.descriptor.SchemaName)
			}
			if got.TableName != tt.descriptor.TableName {
				t.Errorf("TableName = %v, want %v", got.TableName, tt.descriptor.TableName)
			}
		})
	}
}

func TestUpdateDescriptorJSON(t *testing.T) {
	tests := []struct {
		name       string
		descriptor UpdateDescriptor
		wantJSON   string
	}{
		{
			name: "update with rowids",
			descriptor: UpdateDescriptor{
				Operation:  "update",
				SchemaName: "main",
				TableName:  "users",
				RowIds:     []int64{1, 2, 3},
			},
			wantJSON: `{"operation":"update","schema_name":"main","table_name":"users","row_ids":[1,2,3]}`,
		},
		{
			name: "update single row",
			descriptor: UpdateDescriptor{
				Operation:  "update",
				SchemaName: "test_schema",
				TableName:  "test_table",
				RowIds:     []int64{42},
			},
			wantJSON: `{"operation":"update","schema_name":"test_schema","table_name":"test_table","row_ids":[42]}`,
		},
		{
			name: "update many rows",
			descriptor: UpdateDescriptor{
				Operation:  "update",
				SchemaName: "main",
				TableName:  "large_table",
				RowIds:     []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
			wantJSON: `{"operation":"update","schema_name":"main","table_name":"large_table","row_ids":[1,2,3,4,5,6,7,8,9,10]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			gotJSON, err := json.Marshal(tt.descriptor)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}

			if string(gotJSON) != tt.wantJSON {
				t.Errorf("JSON = %s, want %s", string(gotJSON), tt.wantJSON)
			}

			// Unmarshal back
			var got UpdateDescriptor
			if err := json.Unmarshal([]byte(tt.wantJSON), &got); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}

			if got.SchemaName != tt.descriptor.SchemaName {
				t.Errorf("SchemaName = %v, want %v", got.SchemaName, tt.descriptor.SchemaName)
			}
			if got.TableName != tt.descriptor.TableName {
				t.Errorf("TableName = %v, want %v", got.TableName, tt.descriptor.TableName)
			}
			if len(got.RowIds) != len(tt.descriptor.RowIds) {
				t.Errorf("RowIds length = %v, want %v", len(got.RowIds), len(tt.descriptor.RowIds))
			}
			for i, rowid := range got.RowIds {
				if rowid != tt.descriptor.RowIds[i] {
					t.Errorf("RowIds[%d] = %v, want %v", i, rowid, tt.descriptor.RowIds[i])
				}
			}
		})
	}
}

func TestDeleteActionJSON(t *testing.T) {
	tests := []struct {
		name     string
		action   DeleteAction
		wantJSON string
	}{
		{
			name: "delete with rowids",
			action: DeleteAction{
				SchemaName: "main",
				TableName:  "users",
				RowIds:     []int64{1, 2, 3},
			},
			wantJSON: `{"schema_name":"main","table_name":"users","row_ids":[1,2,3]}`,
		},
		{
			name: "delete single row",
			action: DeleteAction{
				SchemaName: "test_schema",
				TableName:  "test_table",
				RowIds:     []int64{999},
			},
			wantJSON: `{"schema_name":"test_schema","table_name":"test_table","row_ids":[999]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			gotJSON, err := json.Marshal(tt.action)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}

			if string(gotJSON) != tt.wantJSON {
				t.Errorf("JSON = %s, want %s", string(gotJSON), tt.wantJSON)
			}

			// Unmarshal back
			var got DeleteAction
			if err := json.Unmarshal([]byte(tt.wantJSON), &got); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}

			if got.SchemaName != tt.action.SchemaName {
				t.Errorf("SchemaName = %v, want %v", got.SchemaName, tt.action.SchemaName)
			}
			if got.TableName != tt.action.TableName {
				t.Errorf("TableName = %v, want %v", got.TableName, tt.action.TableName)
			}
			if len(got.RowIds) != len(tt.action.RowIds) {
				t.Errorf("RowIds length = %v, want %v", len(got.RowIds), len(tt.action.RowIds))
			}
		})
	}
}

func TestDMLResultJSONType(t *testing.T) {
	tests := []struct {
		name     string
		result   DMLResultJSON
		wantJSON string
	}{
		{
			name: "success result",
			result: DMLResultJSON{
				Status:       "success",
				AffectedRows: 5,
				Message:      "Deleted 5 rows",
			},
			wantJSON: `{"status":"success","affected_rows":5,"message":"Deleted 5 rows"}`,
		},
		{
			name: "error result",
			result: DMLResultJSON{
				Status:       "error",
				AffectedRows: 0,
				Message:      "Table not found",
				ErrorCode:    "TABLE_NOT_FOUND",
			},
			wantJSON: `{"status":"error","affected_rows":0,"message":"Table not found","error_code":"TABLE_NOT_FOUND"}`,
		},
		{
			name: "success without message",
			result: DMLResultJSON{
				Status:       "success",
				AffectedRows: 10,
			},
			wantJSON: `{"status":"success","affected_rows":10}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			gotJSON, err := json.Marshal(tt.result)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}

			if string(gotJSON) != tt.wantJSON {
				t.Errorf("JSON = %s, want %s", string(gotJSON), tt.wantJSON)
			}

			// Unmarshal back
			var got DMLResultJSON
			if err := json.Unmarshal([]byte(tt.wantJSON), &got); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}

			if got.Status != tt.result.Status {
				t.Errorf("Status = %v, want %v", got.Status, tt.result.Status)
			}
			if got.AffectedRows != tt.result.AffectedRows {
				t.Errorf("AffectedRows = %v, want %v", got.AffectedRows, tt.result.AffectedRows)
			}
			if got.Message != tt.result.Message {
				t.Errorf("Message = %v, want %v", got.Message, tt.result.Message)
			}
			if got.ErrorCode != tt.result.ErrorCode {
				t.Errorf("ErrorCode = %v, want %v", got.ErrorCode, tt.result.ErrorCode)
			}
		})
	}
}

func TestDMLError(t *testing.T) {
	err := &DMLError{
		Code:    "SCHEMA_MISMATCH",
		Message: "column types do not match",
	}

	if err.Error() != "column types do not match" {
		t.Errorf("DMLError.Error() = %v, want %v", err.Error(), "column types do not match")
	}
}

func TestDMLDescriptorValidation(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr bool
	}{
		{
			name:    "valid insert",
			json:    `{"schema_name":"main","table_name":"users"}`,
			wantErr: false,
		},
		{
			name:    "invalid json",
			json:    `{"schema_name":"main"`,
			wantErr: true,
		},
		{
			name:    "valid update",
			json:    `{"schema_name":"main","table_name":"users","row_ids":[1,2,3]}`,
			wantErr: false,
		},
		{
			name:    "valid delete",
			json:    `{"schema_name":"main","table_name":"users","row_ids":[1]}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Try to unmarshal as different types
			var insert InsertDescriptor
			err := json.Unmarshal([]byte(tt.json), &insert)
			if (err != nil) != tt.wantErr {
				t.Errorf("InsertDescriptor unmarshal error = %v, wantErr %v", err, tt.wantErr)
			}

			var update UpdateDescriptor
			err = json.Unmarshal([]byte(tt.json), &update)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateDescriptor unmarshal error = %v, wantErr %v", err, tt.wantErr)
			}

			var del DeleteAction
			err = json.Unmarshal([]byte(tt.json), &del)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteAction unmarshal error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
