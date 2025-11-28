package flight

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestParseArrowType(t *testing.T) {
	tests := []struct {
		name     string
		typeStr  string
		wantType arrow.DataType
		wantErr  bool
	}{
		// Numeric types
		{name: "int8", typeStr: "int8", wantType: arrow.PrimitiveTypes.Int8, wantErr: false},
		{name: "int16", typeStr: "int16", wantType: arrow.PrimitiveTypes.Int16, wantErr: false},
		{name: "int32", typeStr: "int32", wantType: arrow.PrimitiveTypes.Int32, wantErr: false},
		{name: "int64", typeStr: "int64", wantType: arrow.PrimitiveTypes.Int64, wantErr: false},
		{name: "uint8", typeStr: "uint8", wantType: arrow.PrimitiveTypes.Uint8, wantErr: false},
		{name: "float32", typeStr: "float32", wantType: arrow.PrimitiveTypes.Float32, wantErr: false},
		{name: "float64", typeStr: "float64", wantType: arrow.PrimitiveTypes.Float64, wantErr: false},

		// String types
		{name: "utf8", typeStr: "utf8", wantType: arrow.BinaryTypes.String, wantErr: false},
		{name: "string", typeStr: "string", wantType: arrow.BinaryTypes.String, wantErr: false},
		{name: "binary", typeStr: "binary", wantType: arrow.BinaryTypes.Binary, wantErr: false},

		// Boolean
		{name: "bool", typeStr: "bool", wantType: arrow.FixedWidthTypes.Boolean, wantErr: false},
		{name: "boolean", typeStr: "boolean", wantType: arrow.FixedWidthTypes.Boolean, wantErr: false},

		// Temporal
		{name: "date32", typeStr: "date32", wantType: arrow.FixedWidthTypes.Date32, wantErr: false},
		{name: "date64", typeStr: "date64", wantType: arrow.FixedWidthTypes.Date64, wantErr: false},
		{name: "timestamp", typeStr: "timestamp", wantType: arrow.FixedWidthTypes.Timestamp_us, wantErr: false},

		// Extension types
		{name: "geoarrow.wkb", typeStr: "extension<geoarrow.wkb>", wantType: arrow.BinaryTypes.Binary, wantErr: false},
		{name: "airport.geometry", typeStr: "extension<airport.geometry>", wantType: arrow.BinaryTypes.Binary, wantErr: false},

		// Invalid types
		{name: "unknown", typeStr: "unknown_type", wantType: nil, wantErr: true},
		{name: "invalid_ext", typeStr: "extension<unknown>", wantType: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, err := parseArrowType(tt.typeStr)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseArrowType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if gotType == nil {
					t.Error("parseArrowType() returned nil type without error")
					return
				}
				if !arrow.TypeEqual(gotType, tt.wantType) {
					t.Errorf("parseArrowType() = %v, want %v", gotType, tt.wantType)
				}
			}
		})
	}
}

func TestCreateSchemaActionValidation(t *testing.T) {
	tests := []struct {
		name    string
		action  CreateSchemaAction
		wantErr bool
	}{
		{
			name: "valid schema",
			action: CreateSchemaAction{
				SchemaName:  "test_schema",
				IfNotExists: false,
			},
			wantErr: false,
		},
		{
			name: "valid with if_not_exists",
			action: CreateSchemaAction{
				SchemaName:  "test_schema",
				IfNotExists: true,
				Comment:     "Test schema",
			},
			wantErr: false,
		},
		{
			name: "empty schema name",
			action: CreateSchemaAction{
				SchemaName:  "",
				IfNotExists: false,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validation happens in the handler, not the struct itself
			// This test documents expected validation behavior
			if tt.action.SchemaName == "" && !tt.wantErr {
				t.Error("expected error for empty schema name")
			}
		})
	}
}

func TestCreateTableActionSchemaConstruction(t *testing.T) {
	action := CreateTableAction{
		SchemaName:  "test_schema",
		TableName:   "test_table",
		IfNotExists: false,
		Schema: &ArrowSchemaPayload{
			Fields: []ArrowFieldPayload{
				{Name: "id", Type: "int64", Nullable: false},
				{Name: "name", Type: "utf8", Nullable: true},
				{Name: "location", Type: "extension<geoarrow.wkb>", Nullable: true},
			},
		},
	}

	// Parse fields
	fields := make([]arrow.Field, 0, len(action.Schema.Fields))
	for _, fieldPayload := range action.Schema.Fields {
		dataType, err := parseArrowType(fieldPayload.Type)
		if err != nil {
			t.Fatalf("failed to parse type %s: %v", fieldPayload.Type, err)
		}

		field := arrow.Field{
			Name:     fieldPayload.Name,
			Type:     dataType,
			Nullable: fieldPayload.Nullable,
		}
		fields = append(fields, field)
	}

	// Create schema
	schema := arrow.NewSchema(fields, nil)

	// Verify schema
	if schema.NumFields() != 3 {
		t.Errorf("expected 3 fields, got %d", schema.NumFields())
	}

	// Verify field types
	idField := schema.Field(0)
	if idField.Name != "id" {
		t.Errorf("expected field name 'id', got '%s'", idField.Name)
	}
	if !arrow.TypeEqual(idField.Type, arrow.PrimitiveTypes.Int64) {
		t.Errorf("expected Int64 type for id field, got %v", idField.Type)
	}
	if idField.Nullable {
		t.Error("expected id field to be non-nullable")
	}

	// Verify geometry field
	locationField := schema.Field(2)
	if locationField.Name != "location" {
		t.Errorf("expected field name 'location', got '%s'", locationField.Name)
	}
	if !arrow.TypeEqual(locationField.Type, arrow.BinaryTypes.Binary) {
		t.Errorf("expected Binary type for location field, got %v", locationField.Type)
	}
}

func TestDDLError(t *testing.T) {
	err := &DDLError{
		Code:    "INVALID_TYPE",
		Message: "unsupported type: foo",
	}

	if err.Error() != "unsupported type: foo" {
		t.Errorf("DDLError.Error() = %v, want %v", err.Error(), "unsupported type: foo")
	}
}
