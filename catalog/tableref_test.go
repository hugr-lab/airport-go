package catalog

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/paulmach/orb"
)

func TestFunctionCallArgValidate(t *testing.T) {
	tests := []struct {
		name    string
		arg     FunctionCallArg
		wantErr bool
	}{
		// Supported types
		{
			name:    "string value",
			arg:     FunctionCallArg{Value: "hello", Type: arrow.BinaryTypes.String},
			wantErr: false,
		},
		{
			name:    "bool value",
			arg:     FunctionCallArg{Value: true, Type: arrow.FixedWidthTypes.Boolean},
			wantErr: false,
		},
		{
			name:    "int value",
			arg:     FunctionCallArg{Value: 42, Type: arrow.PrimitiveTypes.Int64},
			wantErr: false,
		},
		{
			name:    "int8 value",
			arg:     FunctionCallArg{Value: int8(1), Type: arrow.PrimitiveTypes.Int8},
			wantErr: false,
		},
		{
			name:    "int16 value",
			arg:     FunctionCallArg{Value: int16(1), Type: arrow.PrimitiveTypes.Int16},
			wantErr: false,
		},
		{
			name:    "int32 value",
			arg:     FunctionCallArg{Value: int32(1), Type: arrow.PrimitiveTypes.Int32},
			wantErr: false,
		},
		{
			name:    "int64 value",
			arg:     FunctionCallArg{Value: int64(1), Type: arrow.PrimitiveTypes.Int64},
			wantErr: false,
		},
		{
			name:    "uint8 value",
			arg:     FunctionCallArg{Value: uint8(1), Type: arrow.PrimitiveTypes.Uint8},
			wantErr: false,
		},
		{
			name:    "uint16 value",
			arg:     FunctionCallArg{Value: uint16(1), Type: arrow.PrimitiveTypes.Uint16},
			wantErr: false,
		},
		{
			name:    "uint32 value",
			arg:     FunctionCallArg{Value: uint32(1), Type: arrow.PrimitiveTypes.Uint32},
			wantErr: false,
		},
		{
			name:    "uint64 value",
			arg:     FunctionCallArg{Value: uint64(1), Type: arrow.PrimitiveTypes.Uint64},
			wantErr: false,
		},
		{
			name:    "float32 value",
			arg:     FunctionCallArg{Value: float32(1.5), Type: arrow.PrimitiveTypes.Float32},
			wantErr: false,
		},
		{
			name:    "float64 value",
			arg:     FunctionCallArg{Value: float64(1.5), Type: arrow.PrimitiveTypes.Float64},
			wantErr: false,
		},
		{
			name:    "time.Time value",
			arg:     FunctionCallArg{Value: time.Now(), Type: arrow.FixedWidthTypes.Timestamp_us},
			wantErr: false,
		},
		{
			name:    "orb.Geometry value",
			arg:     FunctionCallArg{Value: orb.Point{1, 2}, Type: arrow.BinaryTypes.Binary},
			wantErr: false,
		},
		{
			name:    "[]byte value",
			arg:     FunctionCallArg{Value: []byte{1, 2, 3}, Type: arrow.BinaryTypes.Binary},
			wantErr: false,
		},
		{
			name:    "[]any value",
			arg:     FunctionCallArg{Value: []any{"a", "b"}, Type: arrow.ListOf(arrow.BinaryTypes.String)},
			wantErr: false,
		},
		{
			name:    "map[string]any value",
			arg:     FunctionCallArg{Value: map[string]any{"key": "val"}, Type: arrow.BinaryTypes.String},
			wantErr: false,
		},
		{
			name:    "named arg",
			arg:     FunctionCallArg{Name: "header", Value: true, Type: arrow.FixedWidthTypes.Boolean},
			wantErr: false,
		},

		// Error cases
		{
			name:    "nil Type",
			arg:     FunctionCallArg{Value: "hello", Type: nil},
			wantErr: true,
		},
		{
			name:    "nil Value",
			arg:     FunctionCallArg{Value: nil, Type: arrow.BinaryTypes.String},
			wantErr: true,
		},
		{
			name:    "nil Type and Value",
			arg:     FunctionCallArg{},
			wantErr: true,
		},
		{
			name:    "unsupported type (complex128)",
			arg:     FunctionCallArg{Value: complex(1, 2), Type: arrow.PrimitiveTypes.Float64},
			wantErr: true,
		},
		{
			name:    "unsupported type (struct)",
			arg:     FunctionCallArg{Value: struct{}{}, Type: arrow.BinaryTypes.String},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.arg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
