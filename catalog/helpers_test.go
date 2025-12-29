package catalog

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestFindRowIDColumn(t *testing.T) {
	rowidMeta := arrow.NewMetadata([]string{"is_rowid"}, []string{"true"})
	emptyMeta := arrow.NewMetadata([]string{"is_rowid"}, []string{""})

	tests := []struct {
		name   string
		schema *arrow.Schema
		want   int
	}{
		{
			name: "by name",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "rowid", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", Type: arrow.BinaryTypes.String},
			}, nil),
			want: 1,
		},
		{
			name: "by metadata",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "row_identifier", Type: arrow.PrimitiveTypes.Int64, Metadata: rowidMeta},
				{Name: "name", Type: arrow.BinaryTypes.String},
			}, nil),
			want: 1,
		},
		{
			name: "name before metadata (first match wins)",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "rowid", Type: arrow.PrimitiveTypes.Int64},
				{Name: "other_rowid", Type: arrow.PrimitiveTypes.Int64, Metadata: rowidMeta},
			}, nil),
			want: 1,
		},
		{
			name: "metadata before name (first match wins)",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "other_rowid", Type: arrow.PrimitiveTypes.Int64, Metadata: rowidMeta},
				{Name: "rowid", Type: arrow.PrimitiveTypes.Int64},
			}, nil),
			want: 1,
		},
		{
			name: "not found",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", Type: arrow.BinaryTypes.String},
			}, nil),
			want: -1,
		},
		{
			name:   "nil schema",
			schema: nil,
			want:   -1,
		},
		{
			name:   "empty schema",
			schema: arrow.NewSchema([]arrow.Field{}, nil),
			want:   -1,
		},
		{
			name: "empty metadata value",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: emptyMeta},
				{Name: "name", Type: arrow.BinaryTypes.String},
			}, nil),
			want: -1,
		},
		{
			name: "first column",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "rowid", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", Type: arrow.BinaryTypes.String},
			}, nil),
			want: 0,
		},
		{
			name: "last column",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", Type: arrow.BinaryTypes.String},
				{Name: "rowid", Type: arrow.PrimitiveTypes.Int64},
			}, nil),
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FindRowIDColumn(tt.schema)
			if got != tt.want {
				t.Errorf("FindRowIDColumn() = %d, want %d", got, tt.want)
			}
		})
	}
}
