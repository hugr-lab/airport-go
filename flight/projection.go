package flight

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// ProjectSchema returns a projected schema containing only the specified columns.
// If columns is nil or empty, returns the full schema unchanged.
// Column order in the returned schema matches the order in columns slice.
// Original schema metadata is preserved in the projected schema.
func ProjectSchema(schema *arrow.Schema, columns []string) *arrow.Schema {
	if len(columns) == 0 {
		return schema
	}

	// Build column name to index map
	colIndex := make(map[string]int, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		colIndex[schema.Field(i).Name] = i
	}

	// Select only requested columns in order
	fields := make([]arrow.Field, 0, len(columns))
	for _, col := range columns {
		if idx, ok := colIndex[col]; ok {
			fields = append(fields, schema.Field(idx))
		}
	}

	if len(fields) == 0 {
		// No matching columns - return original schema
		return schema
	}

	// Preserve original schema metadata
	meta := schema.Metadata()
	return arrow.NewSchema(fields, &meta)
}
