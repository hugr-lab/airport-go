package catalog

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// FindRowIDColumn returns the index of the rowid column in the schema.
// Returns -1 if no rowid column is found.
//
// Rowid column is identified by:
//   - Column name "rowid" (case-sensitive), or
//   - Metadata key "is_rowid" with non-empty value
//
// Parameters:
//   - schema: Arrow schema to search for rowid column.
//     May be nil (returns -1).
//
// Returns:
//   - Column index (0-based) if found
//   - -1 if no rowid column found or schema is nil
//
// Example:
//
//	idx := catalog.FindRowIDColumn(reader.Schema())
//	if idx == -1 {
//	    return errors.New("rowid column required")
//	}
//	rowidArray := batch.Column(idx)
func FindRowIDColumn(schema *arrow.Schema) int {
	if schema == nil {
		return -1
	}

	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		if field.Name == "rowid" {
			return i
		}
		if md := field.Metadata; md.Len() > 0 {
			if idx := md.FindKey("is_rowid"); idx >= 0 && md.Values()[idx] != "" {
				return i
			}
		}
	}
	return -1
}
