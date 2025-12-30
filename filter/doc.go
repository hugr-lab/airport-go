// Package filter provides parsing and encoding of DuckDB Airport extension filter pushdown JSON.
//
// This package enables Flight server developers to:
//   - Parse filter pushdown JSON from DuckDB into strongly-typed Go structures
//   - Encode parsed expressions to SQL for backend databases (primarily DuckDB)
//   - Map column names during encoding to translate between schemas
//   - Replace column names with SQL expressions for computed columns
//
// # Basic Usage
//
// Parse filter JSON received in ScanOptions.Filter:
//
//	fp, err := filter.Parse(scanOpts.Filter)
//	if err != nil {
//	    return err // Malformed JSON
//	}
//
//	// Encode to DuckDB SQL
//	enc := filter.NewDuckDBEncoder(nil)
//	whereClause := enc.EncodeFilters(fp)
//
//	if whereClause != "" {
//	    query := "SELECT * FROM table WHERE " + whereClause
//	}
//
// # Column Mapping
//
// Map DuckDB column names to backend storage names:
//
//	enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
//	    ColumnMapping: map[string]string{
//	        "user_id": "uid",
//	        "created": "created_at",
//	    },
//	})
//
// # Column Expression Replacement
//
// Replace column names with SQL expressions for computed columns:
//
//	enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
//	    ColumnExpressions: map[string]string{
//	        "full_name": "CONCAT(first_name, ' ', last_name)",
//	    },
//	})
//
// # Unsupported Expression Handling
//
// The encoder gracefully handles unsupported expressions:
//   - For AND: Skips unsupported children, keeps others
//   - For OR: If any child is unsupported, skips entire OR expression
//   - Returns empty string if all expressions are unsupported
//
// This produces the widest possible filter, which is safe because DuckDB
// client applies filters client-side as a fallback.
//
// # Custom Dialects
//
// Implement the Encoder interface for other SQL dialects:
//
//	type PostgreSQLEncoder struct { ... }
//	func (e *PostgreSQLEncoder) Encode(expr Expression) string { ... }
//	func (e *PostgreSQLEncoder) EncodeFilters(fp *FilterPushdown) string { ... }
//
// # Expression Types
//
// The package supports the following expression types:
//   - ComparisonExpression: Binary comparisons (=, <>, <, >, <=, >=, IN, NOT IN, BETWEEN)
//   - ConjunctionExpression: AND/OR with multiple children
//   - ConstantExpression: Literal values with type information
//   - ColumnRefExpression: References to table columns
//   - FunctionExpression: Function calls (LOWER, LENGTH, etc.)
//   - CastExpression: Type casts (CAST, TRY_CAST)
//   - BetweenExpression: BETWEEN lower AND upper
//   - OperatorExpression: Unary operators (IS NULL, IS NOT NULL, NOT)
//   - CaseExpression: CASE WHEN ... THEN ... ELSE ... END
//
// All DuckDB logical types are supported including:
// BOOLEAN, TINYINT-BIGINT, UTINYINT-UBIGINT, HUGEINT, UHUGEINT,
// FLOAT, DOUBLE, DECIMAL, VARCHAR, BLOB, DATE, TIME, TIMESTAMP variants,
// INTERVAL, UUID, and complex types (LIST, STRUCT, MAP, ARRAY).
package filter
