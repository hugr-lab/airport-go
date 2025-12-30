package filter

import "strings"

// Encoder converts parsed filter expressions to SQL strings.
// Implementations handle dialect-specific syntax (DuckDB, PostgreSQL, etc.).
type Encoder interface {
	// Encode converts a single expression to SQL.
	// Returns empty string if expression is unsupported.
	Encode(expr Expression) string

	// EncodeFilters converts all filters to a WHERE clause body.
	// Returns the condition portion without "WHERE" keyword.
	// Returns empty string if no filters can be encoded.
	EncodeFilters(fp *FilterPushdown) string
}

// EncoderOptions configures encoding behavior.
type EncoderOptions struct {
	// ColumnMapping maps original column names to target names.
	// Columns not in the map use their original names.
	ColumnMapping map[string]string

	// ColumnExpressions maps column names to SQL expressions.
	// Takes precedence over ColumnMapping.
	// Use for computed columns or complex transformations.
	ColumnExpressions map[string]string
}

// escapeString escapes single quotes in a string value for SQL.
func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// quoteLiteral returns a SQL string literal with proper escaping.
func quoteLiteral(s string) string {
	return "'" + escapeString(s) + "'"
}

// quoteIdentifier returns a quoted identifier if needed.
// DuckDB uses double quotes for identifiers.
func quoteIdentifier(name string) string {
	if needsQuoting(name) {
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
	return name
}

// needsQuoting returns true if the identifier needs quoting.
func needsQuoting(name string) bool {
	if len(name) == 0 {
		return true
	}

	// Check first character (must be letter or underscore)
	c := name[0]
	if !isLetter(c) && c != '_' {
		return true
	}

	// Check remaining characters (letters, digits, or underscore)
	for i := 1; i < len(name); i++ {
		c = name[i]
		if !isLetter(c) && !isDigit(c) && c != '_' {
			return true
		}
	}

	// Check for reserved words (simplified list)
	upper := strings.ToUpper(name)
	switch upper {
	case "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
		"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "INDEX",
		"JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AS", "IN", "IS", "LIKE",
		"BETWEEN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END", "ORDER", "BY",
		"GROUP", "HAVING", "LIMIT", "OFFSET", "UNION", "EXCEPT", "INTERSECT",
		"ALL", "DISTINCT", "VALUES", "SET", "INTO", "PRIMARY", "KEY", "FOREIGN",
		"REFERENCES", "CONSTRAINT", "DEFAULT", "CHECK", "UNIQUE", "ASC", "DESC",
		"NULLS", "FIRST", "LAST", "CAST", "INTERVAL", "DATE", "TIME", "TIMESTAMP":
		return true
	}

	return false
}

// isLetter returns true if c is an ASCII letter.
func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isDigit returns true if c is an ASCII digit.
func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}
