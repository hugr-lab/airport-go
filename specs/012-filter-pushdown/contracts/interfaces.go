// Package contracts defines the public interfaces for the filter package.
// This file documents the API contract for filter pushdown parsing and encoding.
//
// NOTE: This is a design document, not actual code. The implementation
// will be in the filter/ package.
package contracts

// =============================================================================
// PARSING API
// =============================================================================

// Parse parses filter pushdown JSON from DuckDB Airport extension.
// Returns a FilterPushdown containing parsed expressions and column bindings.
//
// Example:
//
//	fp, err := filter.Parse(scanOpts.Filter)
//	if err != nil {
//	    // Handle malformed JSON
//	    return err
//	}
//	for _, expr := range fp.Filters {
//	    // Process expression
//	}
//
// Error conditions:
//   - Invalid JSON syntax
//   - Missing required fields
//   - Unknown expression class (still parseable, marked as unsupported)
//
// func Parse(data []byte) (*FilterPushdown, error)

// FilterPushdown is the top-level container for parsed filter JSON.
// type FilterPushdown struct {
//     Filters        []Expression // Filter expressions (implicitly AND'ed)
//     ColumnBindings []string     // Column names by binding index
// }

// =============================================================================
// EXPRESSION INTERFACE
// =============================================================================

// Expression is the interface implemented by all filter expression types.
// Use type assertions or type switches to access specific expression data.
//
// Example type switch:
//
//	switch e := expr.(type) {
//	case *filter.ComparisonExpression:
//	    // Handle comparison
//	case *filter.ConjunctionExpression:
//	    // Handle AND/OR
//	case *filter.ConstantExpression:
//	    // Handle literal value
//	case *filter.ColumnRefExpression:
//	    // Handle column reference
//	default:
//	    // Unknown or unsupported expression type
//	}
//
// type Expression interface {
//     Class() ExpressionClass
//     Type() ExpressionType
//     Alias() string
//     isExpression() // marker method
// }

// =============================================================================
// ENCODING API
// =============================================================================

// Encoder converts parsed filter expressions to SQL strings.
// Implementations handle dialect-specific syntax (DuckDB, PostgreSQL, etc.).
//
// type Encoder interface {
//     // Encode converts a single expression to SQL.
//     // Returns empty string if expression is unsupported.
//     Encode(expr Expression) string
//
//     // EncodeFilters converts all filters to a WHERE clause body.
//     // Returns the condition portion without "WHERE" keyword.
//     // Returns empty string if no filters can be encoded.
//     EncodeFilters(fp *FilterPushdown) string
// }

// EncoderOptions configures encoding behavior.
// type EncoderOptions struct {
//     // ColumnMapping maps original column names to target names.
//     // Columns not in the map use their original names.
//     ColumnMapping map[string]string
//
//     // ColumnExpressions maps column names to SQL expressions.
//     // Takes precedence over ColumnMapping.
//     // Use for computed columns or complex transformations.
//     ColumnExpressions map[string]string
// }

// NewDuckDBEncoder creates an encoder for DuckDB SQL dialect.
//
// Example:
//
//	enc := filter.NewDuckDBEncoder(nil) // No column mapping
//	sql := enc.EncodeFilters(fp)
//	if sql != "" {
//	    query := "SELECT * FROM table WHERE " + sql
//	}
//
// Example with column mapping:
//
//	enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
//	    ColumnMapping: map[string]string{
//	        "user_id": "uid",
//	        "created": "created_at",
//	    },
//	})
//
// Example with expression mapping:
//
//	enc := filter.NewDuckDBEncoder(&filter.EncoderOptions{
//	    ColumnExpressions: map[string]string{
//	        "full_name": "CONCAT(first_name, ' ', last_name)",
//	    },
//	})
//
// func NewDuckDBEncoder(opts *EncoderOptions) Encoder

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// ColumnName resolves a column name from a ColumnRefExpression.
// Uses the column binding index to look up the name in FilterPushdown.ColumnBindings.
//
// Example:
//
//	colRef := expr.(*filter.ColumnRefExpression)
//	name, err := fp.ColumnName(colRef)
//
// func (fp *FilterPushdown) ColumnName(ref *ColumnRefExpression) (string, error)

// =============================================================================
// EXPRESSION TYPES
// =============================================================================

// ComparisonExpression represents binary comparisons (=, <, >, <=, >=, <>, IN, etc.)
// type ComparisonExpression struct {
//     Left  Expression
//     Right Expression
//     // Type() returns COMPARE_EQUAL, COMPARE_LESSTHAN, etc.
// }

// ConjunctionExpression represents AND/OR with multiple children.
// type ConjunctionExpression struct {
//     Children []Expression
//     // Type() returns CONJUNCTION_AND or CONJUNCTION_OR
// }

// ConstantExpression represents a literal value.
// type ConstantExpression struct {
//     Value Value
// }

// ColumnRefExpression represents a column reference.
// type ColumnRefExpression struct {
//     Binding    ColumnBinding
//     ReturnType LogicalType
//     Depth      int
// }

// FunctionExpression represents a function call.
// type FunctionExpression struct {
//     Name       string
//     Children   []Expression
//     ReturnType LogicalType
//     IsOperator bool
// }

// CastExpression represents a type cast.
// type CastExpression struct {
//     Child      Expression
//     TargetType LogicalType
//     TryCast    bool
// }

// BetweenExpression represents BETWEEN lower AND upper.
// type BetweenExpression struct {
//     Input          Expression
//     Lower          Expression
//     Upper          Expression
//     LowerInclusive bool
//     UpperInclusive bool
// }

// OperatorExpression represents unary/n-ary operators (IS NULL, NOT, etc.)
// type OperatorExpression struct {
//     Children   []Expression
//     ReturnType LogicalType
//     // Type() returns OPERATOR_IS_NULL, OPERATOR_IS_NOT_NULL, OPERATOR_NOT
// }

// CaseExpression represents CASE WHEN ... THEN ... ELSE ... END.
// type CaseExpression struct {
//     CaseChecks []CaseCheck
//     ElseExpr   Expression
//     ReturnType LogicalType
// }

// =============================================================================
// VALUE TYPES
// =============================================================================

// Value represents a typed constant value.
// type Value struct {
//     Type   LogicalType
//     IsNull bool
//     Data   any // Type-specific: int64, float64, string, []byte, etc.
// }

// LogicalType represents DuckDB logical types.
// type LogicalType struct {
//     ID       LogicalTypeID
//     TypeInfo ExtraTypeInfo // nil for simple types
// }

// ColumnBinding identifies a column by table and column index.
// type ColumnBinding struct {
//     TableIndex  int
//     ColumnIndex int
// }

// =============================================================================
// ENUMERATIONS
// =============================================================================

// ExpressionClass identifies the category of expression.
// type ExpressionClass string
//
// const (
//     ClassBoundComparison ExpressionClass = "BOUND_COMPARISON"
//     ClassBoundConjunction ExpressionClass = "BOUND_CONJUNCTION"
//     ClassBoundConstant ExpressionClass = "BOUND_CONSTANT"
//     ClassBoundColumnRef ExpressionClass = "BOUND_COLUMN_REF"
//     ClassBoundFunction ExpressionClass = "BOUND_FUNCTION"
//     ClassBoundCast ExpressionClass = "BOUND_CAST"
//     ClassBoundBetween ExpressionClass = "BOUND_BETWEEN"
//     ClassBoundOperator ExpressionClass = "BOUND_OPERATOR"
//     ClassBoundCase ExpressionClass = "BOUND_CASE"
//     // ... additional classes
// )

// ExpressionType identifies the specific operation.
// type ExpressionType string
//
// const (
//     // Comparisons
//     TypeCompareEqual ExpressionType = "COMPARE_EQUAL"
//     TypeCompareNotEqual ExpressionType = "COMPARE_NOTEQUAL"
//     TypeCompareLessThan ExpressionType = "COMPARE_LESSTHAN"
//     TypeCompareGreaterThan ExpressionType = "COMPARE_GREATERTHAN"
//     TypeCompareLessThanOrEqual ExpressionType = "COMPARE_LESSTHANOREQUALTO"
//     TypeCompareGreaterThanOrEqual ExpressionType = "COMPARE_GREATERTHANOREQUALTO"
//     TypeCompareIn ExpressionType = "COMPARE_IN"
//     TypeCompareNotIn ExpressionType = "COMPARE_NOT_IN"
//     TypeCompareBetween ExpressionType = "COMPARE_BETWEEN"
//     TypeCompareNotBetween ExpressionType = "COMPARE_NOT_BETWEEN"
//
//     // Conjunctions
//     TypeConjunctionAnd ExpressionType = "CONJUNCTION_AND"
//     TypeConjunctionOr ExpressionType = "CONJUNCTION_OR"
//
//     // Operators
//     TypeOperatorNot ExpressionType = "OPERATOR_NOT"
//     TypeOperatorIsNull ExpressionType = "OPERATOR_IS_NULL"
//     TypeOperatorIsNotNull ExpressionType = "OPERATOR_IS_NOT_NULL"
//
//     // ... additional types
// )

// LogicalTypeID identifies DuckDB data types.
// type LogicalTypeID string
//
// const (
//     TypeBoolean LogicalTypeID = "BOOLEAN"
//     TypeTinyInt LogicalTypeID = "TINYINT"
//     TypeSmallInt LogicalTypeID = "SMALLINT"
//     TypeInteger LogicalTypeID = "INTEGER"
//     TypeBigInt LogicalTypeID = "BIGINT"
//     TypeFloat LogicalTypeID = "FLOAT"
//     TypeDouble LogicalTypeID = "DOUBLE"
//     TypeVarchar LogicalTypeID = "VARCHAR"
//     TypeDate LogicalTypeID = "DATE"
//     TypeTimestamp LogicalTypeID = "TIMESTAMP"
//     TypeUUID LogicalTypeID = "UUID"
//     // ... additional types
// )
