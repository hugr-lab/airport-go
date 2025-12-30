package filter

// ExpressionClass identifies the category of expression.
type ExpressionClass string

const (
	ClassBoundAggregate   ExpressionClass = "BOUND_AGGREGATE"
	ClassBoundCase        ExpressionClass = "BOUND_CASE"
	ClassBoundCast        ExpressionClass = "BOUND_CAST"
	ClassBoundColumnRef   ExpressionClass = "BOUND_COLUMN_REF"
	ClassBoundComparison  ExpressionClass = "BOUND_COMPARISON"
	ClassBoundConjunction ExpressionClass = "BOUND_CONJUNCTION"
	ClassBoundConstant    ExpressionClass = "BOUND_CONSTANT"
	ClassBoundDefault     ExpressionClass = "BOUND_DEFAULT"
	ClassBoundFunction    ExpressionClass = "BOUND_FUNCTION"
	ClassBoundOperator    ExpressionClass = "BOUND_OPERATOR"
	ClassBoundParameter   ExpressionClass = "BOUND_PARAMETER"
	ClassBoundRef         ExpressionClass = "BOUND_REF"
	ClassBoundSubquery    ExpressionClass = "BOUND_SUBQUERY"
	ClassBoundWindow      ExpressionClass = "BOUND_WINDOW"
	ClassBoundBetween     ExpressionClass = "BOUND_BETWEEN"
	ClassBoundUnnest      ExpressionClass = "BOUND_UNNEST"
	ClassBoundLambda      ExpressionClass = "BOUND_LAMBDA"
	ClassBoundLambdaRef   ExpressionClass = "BOUND_LAMBDA_REF"
)

// ExpressionType identifies the specific operation type.
type ExpressionType string

const (
	// Comparison operators
	TypeCompareEqual              ExpressionType = "COMPARE_EQUAL"
	TypeCompareNotEqual           ExpressionType = "COMPARE_NOTEQUAL"
	TypeCompareLessThan           ExpressionType = "COMPARE_LESSTHAN"
	TypeCompareGreaterThan        ExpressionType = "COMPARE_GREATERTHAN"
	TypeCompareLessThanOrEqual    ExpressionType = "COMPARE_LESSTHANOREQUALTO"
	TypeCompareGreaterThanOrEqual ExpressionType = "COMPARE_GREATERTHANOREQUALTO"
	TypeCompareIn                 ExpressionType = "COMPARE_IN"
	TypeCompareNotIn              ExpressionType = "COMPARE_NOT_IN"
	TypeCompareDistinctFrom       ExpressionType = "COMPARE_DISTINCT_FROM"
	TypeCompareBetween            ExpressionType = "COMPARE_BETWEEN"
	TypeCompareNotBetween         ExpressionType = "COMPARE_NOT_BETWEEN"
	TypeCompareNotDistinctFrom    ExpressionType = "COMPARE_NOT_DISTINCT_FROM"

	// Conjunction operators
	TypeConjunctionAnd ExpressionType = "CONJUNCTION_AND"
	TypeConjunctionOr  ExpressionType = "CONJUNCTION_OR"

	// Unary operators
	TypeOperatorNot       ExpressionType = "OPERATOR_NOT"
	TypeOperatorIsNull    ExpressionType = "OPERATOR_IS_NULL"
	TypeOperatorIsNotNull ExpressionType = "OPERATOR_IS_NOT_NULL"
	TypeOperatorNullIf    ExpressionType = "OPERATOR_NULLIF"
	TypeOperatorCoalesce  ExpressionType = "OPERATOR_COALESCE"

	// Value types
	TypeValueConstant  ExpressionType = "VALUE_CONSTANT"
	TypeValueParameter ExpressionType = "VALUE_PARAMETER"
	TypeValueNull      ExpressionType = "VALUE_NULL"
	TypeValueDefault   ExpressionType = "VALUE_DEFAULT"

	// Function types
	TypeFunction      ExpressionType = "FUNCTION"
	TypeBoundFunction ExpressionType = "BOUND_FUNCTION"

	// Aggregate types
	TypeAggregate      ExpressionType = "AGGREGATE"
	TypeBoundAggregate ExpressionType = "BOUND_AGGREGATE"

	// Window function types
	TypeWindowAggregate   ExpressionType = "WINDOW_AGGREGATE"
	TypeWindowRank        ExpressionType = "WINDOW_RANK"
	TypeWindowRankDense   ExpressionType = "WINDOW_RANK_DENSE"
	TypeWindowNtile       ExpressionType = "WINDOW_NTILE"
	TypeWindowPercentRank ExpressionType = "WINDOW_PERCENT_RANK"
	TypeWindowCumeDist    ExpressionType = "WINDOW_CUME_DIST"
	TypeWindowRowNumber   ExpressionType = "WINDOW_ROW_NUMBER"
	TypeWindowFirstValue  ExpressionType = "WINDOW_FIRST_VALUE"
	TypeWindowLastValue   ExpressionType = "WINDOW_LAST_VALUE"
	TypeWindowLead        ExpressionType = "WINDOW_LEAD"
	TypeWindowLag         ExpressionType = "WINDOW_LAG"
	TypeWindowNthValue    ExpressionType = "WINDOW_NTH_VALUE"

	// Other operators
	TypeCaseExpr         ExpressionType = "CASE_EXPR"
	TypeArrayExtract     ExpressionType = "ARRAY_EXTRACT"
	TypeArraySlice       ExpressionType = "ARRAY_SLICE"
	TypeStructExtract    ExpressionType = "STRUCT_EXTRACT"
	TypeArrayConstructor ExpressionType = "ARRAY_CONSTRUCTOR"
	TypeCast             ExpressionType = "CAST"
	TypeBoundRef         ExpressionType = "BOUND_REF"
	TypeBoundColumnRef   ExpressionType = "BOUND_COLUMN_REF"
	TypeBoundUnnest      ExpressionType = "BOUND_UNNEST"
	TypeLambda           ExpressionType = "LAMBDA"
)

// Expression is the interface implemented by all filter expression types.
// Use type assertions or type switches to access specific expression data.
type Expression interface {
	// Class returns the expression class (e.g., BOUND_COMPARISON, BOUND_CONJUNCTION).
	Class() ExpressionClass

	// Type returns the specific expression type (e.g., COMPARE_EQUAL, CONJUNCTION_AND).
	Type() ExpressionType

	// Alias returns the optional alias for the expression.
	Alias() string

	// expressionMarker is a marker method to prevent external implementation.
	expressionMarker()
}

// BaseExpression contains common fields for all expression types.
type BaseExpression struct {
	ExprClass ExpressionClass `json:"expression_class"`
	ExprType  ExpressionType  `json:"type"`
	ExprAlias string          `json:"alias"`
}

// Class returns the expression class.
func (b *BaseExpression) Class() ExpressionClass { return b.ExprClass }

// Type returns the expression type.
func (b *BaseExpression) Type() ExpressionType { return b.ExprType }

// Alias returns the expression alias.
func (b *BaseExpression) Alias() string { return b.ExprAlias }

func (b *BaseExpression) expressionMarker() {}

// ColumnBinding identifies a column by table and column index.
type ColumnBinding struct {
	TableIndex  int `json:"table_index"`
	ColumnIndex int `json:"column_index"`
}

// FilterPushdown is the top-level container for parsed filter JSON.
type FilterPushdown struct {
	// Filters contains the parsed filter expressions.
	// Multiple filters are implicitly AND'ed together.
	Filters []Expression

	// ColumnBindings maps column binding indices to column names.
	ColumnBindings []string
}

// ColumnName resolves a column name from a ColumnRefExpression.
// Returns an error if the binding index is out of range.
func (fp *FilterPushdown) ColumnName(ref *ColumnRefExpression) (string, error) {
	if ref.Binding.ColumnIndex < 0 || ref.Binding.ColumnIndex >= len(fp.ColumnBindings) {
		return "", &ColumnBindingError{Index: ref.Binding.ColumnIndex, Max: len(fp.ColumnBindings)}
	}
	return fp.ColumnBindings[ref.Binding.ColumnIndex], nil
}

// ColumnBindingError indicates an invalid column binding index.
type ColumnBindingError struct {
	Index int
	Max   int
}

func (e *ColumnBindingError) Error() string {
	return "invalid column binding index: " + itoa(e.Index) + " (max: " + itoa(e.Max-1) + ")"
}

// itoa converts an integer to a string without importing strconv.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// ComparisonExpression represents binary comparisons (=, <>, <, >, <=, >=, IN, NOT IN).
type ComparisonExpression struct {
	BaseExpression
	Left  Expression
	Right Expression
}

// ConjunctionExpression represents AND/OR with multiple children.
type ConjunctionExpression struct {
	BaseExpression
	Children []Expression
}

// ConstantExpression represents a literal value.
type ConstantExpression struct {
	BaseExpression
	Value Value
}

// ColumnRefExpression represents a reference to a table column.
type ColumnRefExpression struct {
	BaseExpression
	Binding    ColumnBinding
	ReturnType LogicalType
	Depth      int
}

// FunctionExpression represents a function call.
type FunctionExpression struct {
	BaseExpression
	Name              string
	Children          []Expression
	ReturnType        LogicalType
	Arguments         []LogicalType
	OriginalArguments []LogicalType
	CatalogName       string
	SchemaName        string
	HasSerialize      bool
	IsOperator        bool
}

// CastExpression represents a type cast.
type CastExpression struct {
	BaseExpression
	Child      Expression
	ReturnType LogicalType
	TryCast    bool
}

// BetweenExpression represents BETWEEN lower AND upper.
type BetweenExpression struct {
	BaseExpression
	Input          Expression
	Lower          Expression
	Upper          Expression
	LowerInclusive bool
	UpperInclusive bool
}

// OperatorExpression represents unary or n-ary operators (IS NULL, IS NOT NULL, NOT, etc.).
type OperatorExpression struct {
	BaseExpression
	Children   []Expression
	ReturnType LogicalType
}

// CaseExpression represents CASE WHEN ... THEN ... ELSE ... END.
type CaseExpression struct {
	BaseExpression
	CaseChecks []CaseCheck
	ElseExpr   Expression
	ReturnType LogicalType
}

// CaseCheck represents a single WHEN...THEN pair in a CASE expression.
type CaseCheck struct {
	WhenExpr Expression
	ThenExpr Expression
}

// ParameterExpression represents a bound parameter reference.
type ParameterExpression struct {
	BaseExpression
	Identifier string
	ReturnType LogicalType
}

// ReferenceExpression represents a bound reference expression.
type ReferenceExpression struct {
	BaseExpression
	ReturnType LogicalType
	Index      int
}

// AggregateExpression represents an aggregate function.
type AggregateExpression struct {
	BaseExpression
	Name              string
	Children          []Expression
	ReturnType        LogicalType
	Arguments         []LogicalType
	OriginalArguments []LogicalType
	AggregateType     string
	Filter            Expression
	HasSerialize      bool
}

// WindowExpression represents a window function.
type WindowExpression struct {
	BaseExpression
	Children      []Expression
	Partitions    []Expression
	ReturnType    LogicalType
	IgnoreNulls   bool
	Distinct      bool
	Start         string
	End           string
	ExcludeClause string
}

// UnsupportedExpression represents an expression type that is not yet supported.
// This allows parsing to succeed while marking the expression as unsupported for encoding.
type UnsupportedExpression struct {
	BaseExpression
}
