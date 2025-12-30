package filter

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// DuckDBEncoder encodes filter expressions to DuckDB SQL syntax.
type DuckDBEncoder struct {
	opts           *EncoderOptions
	columnBindings []string
}

// NewDuckDBEncoder creates a new DuckDB SQL encoder.
// If opts is nil, default options are used.
func NewDuckDBEncoder(opts *EncoderOptions) *DuckDBEncoder {
	if opts == nil {
		opts = &EncoderOptions{}
	}
	return &DuckDBEncoder{opts: opts}
}

// EncodeFilters converts all filters to a WHERE clause body.
// Returns the condition portion without "WHERE" keyword.
// Returns empty string if no filters can be encoded.
func (e *DuckDBEncoder) EncodeFilters(fp *FilterPushdown) string {
	if fp == nil || len(fp.Filters) == 0 {
		return ""
	}

	e.columnBindings = fp.ColumnBindings

	var parts []string
	for _, filter := range fp.Filters {
		encoded := e.Encode(filter)
		if encoded != "" {
			parts = append(parts, encoded)
		}
	}

	if len(parts) == 0 {
		return ""
	}

	if len(parts) == 1 {
		return parts[0]
	}

	return "(" + strings.Join(parts, ") AND (") + ")"
}

// Encode converts a single expression to SQL.
// Returns empty string if expression is unsupported.
func (e *DuckDBEncoder) Encode(expr Expression) string {
	if expr == nil {
		return ""
	}

	switch ex := expr.(type) {
	case *ComparisonExpression:
		return e.encodeComparison(ex)
	case *ConjunctionExpression:
		return e.encodeConjunction(ex)
	case *ConstantExpression:
		return e.encodeConstant(ex)
	case *ColumnRefExpression:
		return e.encodeColumnRef(ex)
	case *FunctionExpression:
		return e.encodeFunction(ex)
	case *CastExpression:
		return e.encodeCast(ex)
	case *BetweenExpression:
		return e.encodeBetween(ex)
	case *OperatorExpression:
		return e.encodeOperator(ex)
	case *CaseExpression:
		return e.encodeCase(ex)
	case *ParameterExpression:
		return e.encodeParameter(ex)
	case *AggregateExpression, *WindowExpression, *UnsupportedExpression:
		// These expression types are not supported for filter pushdown
		return ""
	default:
		return ""
	}
}

// encodeComparison encodes a comparison expression.
func (e *DuckDBEncoder) encodeComparison(c *ComparisonExpression) string {
	left := e.Encode(c.Left)
	right := e.Encode(c.Right)

	if left == "" || right == "" {
		return ""
	}

	switch c.Type() {
	case TypeCompareEqual:
		return left + " = " + right
	case TypeCompareNotEqual:
		return left + " <> " + right
	case TypeCompareLessThan:
		return left + " < " + right
	case TypeCompareGreaterThan:
		return left + " > " + right
	case TypeCompareLessThanOrEqual:
		return left + " <= " + right
	case TypeCompareGreaterThanOrEqual:
		return left + " >= " + right
	case TypeCompareIn:
		return e.encodeIn(c, false)
	case TypeCompareNotIn:
		return e.encodeIn(c, true)
	case TypeCompareDistinctFrom:
		return left + " IS DISTINCT FROM " + right
	case TypeCompareNotDistinctFrom:
		return left + " IS NOT DISTINCT FROM " + right
	case TypeCompareBetween, TypeCompareNotBetween:
		// These are handled by BetweenExpression
		return ""
	default:
		return ""
	}
}

// encodeIn encodes IN/NOT IN expressions.
func (e *DuckDBEncoder) encodeIn(c *ComparisonExpression, notIn bool) string {
	left := e.Encode(c.Left)
	if left == "" {
		return ""
	}

	// The right side should be a function expression with list_value
	funcExpr, ok := c.Right.(*FunctionExpression)
	if !ok {
		return ""
	}

	// Encode all children as the IN list
	var values []string
	for _, child := range funcExpr.Children {
		encoded := e.Encode(child)
		if encoded == "" {
			return ""
		}
		values = append(values, encoded)
	}

	if len(values) == 0 {
		return ""
	}

	op := " IN "
	if notIn {
		op = " NOT IN "
	}

	return left + op + "(" + strings.Join(values, ", ") + ")"
}

// encodeConjunction encodes AND/OR conjunctions.
func (e *DuckDBEncoder) encodeConjunction(c *ConjunctionExpression) string {
	var parts []string
	for _, child := range c.Children {
		encoded := e.Encode(child)
		if encoded != "" {
			parts = append(parts, encoded)
		}
	}

	// Handle unsupported expression rules:
	// - For OR: if any child is unsupported, skip entire OR
	// - For AND: skip unsupported children, keep others
	if c.Type() == TypeConjunctionOr {
		if len(parts) != len(c.Children) {
			// Some child was unsupported, skip entire OR
			return ""
		}
	}

	if len(parts) == 0 {
		return ""
	}

	if len(parts) == 1 {
		return parts[0]
	}

	op := " AND "
	if c.Type() == TypeConjunctionOr {
		op = " OR "
	}

	return "(" + strings.Join(parts, op) + ")"
}

// encodeConstant encodes a constant value.
func (e *DuckDBEncoder) encodeConstant(c *ConstantExpression) string {
	return e.formatValue(c.Value)
}

// encodeColumnRef encodes a column reference.
func (e *DuckDBEncoder) encodeColumnRef(c *ColumnRefExpression) string {
	// Get the column name from bindings
	if c.Binding.ColumnIndex < 0 || c.Binding.ColumnIndex >= len(e.columnBindings) {
		return ""
	}

	colName := e.columnBindings[c.Binding.ColumnIndex]

	// Check for expression mapping first (takes precedence)
	if e.opts.ColumnExpressions != nil {
		if expr, ok := e.opts.ColumnExpressions[colName]; ok {
			return expr
		}
	}

	// Check for name mapping
	if e.opts.ColumnMapping != nil {
		if mapped, ok := e.opts.ColumnMapping[colName]; ok {
			colName = mapped
		}
	}

	return quoteIdentifier(colName)
}

// encodeFunction encodes a function expression.
func (e *DuckDBEncoder) encodeFunction(f *FunctionExpression) string {
	// Encode all arguments
	var args []string
	for _, child := range f.Children {
		encoded := e.Encode(child)
		if encoded == "" {
			return ""
		}
		args = append(args, encoded)
	}

	// Handle operators represented as functions
	if f.IsOperator {
		return e.encodeOperatorFunction(f.Name, args)
	}

	// Regular function call
	return f.Name + "(" + strings.Join(args, ", ") + ")"
}

// encodeOperatorFunction encodes operators that are represented as functions.
func (e *DuckDBEncoder) encodeOperatorFunction(name string, args []string) string {
	switch name {
	case "+", "-", "*", "/", "%":
		if len(args) == 2 {
			return "(" + args[0] + " " + name + " " + args[1] + ")"
		}
		if len(args) == 1 && (name == "-" || name == "+") {
			return name + args[0]
		}
	case "~~": // LIKE
		if len(args) == 2 {
			return args[0] + " LIKE " + args[1]
		}
	case "!~~": // NOT LIKE
		if len(args) == 2 {
			return args[0] + " NOT LIKE " + args[1]
		}
	case "~~*": // ILIKE
		if len(args) == 2 {
			return args[0] + " ILIKE " + args[1]
		}
	case "!~~*": // NOT ILIKE
		if len(args) == 2 {
			return args[0] + " NOT ILIKE " + args[1]
		}
	case "~": // Regex match
		if len(args) == 2 {
			return args[0] + " ~ " + args[1]
		}
	case "!~": // Not regex match
		if len(args) == 2 {
			return args[0] + " !~ " + args[1]
		}
	case "~*": // Case-insensitive regex
		if len(args) == 2 {
			return args[0] + " ~* " + args[1]
		}
	case "!~*": // Not case-insensitive regex
		if len(args) == 2 {
			return args[0] + " !~* " + args[1]
		}
	case "||": // String concatenation
		if len(args) >= 2 {
			return "(" + strings.Join(args, " || ") + ")"
		}
	}

	// Default: treat as regular function
	return name + "(" + strings.Join(args, ", ") + ")"
}

// encodeCast encodes a CAST expression.
func (e *DuckDBEncoder) encodeCast(c *CastExpression) string {
	child := e.Encode(c.Child)
	if child == "" {
		return ""
	}

	typeName := e.formatTypeName(c.ReturnType)
	if typeName == "" {
		return ""
	}

	if c.TryCast {
		return "TRY_CAST(" + child + " AS " + typeName + ")"
	}
	return "CAST(" + child + " AS " + typeName + ")"
}

// encodeBetween encodes a BETWEEN expression.
func (e *DuckDBEncoder) encodeBetween(b *BetweenExpression) string {
	input := e.Encode(b.Input)
	lower := e.Encode(b.Lower)
	upper := e.Encode(b.Upper)

	if input == "" || lower == "" || upper == "" {
		return ""
	}

	notBetween := b.Type() == TypeCompareNotBetween

	// Standard BETWEEN is always inclusive
	if b.LowerInclusive && b.UpperInclusive {
		if notBetween {
			return input + " NOT BETWEEN " + lower + " AND " + upper
		}
		return input + " BETWEEN " + lower + " AND " + upper
	}

	// For non-standard bounds, use comparison operators
	var conditions []string
	if b.LowerInclusive {
		conditions = append(conditions, input+" >= "+lower)
	} else {
		conditions = append(conditions, input+" > "+lower)
	}
	if b.UpperInclusive {
		conditions = append(conditions, input+" <= "+upper)
	} else {
		conditions = append(conditions, input+" < "+upper)
	}

	result := "(" + strings.Join(conditions, " AND ") + ")"
	if notBetween {
		return "NOT " + result
	}
	return result
}

// encodeOperator encodes operator expressions (IS NULL, IS NOT NULL, NOT, IN, NOT IN, etc.).
func (e *DuckDBEncoder) encodeOperator(o *OperatorExpression) string {
	if len(o.Children) == 0 {
		return ""
	}

	switch o.Type() {
	case TypeOperatorIsNull:
		child := e.Encode(o.Children[0])
		if child == "" {
			return ""
		}
		return child + " IS NULL"

	case TypeOperatorIsNotNull:
		child := e.Encode(o.Children[0])
		if child == "" {
			return ""
		}
		return child + " IS NOT NULL"

	case TypeOperatorNot:
		child := e.Encode(o.Children[0])
		if child == "" {
			return ""
		}
		return "NOT (" + child + ")"

	case TypeOperatorCoalesce:
		var args []string
		for _, child := range o.Children {
			encoded := e.Encode(child)
			if encoded == "" {
				return ""
			}
			args = append(args, encoded)
		}
		return "COALESCE(" + strings.Join(args, ", ") + ")"

	case TypeOperatorNullIf:
		if len(o.Children) != 2 {
			return ""
		}
		left := e.Encode(o.Children[0])
		right := e.Encode(o.Children[1])
		if left == "" || right == "" {
			return ""
		}
		return "NULLIF(" + left + ", " + right + ")"

	case TypeCompareIn:
		return e.encodeInOperator(o, false)

	case TypeCompareNotIn:
		return e.encodeInOperator(o, true)

	default:
		return ""
	}
}

// encodeInOperator encodes IN/NOT IN operator expressions.
// Format: children[0] = column, children[1...n] = values
func (e *DuckDBEncoder) encodeInOperator(o *OperatorExpression, notIn bool) string {
	if len(o.Children) < 2 {
		return ""
	}

	// First child is the column/expression being tested
	left := e.Encode(o.Children[0])
	if left == "" {
		return ""
	}

	// Remaining children are the values in the IN list
	var values []string
	for i := 1; i < len(o.Children); i++ {
		encoded := e.Encode(o.Children[i])
		if encoded == "" {
			return ""
		}
		values = append(values, encoded)
	}

	if len(values) == 0 {
		return ""
	}

	op := " IN "
	if notIn {
		op = " NOT IN "
	}

	return left + op + "(" + strings.Join(values, ", ") + ")"
}

// encodeCase encodes a CASE expression.
func (e *DuckDBEncoder) encodeCase(c *CaseExpression) string {
	var sb strings.Builder
	sb.WriteString("CASE")

	for _, check := range c.CaseChecks {
		whenExpr := e.Encode(check.WhenExpr)
		thenExpr := e.Encode(check.ThenExpr)
		if whenExpr == "" || thenExpr == "" {
			return ""
		}
		sb.WriteString(" WHEN ")
		sb.WriteString(whenExpr)
		sb.WriteString(" THEN ")
		sb.WriteString(thenExpr)
	}

	if c.ElseExpr != nil {
		elseExpr := e.Encode(c.ElseExpr)
		if elseExpr == "" {
			return ""
		}
		sb.WriteString(" ELSE ")
		sb.WriteString(elseExpr)
	}

	sb.WriteString(" END")
	return sb.String()
}

// encodeParameter encodes a parameter expression.
func (e *DuckDBEncoder) encodeParameter(p *ParameterExpression) string {
	return p.Identifier
}

// formatValue formats a Value as a SQL literal.
func (e *DuckDBEncoder) formatValue(v Value) string {
	if v.IsNull {
		return "NULL"
	}

	switch v.Type.ID {
	case TypeIDBoolean:
		return e.formatBoolValue(v.Data)
	case TypeIDTinyInt, TypeIDSmallInt, TypeIDInteger, TypeIDBigInt:
		return e.formatIntValue(v.Data)
	case TypeIDUTinyInt, TypeIDUSmallInt, TypeIDUInteger, TypeIDUBigInt:
		return e.formatUIntValue(v.Data)
	case TypeIDHugeInt:
		return e.formatHugeIntValue(v.Data)
	case TypeIDUHugeInt:
		return e.formatUHugeIntValue(v.Data)
	case TypeIDFloat, TypeIDDouble:
		return e.formatFloatValue(v.Data)
	case TypeIDDecimal:
		return e.formatDecimalValue(v.Data)
	case TypeIDVarchar, TypeIDChar:
		return e.formatStringValue(v.Data)
	case TypeIDBlob:
		return e.formatBlobValue(v.Data)
	case TypeIDDate:
		return e.formatDateValue(v.Data)
	case TypeIDTime, TypeIDTimeTZ:
		return e.formatTimeValue(v.Data)
	case TypeIDTimestamp, TypeIDTimestampTZ, TypeIDTimestampMs, TypeIDTimestampNs, TypeIDTimestampSec:
		return e.formatTimestampValue(v.Data, v.Type.ID)
	case TypeIDInterval:
		return e.formatIntervalValue(v.Data)
	case TypeIDUUID:
		return e.formatUUIDValue(v.Data)
	case TypeIDList, TypeIDArray:
		return e.formatListValue(v.Data)
	case TypeIDStruct:
		return e.formatStructValue(v.Data)
	case TypeIDMap:
		return e.formatMapValue(v.Data)
	default:
		// For unknown types, try to format as generic
		return e.formatGenericValue(v.Data)
	}
}

// formatBoolValue formats a boolean value.
func (e *DuckDBEncoder) formatBoolValue(data any) string {
	if b, ok := data.(bool); ok {
		if b {
			return "TRUE"
		}
		return "FALSE"
	}
	return ""
}

// formatIntValue formats a signed integer value.
func (e *DuckDBEncoder) formatIntValue(data any) string {
	switch v := data.(type) {
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case float64:
		return strconv.FormatInt(int64(v), 10)
	default:
		return ""
	}
}

// formatUIntValue formats an unsigned integer value.
func (e *DuckDBEncoder) formatUIntValue(data any) string {
	switch v := data.(type) {
	case uint64:
		return strconv.FormatUint(v, 10)
	case float64:
		return strconv.FormatUint(uint64(v), 10)
	default:
		return ""
	}
}

// formatHugeIntValue formats a 128-bit signed integer value.
func (e *DuckDBEncoder) formatHugeIntValue(data any) string {
	switch v := data.(type) {
	case HugeInt:
		// Convert upper/lower to big.Int
		bi := new(big.Int)
		bi.SetInt64(v.Upper)
		bi.Lsh(bi, 64)
		lower := new(big.Int).SetUint64(v.Lower)
		bi.Or(bi, lower)
		return bi.String()
	default:
		return ""
	}
}

// formatUHugeIntValue formats a 128-bit unsigned integer value.
func (e *DuckDBEncoder) formatUHugeIntValue(data any) string {
	switch v := data.(type) {
	case UHugeInt:
		bi := new(big.Int)
		bi.SetUint64(v.Upper)
		bi.Lsh(bi, 64)
		lower := new(big.Int).SetUint64(v.Lower)
		bi.Or(bi, lower)
		return bi.String()
	default:
		return ""
	}
}

// formatFloatValue formats a floating-point value.
func (e *DuckDBEncoder) formatFloatValue(data any) string {
	switch v := data.(type) {
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	default:
		return ""
	}
}

// formatDecimalValue formats a decimal value.
func (e *DuckDBEncoder) formatDecimalValue(data any) string {
	switch v := data.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return ""
	}
}

// formatStringValue formats a string value with proper escaping.
func (e *DuckDBEncoder) formatStringValue(data any) string {
	switch v := data.(type) {
	case string:
		return quoteLiteral(v)
	default:
		return ""
	}
}

// formatBlobValue formats a blob value as a hex literal.
func (e *DuckDBEncoder) formatBlobValue(data any) string {
	switch v := data.(type) {
	case []byte:
		var sb strings.Builder
		sb.WriteString("'\\x")
		for _, b := range v {
			sb.WriteString(fmt.Sprintf("%02x", b))
		}
		sb.WriteString("'")
		return sb.String()
	case string:
		// Already a string, escape it
		return quoteLiteral(v)
	default:
		return ""
	}
}

// formatDateValue formats a date value.
// The value is days since Unix epoch (1970-01-01).
func (e *DuckDBEncoder) formatDateValue(data any) string {
	switch v := data.(type) {
	case int64:
		// Convert days since epoch to date
		t := time.Unix(v*86400, 0).UTC()
		return "DATE '" + t.Format("2006-01-02") + "'"
	case float64:
		t := time.Unix(int64(v)*86400, 0).UTC()
		return "DATE '" + t.Format("2006-01-02") + "'"
	default:
		return ""
	}
}

// formatTimeValue formats a time value.
// The value is microseconds since midnight.
func (e *DuckDBEncoder) formatTimeValue(data any) string {
	switch v := data.(type) {
	case int64:
		// Convert microseconds to time components
		micros := v
		hours := micros / 3600000000
		micros %= 3600000000
		mins := micros / 60000000
		micros %= 60000000
		secs := micros / 1000000
		micros %= 1000000

		if micros > 0 {
			return fmt.Sprintf("TIME '%02d:%02d:%02d.%06d'", hours, mins, secs, micros)
		}
		return fmt.Sprintf("TIME '%02d:%02d:%02d'", hours, mins, secs)
	case float64:
		return e.formatTimeValue(int64(v))
	default:
		return ""
	}
}

// formatTimestampValue formats a timestamp value.
func (e *DuckDBEncoder) formatTimestampValue(data any, typeID LogicalTypeID) string {
	var micros int64

	switch v := data.(type) {
	case int64:
		micros = v
	case float64:
		micros = int64(v)
	default:
		return ""
	}

	// Convert based on timestamp precision
	var t time.Time
	switch typeID {
	case TypeIDTimestampSec:
		t = time.Unix(micros, 0).UTC()
	case TypeIDTimestampMs:
		t = time.Unix(micros/1000, (micros%1000)*1000000).UTC()
	case TypeIDTimestampNs:
		t = time.Unix(micros/1000000000, micros%1000000000).UTC()
	default: // TypeIDTimestamp, TypeIDTimestampTZ (microseconds)
		t = time.Unix(micros/1000000, (micros%1000000)*1000).UTC()
	}

	// Format with microsecond precision if needed
	formatted := t.Format("2006-01-02 15:04:05")
	if micros%1000000 != 0 && typeID != TypeIDTimestampSec {
		micro := (micros % 1000000)
		if micro < 0 {
			micro = -micro
		}
		formatted = fmt.Sprintf("%s.%06d", t.Format("2006-01-02 15:04:05"), micro)
	}

	return "TIMESTAMP '" + formatted + "'"
}

// formatIntervalValue formats an interval value.
func (e *DuckDBEncoder) formatIntervalValue(data any) string {
	switch v := data.(type) {
	case Interval:
		var parts []string

		if v.Months != 0 {
			years := v.Months / 12
			months := v.Months % 12
			if years != 0 {
				parts = append(parts, fmt.Sprintf("%d years", years))
			}
			if months != 0 {
				parts = append(parts, fmt.Sprintf("%d months", months))
			}
		}

		if v.Days != 0 {
			parts = append(parts, fmt.Sprintf("%d days", v.Days))
		}

		if v.Micros != 0 {
			// Convert to hours, minutes, seconds, microseconds
			micros := v.Micros
			hours := micros / 3600000000
			micros %= 3600000000
			mins := micros / 60000000
			micros %= 60000000
			secs := micros / 1000000
			micros %= 1000000

			if hours != 0 {
				parts = append(parts, fmt.Sprintf("%d hours", hours))
			}
			if mins != 0 {
				parts = append(parts, fmt.Sprintf("%d minutes", mins))
			}
			if secs != 0 || micros != 0 {
				if micros != 0 {
					parts = append(parts, fmt.Sprintf("%d.%06d seconds", secs, micros))
				} else {
					parts = append(parts, fmt.Sprintf("%d seconds", secs))
				}
			}
		}

		if len(parts) == 0 {
			return "INTERVAL '0 seconds'"
		}

		return "INTERVAL '" + strings.Join(parts, " ") + "'"
	default:
		return ""
	}
}

// formatUUIDValue formats a UUID value.
func (e *DuckDBEncoder) formatUUIDValue(data any) string {
	switch v := data.(type) {
	case string:
		return quoteLiteral(v)
	default:
		return ""
	}
}

// formatListValue formats a list/array value.
func (e *DuckDBEncoder) formatListValue(data any) string {
	switch v := data.(type) {
	case ListValue:
		var parts []string
		for _, child := range v.Children {
			formatted := e.formatValue(child)
			if formatted == "" {
				return ""
			}
			parts = append(parts, formatted)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return ""
	}
}

// formatStructValue formats a struct value.
func (e *DuckDBEncoder) formatStructValue(data any) string {
	switch v := data.(type) {
	case StructValue:
		var parts []string
		for _, child := range v.Children {
			formatted := e.formatValue(child)
			if formatted == "" {
				return ""
			}
			parts = append(parts, formatted)
		}
		return "ROW(" + strings.Join(parts, ", ") + ")"
	default:
		return ""
	}
}

// formatMapValue formats a map value.
func (e *DuckDBEncoder) formatMapValue(data any) string {
	switch v := data.(type) {
	case MapValue:
		var entries []string
		for i := 0; i < len(v.Keys) && i < len(v.Values); i++ {
			key := e.formatValue(v.Keys[i])
			val := e.formatValue(v.Values[i])
			if key == "" || val == "" {
				return ""
			}
			entries = append(entries, key+": "+val)
		}
		return "MAP{" + strings.Join(entries, ", ") + "}"
	default:
		return ""
	}
}

// formatGenericValue formats a generic value.
func (e *DuckDBEncoder) formatGenericValue(data any) string {
	switch v := data.(type) {
	case string:
		return quoteLiteral(v)
	case bool:
		return e.formatBoolValue(v)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%g", v)
	case nil:
		return "NULL"
	default:
		return ""
	}
}

// formatTypeName formats a LogicalType as a SQL type name.
func (e *DuckDBEncoder) formatTypeName(lt LogicalType) string {
	switch lt.ID {
	case TypeIDBoolean:
		return "BOOLEAN"
	case TypeIDTinyInt:
		return "TINYINT"
	case TypeIDSmallInt:
		return "SMALLINT"
	case TypeIDInteger:
		return "INTEGER"
	case TypeIDBigInt:
		return "BIGINT"
	case TypeIDUTinyInt:
		return "UTINYINT"
	case TypeIDUSmallInt:
		return "USMALLINT"
	case TypeIDUInteger:
		return "UINTEGER"
	case TypeIDUBigInt:
		return "UBIGINT"
	case TypeIDHugeInt:
		return "HUGEINT"
	case TypeIDUHugeInt:
		return "UHUGEINT"
	case TypeIDFloat:
		return "FLOAT"
	case TypeIDDouble:
		return "DOUBLE"
	case TypeIDDecimal:
		if info, ok := lt.TypeInfo.(*DecimalTypeInfo); ok {
			return fmt.Sprintf("DECIMAL(%d, %d)", info.Width, info.Scale)
		}
		return "DECIMAL"
	case TypeIDVarchar:
		return "VARCHAR"
	case TypeIDChar:
		return "CHAR"
	case TypeIDBlob:
		return "BLOB"
	case TypeIDDate:
		return "DATE"
	case TypeIDTime:
		return "TIME"
	case TypeIDTimeTZ:
		return "TIME WITH TIME ZONE"
	case TypeIDTimestamp:
		return "TIMESTAMP"
	case TypeIDTimestampTZ:
		return "TIMESTAMP WITH TIME ZONE"
	case TypeIDTimestampMs:
		return "TIMESTAMP_MS"
	case TypeIDTimestampNs:
		return "TIMESTAMP_NS"
	case TypeIDTimestampSec:
		return "TIMESTAMP_S"
	case TypeIDInterval:
		return "INTERVAL"
	case TypeIDUUID:
		return "UUID"
	case TypeIDList:
		if info, ok := lt.TypeInfo.(*ListTypeInfo); ok {
			childType := e.formatTypeName(info.ChildType)
			return childType + "[]"
		}
		return "LIST"
	case TypeIDArray:
		if info, ok := lt.TypeInfo.(*ArrayTypeInfo); ok {
			childType := e.formatTypeName(info.ChildType)
			return fmt.Sprintf("%s[%d]", childType, info.Size)
		}
		return "ARRAY"
	case TypeIDStruct:
		if info, ok := lt.TypeInfo.(*StructTypeInfo); ok {
			var fields []string
			for _, field := range info.ChildTypes {
				fieldType := e.formatTypeName(field.Type)
				fields = append(fields, quoteIdentifier(field.Name)+" "+fieldType)
			}
			return "STRUCT(" + strings.Join(fields, ", ") + ")"
		}
		return "STRUCT"
	case TypeIDMap:
		return "MAP"
	case TypeIDEnum:
		return "ENUM"
	default:
		return string(lt.ID)
	}
}
