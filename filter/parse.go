package filter

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// Parse parses filter pushdown JSON from DuckDB Airport extension.
// Returns a FilterPushdown containing parsed expressions and column bindings.
//
// Error conditions:
//   - Invalid JSON syntax
//   - Missing required fields
//   - Unknown expression class (parseable but marked as unsupported)
func Parse(data []byte) (*FilterPushdown, error) {
	if len(data) == 0 {
		return &FilterPushdown{}, nil
	}

	var raw rawFilterPushdown
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("filter: invalid JSON: %w", err)
	}

	fp := &FilterPushdown{
		ColumnBindings: raw.ColumnBindings,
		Filters:        make([]Expression, 0, len(raw.Filters)),
	}

	for i, rawExpr := range raw.Filters {
		expr, err := parseExpression(rawExpr, fp.ColumnBindings)
		if err != nil {
			return nil, fmt.Errorf("filter: error parsing filter %d: %w", i, err)
		}
		fp.Filters = append(fp.Filters, expr)
	}

	return fp, nil
}

// rawFilterPushdown is the intermediate structure for JSON parsing.
type rawFilterPushdown struct {
	Filters        []json.RawMessage `json:"filters"`
	ColumnBindings []string          `json:"column_binding_names_by_index"`
}

// rawExpression is used for two-phase parsing to determine expression class.
type rawExpression struct {
	ExpressionClass string `json:"expression_class"`
	Type            string `json:"type"`
	Alias           string `json:"alias"`
}

// parseExpression parses a single expression from raw JSON.
func parseExpression(data json.RawMessage, columnBindings []string) (Expression, error) {
	var raw rawExpression
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid expression: %w", err)
	}

	switch ExpressionClass(raw.ExpressionClass) {
	case ClassBoundComparison:
		return parseComparisonExpression(data, columnBindings)
	case ClassBoundConjunction:
		return parseConjunctionExpression(data, columnBindings)
	case ClassBoundConstant:
		return parseConstantExpression(data)
	case ClassBoundColumnRef:
		return parseColumnRefExpression(data)
	case ClassBoundFunction:
		return parseFunctionExpression(data, columnBindings)
	case ClassBoundCast:
		return parseCastExpression(data, columnBindings)
	case ClassBoundBetween:
		return parseBetweenExpression(data, columnBindings)
	case ClassBoundOperator:
		return parseOperatorExpression(data, columnBindings)
	case ClassBoundCase:
		return parseCaseExpression(data, columnBindings)
	case ClassBoundParameter:
		return parseParameterExpression(data)
	case ClassBoundRef:
		return parseReferenceExpression(data)
	case ClassBoundAggregate:
		return parseAggregateExpression(data, columnBindings)
	case ClassBoundWindow:
		return parseWindowExpression(data, columnBindings)
	default:
		// Return an unsupported expression that can be identified during encoding
		return &UnsupportedExpression{
			BaseExpression: BaseExpression{
				ExprClass: ExpressionClass(raw.ExpressionClass),
				ExprType:  ExpressionType(raw.Type),
				ExprAlias: raw.Alias,
			},
		}, nil
	}
}

// rawComparison is the JSON structure for comparison expressions.
type rawComparison struct {
	ExpressionClass string          `json:"expression_class"`
	Type            string          `json:"type"`
	Alias           string          `json:"alias"`
	Left            json.RawMessage `json:"left"`
	Right           json.RawMessage `json:"right"`
}

func parseComparisonExpression(data json.RawMessage, columnBindings []string) (*ComparisonExpression, error) {
	var raw rawComparison
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid comparison expression: %w", err)
	}

	left, err := parseExpression(raw.Left, columnBindings)
	if err != nil {
		return nil, fmt.Errorf("invalid left operand: %w", err)
	}

	right, err := parseExpression(raw.Right, columnBindings)
	if err != nil {
		return nil, fmt.Errorf("invalid right operand: %w", err)
	}

	return &ComparisonExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Left:  left,
		Right: right,
	}, nil
}

// rawConjunction is the JSON structure for conjunction expressions.
type rawConjunction struct {
	ExpressionClass string            `json:"expression_class"`
	Type            string            `json:"type"`
	Alias           string            `json:"alias"`
	Children        []json.RawMessage `json:"children"`
}

func parseConjunctionExpression(data json.RawMessage, columnBindings []string) (*ConjunctionExpression, error) {
	var raw rawConjunction
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid conjunction expression: %w", err)
	}

	children := make([]Expression, 0, len(raw.Children))
	for i, child := range raw.Children {
		expr, err := parseExpression(child, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid child %d: %w", i, err)
		}
		children = append(children, expr)
	}

	return &ConjunctionExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Children: children,
	}, nil
}

// rawConstant is the JSON structure for constant expressions.
type rawConstant struct {
	ExpressionClass string          `json:"expression_class"`
	Type            string          `json:"type"`
	Alias           string          `json:"alias"`
	Value           json.RawMessage `json:"value"`
}

func parseConstantExpression(data json.RawMessage) (*ConstantExpression, error) {
	var raw rawConstant
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid constant expression: %w", err)
	}

	value, err := parseValue(raw.Value)
	if err != nil {
		return nil, fmt.Errorf("invalid value: %w", err)
	}

	return &ConstantExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Value: value,
	}, nil
}

// rawColumnRef is the JSON structure for column reference expressions.
type rawColumnRef struct {
	ExpressionClass string          `json:"expression_class"`
	Type            string          `json:"type"`
	Alias           string          `json:"alias"`
	ReturnType      json.RawMessage `json:"return_type"`
	Binding         ColumnBinding   `json:"binding"`
	Depth           int             `json:"depth"`
}

func parseColumnRefExpression(data json.RawMessage) (*ColumnRefExpression, error) {
	var raw rawColumnRef
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid column ref expression: %w", err)
	}

	returnType, err := parseLogicalType(raw.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("invalid return type: %w", err)
	}

	return &ColumnRefExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		ReturnType: returnType,
		Binding:    raw.Binding,
		Depth:      raw.Depth,
	}, nil
}

// rawFunction is the JSON structure for function expressions.
type rawFunction struct {
	ExpressionClass   string            `json:"expression_class"`
	Type              string            `json:"type"`
	Alias             string            `json:"alias"`
	ReturnType        json.RawMessage   `json:"return_type"`
	Children          []json.RawMessage `json:"children"`
	Name              string            `json:"name"`
	Arguments         []json.RawMessage `json:"arguments"`
	OriginalArguments []json.RawMessage `json:"original_arguments"`
	CatalogName       string            `json:"catalog_name"`
	SchemaName        string            `json:"schema_name"`
	HasSerialize      bool              `json:"has_serialize"`
	IsOperator        bool              `json:"is_operator"`
}

func parseFunctionExpression(data json.RawMessage, columnBindings []string) (*FunctionExpression, error) {
	var raw rawFunction
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid function expression: %w", err)
	}

	returnType, err := parseLogicalType(raw.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("invalid return type: %w", err)
	}

	children := make([]Expression, 0, len(raw.Children))
	for i, child := range raw.Children {
		expr, err := parseExpression(child, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid child %d: %w", i, err)
		}
		children = append(children, expr)
	}

	arguments := make([]LogicalType, 0, len(raw.Arguments))
	for _, arg := range raw.Arguments {
		argType, err := parseLogicalType(arg)
		if err != nil {
			return nil, fmt.Errorf("invalid argument type: %w", err)
		}
		arguments = append(arguments, argType)
	}

	originalArguments := make([]LogicalType, 0, len(raw.OriginalArguments))
	for _, arg := range raw.OriginalArguments {
		argType, err := parseLogicalType(arg)
		if err != nil {
			return nil, fmt.Errorf("invalid original argument type: %w", err)
		}
		originalArguments = append(originalArguments, argType)
	}

	return &FunctionExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Name:              raw.Name,
		Children:          children,
		ReturnType:        returnType,
		Arguments:         arguments,
		OriginalArguments: originalArguments,
		CatalogName:       raw.CatalogName,
		SchemaName:        raw.SchemaName,
		HasSerialize:      raw.HasSerialize,
		IsOperator:        raw.IsOperator,
	}, nil
}

// rawCast is the JSON structure for cast expressions.
type rawCast struct {
	ExpressionClass string          `json:"expression_class"`
	Type            string          `json:"type"`
	Alias           string          `json:"alias"`
	Child           json.RawMessage `json:"child"`
	ReturnType      json.RawMessage `json:"return_type"`
	TryCast         bool            `json:"try_cast"`
}

func parseCastExpression(data json.RawMessage, columnBindings []string) (*CastExpression, error) {
	var raw rawCast
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid cast expression: %w", err)
	}

	child, err := parseExpression(raw.Child, columnBindings)
	if err != nil {
		return nil, fmt.Errorf("invalid child: %w", err)
	}

	returnType, err := parseLogicalType(raw.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("invalid return type: %w", err)
	}

	return &CastExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Child:      child,
		ReturnType: returnType,
		TryCast:    raw.TryCast,
	}, nil
}

// rawBetween is the JSON structure for between expressions.
type rawBetween struct {
	ExpressionClass string          `json:"expression_class"`
	Type            string          `json:"type"`
	Alias           string          `json:"alias"`
	Input           json.RawMessage `json:"input"`
	Lower           json.RawMessage `json:"lower"`
	Upper           json.RawMessage `json:"upper"`
	LowerInclusive  bool            `json:"lower_inclusive"`
	UpperInclusive  bool            `json:"upper_inclusive"`
}

func parseBetweenExpression(data json.RawMessage, columnBindings []string) (*BetweenExpression, error) {
	var raw rawBetween
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid between expression: %w", err)
	}

	input, err := parseExpression(raw.Input, columnBindings)
	if err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}

	lower, err := parseExpression(raw.Lower, columnBindings)
	if err != nil {
		return nil, fmt.Errorf("invalid lower bound: %w", err)
	}

	upper, err := parseExpression(raw.Upper, columnBindings)
	if err != nil {
		return nil, fmt.Errorf("invalid upper bound: %w", err)
	}

	return &BetweenExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Input:          input,
		Lower:          lower,
		Upper:          upper,
		LowerInclusive: raw.LowerInclusive,
		UpperInclusive: raw.UpperInclusive,
	}, nil
}

// rawOperator is the JSON structure for operator expressions.
type rawOperator struct {
	ExpressionClass string            `json:"expression_class"`
	Type            string            `json:"type"`
	Alias           string            `json:"alias"`
	ReturnType      json.RawMessage   `json:"return_type"`
	Children        []json.RawMessage `json:"children"`
}

func parseOperatorExpression(data json.RawMessage, columnBindings []string) (*OperatorExpression, error) {
	var raw rawOperator
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid operator expression: %w", err)
	}

	returnType, err := parseLogicalType(raw.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("invalid return type: %w", err)
	}

	children := make([]Expression, 0, len(raw.Children))
	for i, child := range raw.Children {
		expr, err := parseExpression(child, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid child %d: %w", i, err)
		}
		children = append(children, expr)
	}

	return &OperatorExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Children:   children,
		ReturnType: returnType,
	}, nil
}

// rawCase is the JSON structure for case expressions.
type rawCase struct {
	ExpressionClass string          `json:"expression_class"`
	Type            string          `json:"type"`
	Alias           string          `json:"alias"`
	ReturnType      json.RawMessage `json:"return_type"`
	CaseChecks      []rawCaseCheck  `json:"case_checks"`
	ElseExpr        json.RawMessage `json:"else_expr"`
}

type rawCaseCheck struct {
	WhenExpr json.RawMessage `json:"when_expr"`
	ThenExpr json.RawMessage `json:"then_expr"`
}

func parseCaseExpression(data json.RawMessage, columnBindings []string) (*CaseExpression, error) {
	var raw rawCase
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid case expression: %w", err)
	}

	returnType, err := parseLogicalType(raw.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("invalid return type: %w", err)
	}

	caseChecks := make([]CaseCheck, 0, len(raw.CaseChecks))
	for i, check := range raw.CaseChecks {
		whenExpr, err := parseExpression(check.WhenExpr, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid when expression %d: %w", i, err)
		}
		thenExpr, err := parseExpression(check.ThenExpr, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid then expression %d: %w", i, err)
		}
		caseChecks = append(caseChecks, CaseCheck{
			WhenExpr: whenExpr,
			ThenExpr: thenExpr,
		})
	}

	var elseExpr Expression
	if len(raw.ElseExpr) > 0 && string(raw.ElseExpr) != "null" {
		elseExpr, err = parseExpression(raw.ElseExpr, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid else expression: %w", err)
		}
	}

	return &CaseExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		CaseChecks: caseChecks,
		ElseExpr:   elseExpr,
		ReturnType: returnType,
	}, nil
}

// rawParameter is the JSON structure for parameter expressions.
type rawParameter struct {
	ExpressionClass string          `json:"expression_class"`
	Type            string          `json:"type"`
	Alias           string          `json:"alias"`
	Identifier      string          `json:"identifier"`
	ReturnType      json.RawMessage `json:"return_type"`
}

func parseParameterExpression(data json.RawMessage) (*ParameterExpression, error) {
	var raw rawParameter
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid parameter expression: %w", err)
	}

	returnType, err := parseLogicalType(raw.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("invalid return type: %w", err)
	}

	return &ParameterExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Identifier: raw.Identifier,
		ReturnType: returnType,
	}, nil
}

// rawReference is the JSON structure for reference expressions.
type rawReference struct {
	ExpressionClass string          `json:"expression_class"`
	Type            string          `json:"type"`
	Alias           string          `json:"alias"`
	ReturnType      json.RawMessage `json:"return_type"`
	Index           int             `json:"index"`
}

func parseReferenceExpression(data json.RawMessage) (*ReferenceExpression, error) {
	var raw rawReference
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid reference expression: %w", err)
	}

	returnType, err := parseLogicalType(raw.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("invalid return type: %w", err)
	}

	return &ReferenceExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		ReturnType: returnType,
		Index:      raw.Index,
	}, nil
}

// rawAggregate is the JSON structure for aggregate expressions.
type rawAggregate struct {
	ExpressionClass   string            `json:"expression_class"`
	Type              string            `json:"type"`
	Alias             string            `json:"alias"`
	Name              string            `json:"name"`
	Children          []json.RawMessage `json:"children"`
	ReturnType        json.RawMessage   `json:"return_type"`
	Arguments         []json.RawMessage `json:"arguments"`
	OriginalArguments []json.RawMessage `json:"original_arguments"`
	AggregateType     string            `json:"aggregate_type"`
	Filter            json.RawMessage   `json:"filter"`
	HasSerialize      bool              `json:"has_serialize"`
}

func parseAggregateExpression(data json.RawMessage, columnBindings []string) (*AggregateExpression, error) {
	var raw rawAggregate
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid aggregate expression: %w", err)
	}

	returnType, err := parseLogicalType(raw.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("invalid return type: %w", err)
	}

	children := make([]Expression, 0, len(raw.Children))
	for i, child := range raw.Children {
		expr, err := parseExpression(child, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid child %d: %w", i, err)
		}
		children = append(children, expr)
	}

	arguments := make([]LogicalType, 0, len(raw.Arguments))
	for _, arg := range raw.Arguments {
		argType, err := parseLogicalType(arg)
		if err != nil {
			return nil, fmt.Errorf("invalid argument type: %w", err)
		}
		arguments = append(arguments, argType)
	}

	originalArguments := make([]LogicalType, 0, len(raw.OriginalArguments))
	for _, arg := range raw.OriginalArguments {
		argType, err := parseLogicalType(arg)
		if err != nil {
			return nil, fmt.Errorf("invalid original argument type: %w", err)
		}
		originalArguments = append(originalArguments, argType)
	}

	var filter Expression
	if len(raw.Filter) > 0 && string(raw.Filter) != "null" {
		filter, err = parseExpression(raw.Filter, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid filter: %w", err)
		}
	}

	return &AggregateExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Name:              raw.Name,
		Children:          children,
		ReturnType:        returnType,
		Arguments:         arguments,
		OriginalArguments: originalArguments,
		AggregateType:     raw.AggregateType,
		Filter:            filter,
		HasSerialize:      raw.HasSerialize,
	}, nil
}

// rawWindow is the JSON structure for window expressions.
type rawWindow struct {
	ExpressionClass string            `json:"expression_class"`
	Type            string            `json:"type"`
	Alias           string            `json:"alias"`
	Children        []json.RawMessage `json:"children"`
	Partitions      []json.RawMessage `json:"partitions"`
	ReturnType      json.RawMessage   `json:"return_type"`
	IgnoreNulls     bool              `json:"ignore_nulls"`
	Distinct        bool              `json:"distinct"`
	Start           string            `json:"start"`
	End             string            `json:"end"`
	ExcludeClause   string            `json:"exclude_clause"`
}

func parseWindowExpression(data json.RawMessage, columnBindings []string) (*WindowExpression, error) {
	var raw rawWindow
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid window expression: %w", err)
	}

	returnType, err := parseLogicalType(raw.ReturnType)
	if err != nil {
		return nil, fmt.Errorf("invalid return type: %w", err)
	}

	children := make([]Expression, 0, len(raw.Children))
	for i, child := range raw.Children {
		expr, err := parseExpression(child, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid child %d: %w", i, err)
		}
		children = append(children, expr)
	}

	partitions := make([]Expression, 0, len(raw.Partitions))
	for i, part := range raw.Partitions {
		expr, err := parseExpression(part, columnBindings)
		if err != nil {
			return nil, fmt.Errorf("invalid partition %d: %w", i, err)
		}
		partitions = append(partitions, expr)
	}

	return &WindowExpression{
		BaseExpression: BaseExpression{
			ExprClass: ExpressionClass(raw.ExpressionClass),
			ExprType:  ExpressionType(raw.Type),
			ExprAlias: raw.Alias,
		},
		Children:      children,
		Partitions:    partitions,
		ReturnType:    returnType,
		IgnoreNulls:   raw.IgnoreNulls,
		Distinct:      raw.Distinct,
		Start:         raw.Start,
		End:           raw.End,
		ExcludeClause: raw.ExcludeClause,
	}, nil
}

// parseLogicalType parses a LogicalType from JSON.
func parseLogicalType(data json.RawMessage) (LogicalType, error) {
	if len(data) == 0 || string(data) == "null" {
		return LogicalType{}, nil
	}

	var raw struct {
		ID       string          `json:"id"`
		TypeInfo json.RawMessage `json:"type_info"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return LogicalType{}, fmt.Errorf("invalid logical type: %w", err)
	}

	// Normalize the type ID to handle DuckDB aliases and full SQL names
	lt := LogicalType{
		ID: LogicalTypeID(raw.ID).Normalize(),
	}

	if len(raw.TypeInfo) > 0 && string(raw.TypeInfo) != "null" {
		typeInfo, err := parseExtraTypeInfo(raw.TypeInfo, lt.ID)
		if err != nil {
			return LogicalType{}, fmt.Errorf("invalid type info: %w", err)
		}
		lt.TypeInfo = typeInfo
	}

	return lt, nil
}

// parseExtraTypeInfo parses ExtraTypeInfo based on the logical type.
func parseExtraTypeInfo(data json.RawMessage, _ LogicalTypeID) (ExtraTypeInfo, error) {
	// First, determine the type_info type
	var typeCheck struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &typeCheck); err != nil {
		return nil, err
	}

	switch typeCheck.Type {
	case "DECIMAL_TYPE_INFO":
		var info DecimalTypeInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, err
		}
		return &info, nil

	case "LIST_TYPE_INFO":
		var raw struct {
			Type      string          `json:"type"`
			Alias     string          `json:"alias"`
			ChildType json.RawMessage `json:"child_type"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		childType, err := parseLogicalType(raw.ChildType)
		if err != nil {
			return nil, err
		}
		return &ListTypeInfo{
			Type:      raw.Type,
			Alias:     raw.Alias,
			ChildType: childType,
		}, nil

	case "STRUCT_TYPE_INFO":
		var raw struct {
			Type       string `json:"type"`
			Alias      string `json:"alias"`
			ChildTypes []struct {
				First  string          `json:"first"`
				Second json.RawMessage `json:"second"`
			} `json:"child_types"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		childTypes := make([]StructField, 0, len(raw.ChildTypes))
		for _, ct := range raw.ChildTypes {
			childType, err := parseLogicalType(ct.Second)
			if err != nil {
				return nil, err
			}
			childTypes = append(childTypes, StructField{
				Name: ct.First,
				Type: childType,
			})
		}
		return &StructTypeInfo{
			Type:       raw.Type,
			Alias:      raw.Alias,
			ChildTypes: childTypes,
		}, nil

	case "ARRAY_TYPE_INFO":
		var raw struct {
			Type      string          `json:"type"`
			Alias     string          `json:"alias"`
			ChildType json.RawMessage `json:"child_type"`
			Size      int             `json:"size"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		childType, err := parseLogicalType(raw.ChildType)
		if err != nil {
			return nil, err
		}
		return &ArrayTypeInfo{
			Type:      raw.Type,
			Alias:     raw.Alias,
			ChildType: childType,
			Size:      raw.Size,
		}, nil

	case "ENUM_TYPE_INFO":
		var info EnumTypeInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, err
		}
		return &info, nil

	default:
		// Unknown type info, return nil (not an error)
		return nil, nil
	}
}

// parseValue parses a Value from JSON.
func parseValue(data json.RawMessage) (Value, error) {
	if len(data) == 0 || string(data) == "null" {
		return Value{IsNull: true}, nil
	}

	var raw struct {
		Type   json.RawMessage `json:"type"`
		IsNull bool            `json:"is_null"`
		Value  json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Value{}, fmt.Errorf("invalid value: %w", err)
	}

	logicalType, err := parseLogicalType(raw.Type)
	if err != nil {
		return Value{}, fmt.Errorf("invalid value type: %w", err)
	}

	v := Value{
		Type:   logicalType,
		IsNull: raw.IsNull,
	}

	if raw.IsNull || len(raw.Value) == 0 || string(raw.Value) == "null" {
		return v, nil
	}

	// Parse the value based on type
	v.Data, err = parseValueData(raw.Value, logicalType)
	if err != nil {
		return Value{}, fmt.Errorf("invalid value data: %w", err)
	}

	return v, nil
}

// parseValueData parses the actual value data based on the logical type.
func parseValueData(data json.RawMessage, lt LogicalType) (any, error) {
	switch lt.ID {
	case TypeIDBoolean:
		var v bool
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		return v, nil

	case TypeIDTinyInt, TypeIDSmallInt, TypeIDInteger, TypeIDBigInt:
		var v int64
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		return v, nil

	case TypeIDUTinyInt, TypeIDUSmallInt, TypeIDUInteger, TypeIDUBigInt:
		var v uint64
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		return v, nil

	case TypeIDHugeInt:
		var v HugeInt
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		return v, nil

	case TypeIDUHugeInt:
		var v UHugeInt
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		return v, nil

	case TypeIDFloat, TypeIDDouble:
		var v float64
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		return v, nil

	case TypeIDDecimal:
		// Decimal can be string or number
		var s string
		if err := json.Unmarshal(data, &s); err == nil {
			return s, nil
		}
		var f float64
		if err := json.Unmarshal(data, &f); err != nil {
			return nil, err
		}
		return f, nil

	case TypeIDVarchar, TypeIDChar:
		// Check for base64-encoded string
		var base64Str Base64String
		if err := json.Unmarshal(data, &base64Str); err == nil && base64Str.Base64 != "" {
			decoded, err := base64.StdEncoding.DecodeString(base64Str.Base64)
			if err != nil {
				return nil, fmt.Errorf("invalid base64: %w", err)
			}
			return string(decoded), nil
		}
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, err
		}
		return s, nil

	case TypeIDBlob:
		// Blob can be base64-encoded
		var base64Str Base64String
		if err := json.Unmarshal(data, &base64Str); err == nil && base64Str.Base64 != "" {
			decoded, err := base64.StdEncoding.DecodeString(base64Str.Base64)
			if err != nil {
				return nil, fmt.Errorf("invalid base64: %w", err)
			}
			return decoded, nil
		}
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, err
		}
		return []byte(s), nil

	case TypeIDDate, TypeIDTime, TypeIDTimeTZ,
		TypeIDTimestamp, TypeIDTimestampTZ, TypeIDTimestampMs, TypeIDTimestampNs, TypeIDTimestampSec:
		// Temporal types are integers
		var v int64
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		return v, nil

	case TypeIDInterval:
		var v Interval
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		return v, nil

	case TypeIDUUID:
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, err
		}
		return s, nil

	case TypeIDList, TypeIDArray:
		var raw struct {
			Children []json.RawMessage `json:"children"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		children := make([]Value, 0, len(raw.Children))
		for _, child := range raw.Children {
			v, err := parseValue(child)
			if err != nil {
				return nil, err
			}
			children = append(children, v)
		}
		return ListValue{Children: children}, nil

	case TypeIDStruct:
		var raw struct {
			Children []json.RawMessage `json:"children"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		children := make([]Value, 0, len(raw.Children))
		for _, child := range raw.Children {
			v, err := parseValue(child)
			if err != nil {
				return nil, err
			}
			children = append(children, v)
		}
		return StructValue{Children: children}, nil

	case TypeIDMap:
		var raw struct {
			Keys   []json.RawMessage `json:"keys"`
			Values []json.RawMessage `json:"values"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			// Try alternative format with children
			var alt struct {
				Children []json.RawMessage `json:"children"`
			}
			if err := json.Unmarshal(data, &alt); err != nil {
				return nil, err
			}
			children := make([]Value, 0, len(alt.Children))
			for _, child := range alt.Children {
				v, err := parseValue(child)
				if err != nil {
					return nil, err
				}
				children = append(children, v)
			}
			return StructValue{Children: children}, nil
		}
		keys := make([]Value, 0, len(raw.Keys))
		for _, key := range raw.Keys {
			v, err := parseValue(key)
			if err != nil {
				return nil, err
			}
			keys = append(keys, v)
		}
		values := make([]Value, 0, len(raw.Values))
		for _, val := range raw.Values {
			v, err := parseValue(val)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		return MapValue{Keys: keys, Values: values}, nil

	default:
		// For unknown types, try to parse as generic JSON
		var v any
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		return v, nil
	}
}
