package filter

import (
	"testing"
)

func TestParseEmpty(t *testing.T) {
	fp, err := Parse(nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(fp.Filters) != 0 {
		t.Errorf("expected 0 filters, got %d", len(fp.Filters))
	}

	fp, err = Parse([]byte{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(fp.Filters) != 0 {
		t.Errorf("expected 0 filters, got %d", len(fp.Filters))
	}
}

func TestParseSimpleEquality(t *testing.T) {
	// WHERE id = 42
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_COMPARISON",
				"type": "COMPARE_EQUAL",
				"alias": "",
				"left": {
					"expression_class": "BOUND_COLUMN_REF",
					"type": "BOUND_COLUMN_REF",
					"alias": "",
					"return_type": {"id": "INTEGER", "type_info": null},
					"binding": {"table_index": 0, "column_index": 0},
					"depth": 0
				},
				"right": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {
						"type": {"id": "INTEGER", "type_info": null},
						"is_null": false,
						"value": 42
					}
				}
			}
		],
		"column_binding_names_by_index": ["id", "name", "value"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(fp.Filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(fp.Filters))
	}

	if len(fp.ColumnBindings) != 3 {
		t.Errorf("expected 3 column bindings, got %d", len(fp.ColumnBindings))
	}

	comp, ok := fp.Filters[0].(*ComparisonExpression)
	if !ok {
		t.Fatalf("expected ComparisonExpression, got %T", fp.Filters[0])
	}

	if comp.Type() != TypeCompareEqual {
		t.Errorf("expected COMPARE_EQUAL, got %s", comp.Type())
	}

	colRef, ok := comp.Left.(*ColumnRefExpression)
	if !ok {
		t.Fatalf("expected ColumnRefExpression on left, got %T", comp.Left)
	}
	if colRef.Binding.ColumnIndex != 0 {
		t.Errorf("expected column index 0, got %d", colRef.Binding.ColumnIndex)
	}

	name, err := fp.ColumnName(colRef)
	if err != nil {
		t.Errorf("ColumnName failed: %v", err)
	}
	if name != "id" {
		t.Errorf("expected column name 'id', got '%s'", name)
	}

	constExpr, ok := comp.Right.(*ConstantExpression)
	if !ok {
		t.Fatalf("expected ConstantExpression on right, got %T", comp.Right)
	}
	if constExpr.Value.IsNull {
		t.Error("expected non-null value")
	}
	if v, ok := constExpr.Value.Data.(int64); !ok || v != 42 {
		t.Errorf("expected value 42, got %v", constExpr.Value.Data)
	}
}

func TestParseConjunction(t *testing.T) {
	// WHERE status = 'active' AND age > 18
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_CONJUNCTION",
				"type": "CONJUNCTION_AND",
				"alias": "",
				"children": [
					{
						"expression_class": "BOUND_COMPARISON",
						"type": "COMPARE_EQUAL",
						"alias": "",
						"left": {
							"expression_class": "BOUND_COLUMN_REF",
							"type": "BOUND_COLUMN_REF",
							"alias": "",
							"return_type": {"id": "VARCHAR", "type_info": null},
							"binding": {"table_index": 0, "column_index": 0},
							"depth": 0
						},
						"right": {
							"expression_class": "BOUND_CONSTANT",
							"type": "VALUE_CONSTANT",
							"alias": "",
							"value": {
								"type": {"id": "VARCHAR", "type_info": null},
								"is_null": false,
								"value": "active"
							}
						}
					},
					{
						"expression_class": "BOUND_COMPARISON",
						"type": "COMPARE_GREATERTHAN",
						"alias": "",
						"left": {
							"expression_class": "BOUND_COLUMN_REF",
							"type": "BOUND_COLUMN_REF",
							"alias": "",
							"return_type": {"id": "INTEGER", "type_info": null},
							"binding": {"table_index": 0, "column_index": 1},
							"depth": 0
						},
						"right": {
							"expression_class": "BOUND_CONSTANT",
							"type": "VALUE_CONSTANT",
							"alias": "",
							"value": {
								"type": {"id": "INTEGER", "type_info": null},
								"is_null": false,
								"value": 18
							}
						}
					}
				]
			}
		],
		"column_binding_names_by_index": ["status", "age"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	conj, ok := fp.Filters[0].(*ConjunctionExpression)
	if !ok {
		t.Fatalf("expected ConjunctionExpression, got %T", fp.Filters[0])
	}

	if conj.Type() != TypeConjunctionAnd {
		t.Errorf("expected CONJUNCTION_AND, got %s", conj.Type())
	}

	if len(conj.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(conj.Children))
	}

	// Check first child
	comp1, ok := conj.Children[0].(*ComparisonExpression)
	if !ok {
		t.Fatalf("expected ComparisonExpression for child 0, got %T", conj.Children[0])
	}
	if comp1.Type() != TypeCompareEqual {
		t.Errorf("expected COMPARE_EQUAL, got %s", comp1.Type())
	}

	// Check second child
	comp2, ok := conj.Children[1].(*ComparisonExpression)
	if !ok {
		t.Fatalf("expected ComparisonExpression for child 1, got %T", conj.Children[1])
	}
	if comp2.Type() != TypeCompareGreaterThan {
		t.Errorf("expected COMPARE_GREATERTHAN, got %s", comp2.Type())
	}
}

func TestParseFunction(t *testing.T) {
	// WHERE LOWER(name) = 'john'
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_COMPARISON",
				"type": "COMPARE_EQUAL",
				"alias": "",
				"left": {
					"expression_class": "BOUND_FUNCTION",
					"type": "BOUND_FUNCTION",
					"alias": "",
					"return_type": {"id": "VARCHAR", "type_info": null},
					"children": [
						{
							"expression_class": "BOUND_COLUMN_REF",
							"type": "BOUND_COLUMN_REF",
							"alias": "",
							"return_type": {"id": "VARCHAR", "type_info": null},
							"binding": {"table_index": 0, "column_index": 0},
							"depth": 0
						}
					],
					"name": "lower",
					"arguments": [{"id": "VARCHAR", "type_info": null}],
					"original_arguments": [{"id": "VARCHAR", "type_info": null}],
					"catalog_name": "",
					"schema_name": "",
					"has_serialize": false,
					"is_operator": false
				},
				"right": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {
						"type": {"id": "VARCHAR", "type_info": null},
						"is_null": false,
						"value": "john"
					}
				}
			}
		],
		"column_binding_names_by_index": ["name"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	comp, ok := fp.Filters[0].(*ComparisonExpression)
	if !ok {
		t.Fatalf("expected ComparisonExpression, got %T", fp.Filters[0])
	}

	funcExpr, ok := comp.Left.(*FunctionExpression)
	if !ok {
		t.Fatalf("expected FunctionExpression on left, got %T", comp.Left)
	}

	if funcExpr.Name != "lower" {
		t.Errorf("expected function name 'lower', got '%s'", funcExpr.Name)
	}

	if len(funcExpr.Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(funcExpr.Children))
	}

	if funcExpr.IsOperator {
		t.Error("expected IsOperator to be false")
	}
}

func TestParseCast(t *testing.T) {
	// WHERE CAST(value AS INTEGER) > 10
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_COMPARISON",
				"type": "COMPARE_GREATERTHAN",
				"alias": "",
				"left": {
					"expression_class": "BOUND_CAST",
					"type": "CAST",
					"alias": "",
					"child": {
						"expression_class": "BOUND_COLUMN_REF",
						"type": "BOUND_COLUMN_REF",
						"alias": "",
						"return_type": {"id": "VARCHAR", "type_info": null},
						"binding": {"table_index": 0, "column_index": 0},
						"depth": 0
					},
					"return_type": {"id": "INTEGER", "type_info": null},
					"try_cast": false
				},
				"right": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {
						"type": {"id": "INTEGER", "type_info": null},
						"is_null": false,
						"value": 10
					}
				}
			}
		],
		"column_binding_names_by_index": ["value"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	comp, ok := fp.Filters[0].(*ComparisonExpression)
	if !ok {
		t.Fatalf("expected ComparisonExpression, got %T", fp.Filters[0])
	}

	castExpr, ok := comp.Left.(*CastExpression)
	if !ok {
		t.Fatalf("expected CastExpression on left, got %T", comp.Left)
	}

	if castExpr.ReturnType.ID != TypeIDInteger {
		t.Errorf("expected INTEGER return type, got %s", castExpr.ReturnType.ID)
	}

	if castExpr.TryCast {
		t.Error("expected TryCast to be false")
	}
}

func TestParseBetween(t *testing.T) {
	// WHERE price BETWEEN 100 AND 500
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_BETWEEN",
				"type": "COMPARE_BETWEEN",
				"alias": "",
				"input": {
					"expression_class": "BOUND_COLUMN_REF",
					"type": "BOUND_COLUMN_REF",
					"alias": "",
					"return_type": {"id": "DECIMAL", "type_info": {"type": "DECIMAL_TYPE_INFO", "width": 10, "scale": 2}},
					"binding": {"table_index": 0, "column_index": 0},
					"depth": 0
				},
				"lower": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {
						"type": {"id": "INTEGER", "type_info": null},
						"is_null": false,
						"value": 100
					}
				},
				"upper": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {
						"type": {"id": "INTEGER", "type_info": null},
						"is_null": false,
						"value": 500
					}
				},
				"lower_inclusive": true,
				"upper_inclusive": true
			}
		],
		"column_binding_names_by_index": ["price"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	between, ok := fp.Filters[0].(*BetweenExpression)
	if !ok {
		t.Fatalf("expected BetweenExpression, got %T", fp.Filters[0])
	}

	if !between.LowerInclusive || !between.UpperInclusive {
		t.Error("expected both bounds to be inclusive")
	}

	colRef, ok := between.Input.(*ColumnRefExpression)
	if !ok {
		t.Fatalf("expected ColumnRefExpression for input, got %T", between.Input)
	}

	// Check decimal type info
	if colRef.ReturnType.ID != TypeIDDecimal {
		t.Errorf("expected DECIMAL, got %s", colRef.ReturnType.ID)
	}
	decInfo, ok := colRef.ReturnType.TypeInfo.(*DecimalTypeInfo)
	if !ok {
		t.Fatalf("expected DecimalTypeInfo, got %T", colRef.ReturnType.TypeInfo)
	}
	if decInfo.Width != 10 || decInfo.Scale != 2 {
		t.Errorf("expected DECIMAL(10,2), got DECIMAL(%d,%d)", decInfo.Width, decInfo.Scale)
	}
}

func TestParseOperator(t *testing.T) {
	// WHERE deleted_at IS NULL
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_OPERATOR",
				"type": "OPERATOR_IS_NULL",
				"alias": "",
				"return_type": {"id": "BOOLEAN", "type_info": null},
				"children": [
					{
						"expression_class": "BOUND_COLUMN_REF",
						"type": "BOUND_COLUMN_REF",
						"alias": "",
						"return_type": {"id": "TIMESTAMP", "type_info": null},
						"binding": {"table_index": 0, "column_index": 0},
						"depth": 0
					}
				]
			}
		],
		"column_binding_names_by_index": ["deleted_at"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	op, ok := fp.Filters[0].(*OperatorExpression)
	if !ok {
		t.Fatalf("expected OperatorExpression, got %T", fp.Filters[0])
	}

	if op.Type() != TypeOperatorIsNull {
		t.Errorf("expected OPERATOR_IS_NULL, got %s", op.Type())
	}

	if len(op.Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(op.Children))
	}
}

func TestParseMalformedJSON(t *testing.T) {
	_, err := Parse([]byte(`{invalid json}`))
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestParseInvalidColumnBinding(t *testing.T) {
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_COLUMN_REF",
				"type": "BOUND_COLUMN_REF",
				"alias": "",
				"return_type": {"id": "INTEGER", "type_info": null},
				"binding": {"table_index": 0, "column_index": 5},
				"depth": 0
			}
		],
		"column_binding_names_by_index": ["id"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	colRef := fp.Filters[0].(*ColumnRefExpression)
	_, err = fp.ColumnName(colRef)
	if err == nil {
		t.Error("expected error for invalid column binding index")
	}
}

func TestParseUnsupportedExpression(t *testing.T) {
	// Unsupported expression class should still parse
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_SUBQUERY",
				"type": "SOME_TYPE",
				"alias": ""
			}
		],
		"column_binding_names_by_index": []
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	_, ok := fp.Filters[0].(*UnsupportedExpression)
	if !ok {
		t.Fatalf("expected UnsupportedExpression, got %T", fp.Filters[0])
	}
}

func TestParseCase(t *testing.T) {
	// CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_COMPARISON",
				"type": "COMPARE_EQUAL",
				"alias": "",
				"left": {
					"expression_class": "BOUND_CASE",
					"type": "CASE_EXPR",
					"alias": "",
					"return_type": {"id": "VARCHAR", "type_info": null},
					"case_checks": [
						{
							"when_expr": {
								"expression_class": "BOUND_COMPARISON",
								"type": "COMPARE_GREATERTHAN",
								"alias": "",
								"left": {
									"expression_class": "BOUND_COLUMN_REF",
									"type": "BOUND_COLUMN_REF",
									"alias": "",
									"return_type": {"id": "INTEGER", "type_info": null},
									"binding": {"table_index": 0, "column_index": 0},
									"depth": 0
								},
								"right": {
									"expression_class": "BOUND_CONSTANT",
									"type": "VALUE_CONSTANT",
									"alias": "",
									"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 0}
								}
							},
							"then_expr": {
								"expression_class": "BOUND_CONSTANT",
								"type": "VALUE_CONSTANT",
								"alias": "",
								"value": {"type": {"id": "VARCHAR", "type_info": null}, "is_null": false, "value": "positive"}
							}
						}
					],
					"else_expr": {
						"expression_class": "BOUND_CONSTANT",
						"type": "VALUE_CONSTANT",
						"alias": "",
						"value": {"type": {"id": "VARCHAR", "type_info": null}, "is_null": false, "value": "non-positive"}
					}
				},
				"right": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {"type": {"id": "VARCHAR", "type_info": null}, "is_null": false, "value": "positive"}
				}
			}
		],
		"column_binding_names_by_index": ["x"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	comp, ok := fp.Filters[0].(*ComparisonExpression)
	if !ok {
		t.Fatalf("expected ComparisonExpression, got %T", fp.Filters[0])
	}

	caseExpr, ok := comp.Left.(*CaseExpression)
	if !ok {
		t.Fatalf("expected CaseExpression, got %T", comp.Left)
	}

	if len(caseExpr.CaseChecks) != 1 {
		t.Errorf("expected 1 case check, got %d", len(caseExpr.CaseChecks))
	}

	if caseExpr.ElseExpr == nil {
		t.Error("expected else expression")
	}
}
