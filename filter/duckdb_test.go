package filter

import (
	"strings"
	"testing"
)

func TestEncodeSimpleEquality(t *testing.T) {
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
		"column_binding_names_by_index": ["id"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "id = 42"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeComparisonOperators(t *testing.T) {
	tests := []struct {
		opType   string
		expected string
	}{
		{"COMPARE_EQUAL", "col = 42"},
		{"COMPARE_NOTEQUAL", "col <> 42"},
		{"COMPARE_LESSTHAN", "col < 42"},
		{"COMPARE_GREATERTHAN", "col > 42"},
		{"COMPARE_LESSTHANOREQUALTO", "col <= 42"},
		{"COMPARE_GREATERTHANOREQUALTO", "col >= 42"},
	}

	for _, tt := range tests {
		t.Run(tt.opType, func(t *testing.T) {
			json := []byte(`{
				"filters": [
					{
						"expression_class": "BOUND_COMPARISON",
						"type": "` + tt.opType + `",
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
							"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 42}
						}
					}
				],
				"column_binding_names_by_index": ["col"]
			}`)

			fp, err := Parse(json)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			enc := NewDuckDBEncoder(nil)
			sql := enc.EncodeFilters(fp)

			if sql != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, sql)
			}
		})
	}
}

func TestEncodeAndConjunction(t *testing.T) {
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
							"value": {"type": {"id": "VARCHAR", "type_info": null}, "is_null": false, "value": "active"}
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
							"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 18}
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

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "(status = 'active' AND age > 18)"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeOrConjunction(t *testing.T) {
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_CONJUNCTION",
				"type": "CONJUNCTION_OR",
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
							"return_type": {"id": "INTEGER", "type_info": null},
							"binding": {"table_index": 0, "column_index": 0},
							"depth": 0
						},
						"right": {
							"expression_class": "BOUND_CONSTANT",
							"type": "VALUE_CONSTANT",
							"alias": "",
							"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 1}
						}
					},
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
							"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 2}
						}
					}
				]
			}
		],
		"column_binding_names_by_index": ["x"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "(x = 1 OR x = 2)"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeFunction(t *testing.T) {
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
					"has_serialize": false,
					"is_operator": false
				},
				"right": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {"type": {"id": "VARCHAR", "type_info": null}, "is_null": false, "value": "john"}
				}
			}
		],
		"column_binding_names_by_index": ["name"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "lower(name) = 'john'"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeBetween(t *testing.T) {
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
					"return_type": {"id": "INTEGER", "type_info": null},
					"binding": {"table_index": 0, "column_index": 0},
					"depth": 0
				},
				"lower": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 100}
				},
				"upper": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 500}
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

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "price BETWEEN 100 AND 500"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeIsNull(t *testing.T) {
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

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "deleted_at IS NULL"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeIsNotNull(t *testing.T) {
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_OPERATOR",
				"type": "OPERATOR_IS_NOT_NULL",
				"alias": "",
				"return_type": {"id": "BOOLEAN", "type_info": null},
				"children": [
					{
						"expression_class": "BOUND_COLUMN_REF",
						"type": "BOUND_COLUMN_REF",
						"alias": "",
						"return_type": {"id": "VARCHAR", "type_info": null},
						"binding": {"table_index": 0, "column_index": 0},
						"depth": 0
					}
				]
			}
		],
		"column_binding_names_by_index": ["email"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "email IS NOT NULL"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeIn(t *testing.T) {
	json := []byte(`{
		"filters": [
			{
				"expression_class": "BOUND_COMPARISON",
				"type": "COMPARE_IN",
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
					"expression_class": "BOUND_FUNCTION",
					"type": "BOUND_FUNCTION",
					"alias": "",
					"return_type": {"id": "LIST", "type_info": null},
					"children": [
						{"expression_class": "BOUND_CONSTANT", "type": "VALUE_CONSTANT", "alias": "", "value": {"type": {"id": "VARCHAR"}, "is_null": false, "value": "active"}},
						{"expression_class": "BOUND_CONSTANT", "type": "VALUE_CONSTANT", "alias": "", "value": {"type": {"id": "VARCHAR"}, "is_null": false, "value": "pending"}}
					],
					"name": "list_value",
					"has_serialize": false,
					"is_operator": false
				}
			}
		],
		"column_binding_names_by_index": ["status"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "status IN ('active', 'pending')"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeCast(t *testing.T) {
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
					"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 10}
				}
			}
		],
		"column_binding_names_by_index": ["value"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "CAST(value AS INTEGER) > 10"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeStringEscaping(t *testing.T) {
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
					"return_type": {"id": "VARCHAR", "type_info": null},
					"binding": {"table_index": 0, "column_index": 0},
					"depth": 0
				},
				"right": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {"type": {"id": "VARCHAR", "type_info": null}, "is_null": false, "value": "O'Brien"}
				}
			}
		],
		"column_binding_names_by_index": ["name"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "name = 'O''Brien'"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeColumnMapping(t *testing.T) {
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
					"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 42}
				}
			}
		],
		"column_binding_names_by_index": ["user_id"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(&EncoderOptions{
		ColumnMapping: map[string]string{
			"user_id": "uid",
		},
	})
	sql := enc.EncodeFilters(fp)

	expected := "uid = 42"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeColumnExpression(t *testing.T) {
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
					"return_type": {"id": "VARCHAR", "type_info": null},
					"binding": {"table_index": 0, "column_index": 0},
					"depth": 0
				},
				"right": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {"type": {"id": "VARCHAR", "type_info": null}, "is_null": false, "value": "John Doe"}
				}
			}
		],
		"column_binding_names_by_index": ["full_name"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(&EncoderOptions{
		ColumnExpressions: map[string]string{
			"full_name": "CONCAT(first_name, ' ', last_name)",
		},
	})
	sql := enc.EncodeFilters(fp)

	expected := "CONCAT(first_name, ' ', last_name) = 'John Doe'"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeExpressionTakesPrecedence(t *testing.T) {
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
					"return_type": {"id": "VARCHAR", "type_info": null},
					"binding": {"table_index": 0, "column_index": 0},
					"depth": 0
				},
				"right": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {"type": {"id": "VARCHAR", "type_info": null}, "is_null": false, "value": "test"}
				}
			}
		],
		"column_binding_names_by_index": ["col"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(&EncoderOptions{
		ColumnMapping: map[string]string{
			"col": "column_name",
		},
		ColumnExpressions: map[string]string{
			"col": "UPPER(column_name)",
		},
	})
	sql := enc.EncodeFilters(fp)

	// Expression should take precedence
	if !strings.Contains(sql, "UPPER(column_name)") {
		t.Errorf("expected expression mapping to take precedence, got '%s'", sql)
	}
}

func TestEncodeUnsupportedInAnd(t *testing.T) {
	// When an AND has an unsupported child, skip that child but keep others
	fp := &FilterPushdown{
		ColumnBindings: []string{"a", "b"},
		Filters: []Expression{
			&ConjunctionExpression{
				BaseExpression: BaseExpression{
					ExprClass: ClassBoundConjunction,
					ExprType:  TypeConjunctionAnd,
				},
				Children: []Expression{
					&ComparisonExpression{
						BaseExpression: BaseExpression{
							ExprClass: ClassBoundComparison,
							ExprType:  TypeCompareEqual,
						},
						Left: &ColumnRefExpression{
							BaseExpression: BaseExpression{
								ExprClass: ClassBoundColumnRef,
								ExprType:  TypeBoundColumnRef,
							},
							Binding: ColumnBinding{ColumnIndex: 0},
						},
						Right: &ConstantExpression{
							BaseExpression: BaseExpression{
								ExprClass: ClassBoundConstant,
								ExprType:  TypeValueConstant,
							},
							Value: Value{Type: LogicalType{ID: TypeIDInteger}, Data: int64(1)},
						},
					},
					&UnsupportedExpression{
						BaseExpression: BaseExpression{
							ExprClass: ClassBoundWindow,
							ExprType:  TypeWindowRowNumber,
						},
					},
					&ComparisonExpression{
						BaseExpression: BaseExpression{
							ExprClass: ClassBoundComparison,
							ExprType:  TypeCompareEqual,
						},
						Left: &ColumnRefExpression{
							BaseExpression: BaseExpression{
								ExprClass: ClassBoundColumnRef,
								ExprType:  TypeBoundColumnRef,
							},
							Binding: ColumnBinding{ColumnIndex: 1},
						},
						Right: &ConstantExpression{
							BaseExpression: BaseExpression{
								ExprClass: ClassBoundConstant,
								ExprType:  TypeValueConstant,
							},
							Value: Value{Type: LogicalType{ID: TypeIDInteger}, Data: int64(2)},
						},
					},
				},
			},
		},
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	// Should encode supported children, skip unsupported
	expected := "(a = 1 AND b = 2)"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeUnsupportedInOr(t *testing.T) {
	// When an OR has an unsupported child, skip the entire OR
	fp := &FilterPushdown{
		ColumnBindings: []string{"a"},
		Filters: []Expression{
			&ConjunctionExpression{
				BaseExpression: BaseExpression{
					ExprClass: ClassBoundConjunction,
					ExprType:  TypeConjunctionOr,
				},
				Children: []Expression{
					&ComparisonExpression{
						BaseExpression: BaseExpression{
							ExprClass: ClassBoundComparison,
							ExprType:  TypeCompareEqual,
						},
						Left: &ColumnRefExpression{
							BaseExpression: BaseExpression{
								ExprClass: ClassBoundColumnRef,
								ExprType:  TypeBoundColumnRef,
							},
							Binding: ColumnBinding{ColumnIndex: 0},
						},
						Right: &ConstantExpression{
							BaseExpression: BaseExpression{
								ExprClass: ClassBoundConstant,
								ExprType:  TypeValueConstant,
							},
							Value: Value{Type: LogicalType{ID: TypeIDInteger}, Data: int64(1)},
						},
					},
					&UnsupportedExpression{
						BaseExpression: BaseExpression{
							ExprClass: ClassBoundWindow,
							ExprType:  TypeWindowRowNumber,
						},
					},
				},
			},
		},
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	// Entire OR should be skipped
	if sql != "" {
		t.Errorf("expected empty string for OR with unsupported child, got '%s'", sql)
	}
}

func TestEncodeAllUnsupported(t *testing.T) {
	fp := &FilterPushdown{
		ColumnBindings: []string{},
		Filters: []Expression{
			&UnsupportedExpression{
				BaseExpression: BaseExpression{
					ExprClass: ClassBoundWindow,
					ExprType:  TypeWindowRowNumber,
				},
			},
		},
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	if sql != "" {
		t.Errorf("expected empty string for all unsupported, got '%s'", sql)
	}
}

func TestEncodeNullValue(t *testing.T) {
	fp := &FilterPushdown{
		ColumnBindings: []string{"a"},
		Filters: []Expression{
			&ComparisonExpression{
				BaseExpression: BaseExpression{
					ExprClass: ClassBoundComparison,
					ExprType:  TypeCompareEqual,
				},
				Left: &ColumnRefExpression{
					BaseExpression: BaseExpression{
						ExprClass: ClassBoundColumnRef,
						ExprType:  TypeBoundColumnRef,
					},
					Binding: ColumnBinding{ColumnIndex: 0},
				},
				Right: &ConstantExpression{
					BaseExpression: BaseExpression{
						ExprClass: ClassBoundConstant,
						ExprType:  TypeValueConstant,
					},
					Value: Value{Type: LogicalType{ID: TypeIDInteger}, IsNull: true},
				},
			},
		},
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "a = NULL"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeBooleanValue(t *testing.T) {
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
					"return_type": {"id": "BOOLEAN", "type_info": null},
					"binding": {"table_index": 0, "column_index": 0},
					"depth": 0
				},
				"right": {
					"expression_class": "BOUND_CONSTANT",
					"type": "VALUE_CONSTANT",
					"alias": "",
					"value": {"type": {"id": "BOOLEAN", "type_info": null}, "is_null": false, "value": true}
				}
			}
		],
		"column_binding_names_by_index": ["is_active"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "is_active = TRUE"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeMultipleFilters(t *testing.T) {
	// Multiple top-level filters are implicitly AND'ed
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
					"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 1}
				}
			},
			{
				"expression_class": "BOUND_COMPARISON",
				"type": "COMPARE_EQUAL",
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
					"value": {"type": {"id": "INTEGER", "type_info": null}, "is_null": false, "value": 2}
				}
			}
		],
		"column_binding_names_by_index": ["a", "b"]
	}`)

	fp, err := Parse(json)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "(a = 1) AND (b = 2)"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}

func TestEncodeCase(t *testing.T) {
	fp := &FilterPushdown{
		ColumnBindings: []string{"x"},
		Filters: []Expression{
			&ComparisonExpression{
				BaseExpression: BaseExpression{
					ExprClass: ClassBoundComparison,
					ExprType:  TypeCompareEqual,
				},
				Left: &CaseExpression{
					BaseExpression: BaseExpression{
						ExprClass: ClassBoundCase,
						ExprType:  TypeCaseExpr,
					},
					CaseChecks: []CaseCheck{
						{
							WhenExpr: &ComparisonExpression{
								BaseExpression: BaseExpression{
									ExprClass: ClassBoundComparison,
									ExprType:  TypeCompareGreaterThan,
								},
								Left: &ColumnRefExpression{
									BaseExpression: BaseExpression{
										ExprClass: ClassBoundColumnRef,
										ExprType:  TypeBoundColumnRef,
									},
									Binding: ColumnBinding{ColumnIndex: 0},
								},
								Right: &ConstantExpression{
									BaseExpression: BaseExpression{
										ExprClass: ClassBoundConstant,
										ExprType:  TypeValueConstant,
									},
									Value: Value{Type: LogicalType{ID: TypeIDInteger}, Data: int64(0)},
								},
							},
							ThenExpr: &ConstantExpression{
								BaseExpression: BaseExpression{
									ExprClass: ClassBoundConstant,
									ExprType:  TypeValueConstant,
								},
								Value: Value{Type: LogicalType{ID: TypeIDVarchar}, Data: "positive"},
							},
						},
					},
					ElseExpr: &ConstantExpression{
						BaseExpression: BaseExpression{
							ExprClass: ClassBoundConstant,
							ExprType:  TypeValueConstant,
						},
						Value: Value{Type: LogicalType{ID: TypeIDVarchar}, Data: "non-positive"},
					},
				},
				Right: &ConstantExpression{
					BaseExpression: BaseExpression{
						ExprClass: ClassBoundConstant,
						ExprType:  TypeValueConstant,
					},
					Value: Value{Type: LogicalType{ID: TypeIDVarchar}, Data: "positive"},
				},
			},
		},
	}

	enc := NewDuckDBEncoder(nil)
	sql := enc.EncodeFilters(fp)

	expected := "CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END = 'positive'"
	if sql != expected {
		t.Errorf("expected '%s', got '%s'", expected, sql)
	}
}
