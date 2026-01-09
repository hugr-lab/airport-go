package flight

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestExtractScalarValue_Integers(t *testing.T) {
	alloc := memory.NewGoAllocator()

	tests := []struct {
		name     string
		buildArr func() arrow.Array
		expected any
	}{
		{
			name: "Int8",
			buildArr: func() arrow.Array {
				b := array.NewInt8Builder(alloc)
				defer b.Release()
				b.Append(42)
				return b.NewArray()
			},
			expected: int64(42),
		},
		{
			name: "Int16",
			buildArr: func() arrow.Array {
				b := array.NewInt16Builder(alloc)
				defer b.Release()
				b.Append(1000)
				return b.NewArray()
			},
			expected: int64(1000),
		},
		{
			name: "Int32",
			buildArr: func() arrow.Array {
				b := array.NewInt32Builder(alloc)
				defer b.Release()
				b.Append(100000)
				return b.NewArray()
			},
			expected: int64(100000),
		},
		{
			name: "Int64",
			buildArr: func() arrow.Array {
				b := array.NewInt64Builder(alloc)
				defer b.Release()
				b.Append(9223372036854775807)
				return b.NewArray()
			},
			expected: int64(9223372036854775807),
		},
		{
			name: "Uint8",
			buildArr: func() arrow.Array {
				b := array.NewUint8Builder(alloc)
				defer b.Release()
				b.Append(255)
				return b.NewArray()
			},
			expected: int64(255),
		},
		{
			name: "Uint16",
			buildArr: func() arrow.Array {
				b := array.NewUint16Builder(alloc)
				defer b.Release()
				b.Append(65535)
				return b.NewArray()
			},
			expected: int64(65535),
		},
		{
			name: "Uint32",
			buildArr: func() arrow.Array {
				b := array.NewUint32Builder(alloc)
				defer b.Release()
				b.Append(4294967295)
				return b.NewArray()
			},
			expected: int64(4294967295),
		},
		{
			name: "Uint64",
			buildArr: func() arrow.Array {
				b := array.NewUint64Builder(alloc)
				defer b.Release()
				b.Append(1844674407370955161)
				return b.NewArray()
			},
			expected: int64(1844674407370955161),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr := tt.buildArr()
			defer arr.Release()

			result := extractScalarValue(arr, 0)
			if result != tt.expected {
				t.Errorf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

func TestExtractScalarValue_Floats(t *testing.T) {
	alloc := memory.NewGoAllocator()

	t.Run("Float32", func(t *testing.T) {
		b := array.NewFloat32Builder(alloc)
		defer b.Release()
		b.Append(3.14)
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if v, ok := result.(float64); !ok || v < 3.13 || v > 3.15 {
			t.Errorf("expected ~3.14, got %v", result)
		}
	})

	t.Run("Float64", func(t *testing.T) {
		b := array.NewFloat64Builder(alloc)
		defer b.Release()
		b.Append(3.141592653589793)
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if result != 3.141592653589793 {
			t.Errorf("expected 3.141592653589793, got %v", result)
		}
	})
}

func TestExtractScalarValue_StringAndBinary(t *testing.T) {
	alloc := memory.NewGoAllocator()

	t.Run("String", func(t *testing.T) {
		b := array.NewStringBuilder(alloc)
		defer b.Release()
		b.Append("hello world")
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if result != "hello world" {
			t.Errorf("expected 'hello world', got %v", result)
		}
	})

	t.Run("Binary", func(t *testing.T) {
		b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		defer b.Release()
		b.Append([]byte{0x01, 0x02, 0x03})
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if v, ok := result.([]byte); !ok || len(v) != 3 || v[0] != 0x01 {
			t.Errorf("expected []byte{0x01, 0x02, 0x03}, got %v", result)
		}
	})
}

func TestExtractScalarValue_Boolean(t *testing.T) {
	alloc := memory.NewGoAllocator()

	t.Run("True", func(t *testing.T) {
		b := array.NewBooleanBuilder(alloc)
		defer b.Release()
		b.Append(true)
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})

	t.Run("False", func(t *testing.T) {
		b := array.NewBooleanBuilder(alloc)
		defer b.Release()
		b.Append(false)
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if result != false {
			t.Errorf("expected false, got %v", result)
		}
	})
}

func TestExtractScalarValue_Null(t *testing.T) {
	alloc := memory.NewGoAllocator()

	b := array.NewInt64Builder(alloc)
	defer b.Release()
	b.AppendNull()
	arr := b.NewArray()
	defer arr.Release()

	result := extractScalarValue(arr, 0)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestExtractScalarValue_Date(t *testing.T) {
	alloc := memory.NewGoAllocator()

	t.Run("Date32", func(t *testing.T) {
		b := array.NewDate32Builder(alloc)
		defer b.Release()
		// 19716 days since epoch = 2023-12-25
		b.Append(arrow.Date32(19716))
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if v, ok := result.(time.Time); !ok {
			t.Errorf("expected time.Time, got %T", result)
		} else if v.Year() != 2023 || v.Month() != 12 || v.Day() != 25 {
			t.Errorf("expected 2023-12-25, got %v", v)
		}
	})

	t.Run("Date64", func(t *testing.T) {
		b := array.NewDate64Builder(alloc)
		defer b.Release()
		// 1703462400000 ms = 2023-12-25 00:00:00 UTC
		b.Append(arrow.Date64(1703462400000))
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if v, ok := result.(time.Time); !ok {
			t.Errorf("expected time.Time, got %T", result)
		} else if v.Year() != 2023 || v.Month() != 12 || v.Day() != 25 {
			t.Errorf("expected 2023-12-25, got %v", v)
		}
	})
}

func TestExtractScalarValue_Timestamp(t *testing.T) {
	alloc := memory.NewGoAllocator()

	t.Run("TimestampSecond", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Second}
		b := array.NewTimestampBuilder(alloc, dt)
		defer b.Release()
		// 1703462400 seconds = 2023-12-25 00:00:00 UTC
		b.Append(arrow.Timestamp(1703462400))
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if v, ok := result.(time.Time); !ok {
			t.Errorf("expected time.Time, got %T", result)
		} else if v.Year() != 2023 || v.Month() != 12 || v.Day() != 25 {
			t.Errorf("expected 2023-12-25, got %v", v)
		}
	})

	t.Run("TimestampMillisecond", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Millisecond}
		b := array.NewTimestampBuilder(alloc, dt)
		defer b.Release()
		b.Append(arrow.Timestamp(1703462400000))
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if v, ok := result.(time.Time); !ok {
			t.Errorf("expected time.Time, got %T", result)
		} else if v.Year() != 2023 || v.Month() != 12 || v.Day() != 25 {
			t.Errorf("expected 2023-12-25, got %v", v)
		}
	})
}

func TestExtractScalarValue_Time(t *testing.T) {
	alloc := memory.NewGoAllocator()

	t.Run("Time32Second", func(t *testing.T) {
		dt := &arrow.Time32Type{Unit: arrow.Second}
		b := array.NewTime32Builder(alloc, dt)
		defer b.Release()
		// 3661 seconds = 01:01:01
		b.Append(arrow.Time32(3661))
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if v, ok := result.(int32); !ok || v != 3661 {
			t.Errorf("expected int32(3661), got %v (%T)", result, result)
		}
	})

	t.Run("Time64Microsecond", func(t *testing.T) {
		dt := &arrow.Time64Type{Unit: arrow.Microsecond}
		b := array.NewTime64Builder(alloc, dt)
		defer b.Release()
		// 3661000000 microseconds = 01:01:01
		b.Append(arrow.Time64(3661000000))
		arr := b.NewArray()
		defer arr.Release()

		result := extractScalarValue(arr, 0)
		if v, ok := result.(int64); !ok || v != 3661000000 {
			t.Errorf("expected int64(3661000000), got %v (%T)", result, result)
		}
	})
}

func TestExtractScalarValue_Decimal128(t *testing.T) {
	alloc := memory.NewGoAllocator()

	dt := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	b := array.NewDecimal128Builder(alloc, dt)
	defer b.Release()
	// 12345 with scale 2 = 123.45
	b.Append(decimal128.FromI64(12345))
	arr := b.NewArray()
	defer arr.Release()

	result := extractScalarValue(arr, 0)
	if v, ok := result.(string); !ok || v != "123.45" {
		t.Errorf("expected '123.45', got %v (%T)", result, result)
	}
}

func TestExtractScalarValue_List(t *testing.T) {
	alloc := memory.NewGoAllocator()

	lb := array.NewListBuilder(alloc, arrow.PrimitiveTypes.Int64)
	defer lb.Release()

	vb := lb.ValueBuilder().(*array.Int64Builder)
	lb.Append(true)
	vb.Append(1)
	vb.Append(2)
	vb.Append(3)

	arr := lb.NewArray()
	defer arr.Release()

	result := extractScalarValue(arr, 0)
	if v, ok := result.([]any); !ok {
		t.Errorf("expected []any, got %T", result)
	} else if len(v) != 3 {
		t.Errorf("expected 3 elements, got %d", len(v))
	} else if v[0] != int64(1) || v[1] != int64(2) || v[2] != int64(3) {
		t.Errorf("expected [1, 2, 3], got %v", v)
	}
}

func TestExtractScalarValue_Struct(t *testing.T) {
	alloc := memory.NewGoAllocator()

	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
	}
	structType := arrow.StructOf(fields...)

	sb := array.NewStructBuilder(alloc, structType)
	defer sb.Release()

	nameBuilder := sb.FieldBuilder(0).(*array.StringBuilder)
	ageBuilder := sb.FieldBuilder(1).(*array.Int64Builder)

	sb.Append(true)
	nameBuilder.Append("Alice")
	ageBuilder.Append(30)

	arr := sb.NewArray()
	defer arr.Release()

	result := extractScalarValue(arr, 0)
	v, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if v["name"] != "Alice" {
		t.Errorf("expected name='Alice', got %v", v["name"])
	}
	if v["age"] != int64(30) {
		t.Errorf("expected age=30, got %v", v["age"])
	}
}

func TestExtractScalarValue_Map(t *testing.T) {
	alloc := memory.NewGoAllocator()

	mb := array.NewMapBuilder(alloc, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64, false)
	defer mb.Release()

	kb := mb.KeyBuilder().(*array.StringBuilder)
	ib := mb.ItemBuilder().(*array.Int64Builder)

	mb.Append(true)
	kb.Append("a")
	ib.Append(1)
	kb.Append("b")
	ib.Append(2)

	arr := mb.NewArray()
	defer arr.Release()

	result := extractScalarValue(arr, 0)
	v, ok := result.(map[any]any)
	if !ok {
		t.Fatalf("expected map[any]any, got %T", result)
	}
	if len(v) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(v))
	}
	if v["a"] != int64(1) {
		t.Errorf("expected a=1, got %v", v["a"])
	}
	if v["b"] != int64(2) {
		t.Errorf("expected b=2, got %v", v["b"])
	}
}

func TestExtractScalarValue_MultipleIndices(t *testing.T) {
	alloc := memory.NewGoAllocator()

	b := array.NewStringBuilder(alloc)
	defer b.Release()
	b.Append("first")
	b.Append("second")
	b.Append("third")
	arr := b.NewArray()
	defer arr.Release()

	if extractScalarValue(arr, 0) != "first" {
		t.Error("index 0 should be 'first'")
	}
	if extractScalarValue(arr, 1) != "second" {
		t.Error("index 1 should be 'second'")
	}
	if extractScalarValue(arr, 2) != "third" {
		t.Error("index 2 should be 'third'")
	}
}

func TestExtractScalarValue_NestedList(t *testing.T) {
	alloc := memory.NewGoAllocator()

	// List of lists of int64
	innerListType := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	lb := array.NewListBuilder(alloc, innerListType)
	defer lb.Release()

	innerLb := lb.ValueBuilder().(*array.ListBuilder)
	innerVb := innerLb.ValueBuilder().(*array.Int64Builder)

	lb.Append(true)
	innerLb.Append(true)
	innerVb.Append(1)
	innerVb.Append(2)
	innerLb.Append(true)
	innerVb.Append(3)
	innerVb.Append(4)

	arr := lb.NewArray()
	defer arr.Release()

	result := extractScalarValue(arr, 0)
	v, ok := result.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result)
	}
	if len(v) != 2 {
		t.Fatalf("expected 2 inner lists, got %d", len(v))
	}
	inner0, ok := v[0].([]any)
	if !ok || len(inner0) != 2 {
		t.Errorf("expected inner list with 2 elements, got %v", v[0])
	}
	inner1, ok := v[1].([]any)
	if !ok || len(inner1) != 2 {
		t.Errorf("expected inner list with 2 elements, got %v", v[1])
	}
}
