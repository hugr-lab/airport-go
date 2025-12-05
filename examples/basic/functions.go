package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go/catalog"
)

// UppercaseFunc is a simple scalar function that converts strings to uppercase.
// It demonstrates the ScalarFunction interface implementation.
type UppercaseFunc struct{}

// Name returns the function name as it appears in queries.
func (f *UppercaseFunc) Name() string {
	return "UPPERCASE"
}

// Comment returns documentation for this function.
func (f *UppercaseFunc) Comment() string {
	return "Converts all characters in a string to uppercase"
}

// Signature returns the function signature.
// Input: string column, Output: string column
func (f *UppercaseFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.BinaryTypes.String},
		ReturnType: arrow.BinaryTypes.String,
	}
}

// Execute applies the uppercase transformation to an Arrow record.
// This processes entire batches at once (vectorized execution).
//
//nolint:unparam
func (f *UppercaseFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.RecordBatch, error) {
	if input.NumCols() != 1 {
		return nil, fmt.Errorf("UPPERCASE expects exactly 1 column, got %d", input.NumCols())
	}

	// Get input column
	inputCol := input.Column(0)
	inputArray, ok := inputCol.(*array.String)
	if !ok {
		return nil, fmt.Errorf("UPPERCASE expects string column, got %T", inputCol)
	}

	// Build output array
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()

	// Process each value
	for i := 0; i < inputArray.Len(); i++ {
		if inputArray.IsNull(i) {
			builder.AppendNull()
		} else {
			value := inputArray.Value(i)
			builder.Append(strings.ToUpper(value))
		}
	}

	outputArray := builder.NewStringArray()
	defer outputArray.Release()

	// Create output schema
	outputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.BinaryTypes.String},
	}, nil)

	// Build output record with the output array
	return array.NewRecordBatch(outputSchema, []arrow.Array{outputArray}, int64(outputArray.Len())), nil
}

// LengthFunc returns the length of strings.
type LengthFunc struct{}

func (f *LengthFunc) Name() string {
	return "LENGTH"
}

func (f *LengthFunc) Comment() string {
	return "Returns the length of a string in characters"
}

func (f *LengthFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.BinaryTypes.String},
		ReturnType: arrow.PrimitiveTypes.Int64,
	}
}

//nolint:unparam
func (f *LengthFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.RecordBatch, error) {
	if input.NumCols() != 1 {
		return nil, fmt.Errorf("LENGTH expects exactly 1 column, got %d", input.NumCols())
	}

	inputCol := input.Column(0)
	inputArray, ok := inputCol.(*array.String)
	if !ok {
		return nil, fmt.Errorf("LENGTH expects string column, got %T", inputCol)
	}

	// Build output array
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < inputArray.Len(); i++ {
		if inputArray.IsNull(i) {
			builder.AppendNull()
		} else {
			value := inputArray.Value(i)
			builder.Append(int64(len(value)))
		}
	}

	outputArray := builder.NewInt64Array()
	defer outputArray.Release()

	// Create output record
	outputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "length", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	return array.NewRecordBatch(outputSchema, []arrow.Array{outputArray}, int64(outputArray.Len())), nil
}
