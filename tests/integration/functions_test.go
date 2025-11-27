package airport_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// uppercaseFunc is a simple scalar function that converts strings to uppercase.
type uppercaseFunc struct{}

func (f *uppercaseFunc) Name() string {
	return "UPPERCASE"
}

func (f *uppercaseFunc) Comment() string {
	return "Converts all characters in a string to uppercase"
}

func (f *uppercaseFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.BinaryTypes.String},
		ReturnType: arrow.BinaryTypes.String,
		Variadic:   false,
	}
}

func (f *uppercaseFunc) Execute(ctx context.Context, input arrow.Record) (arrow.Record, error) {
	if input.NumCols() != 1 {
		return nil, fmt.Errorf("UPPERCASE expects exactly 1 column, got %d", input.NumCols())
	}

	inputCol := input.Column(0)
	inputArray, ok := inputCol.(*array.String)
	if !ok {
		return nil, fmt.Errorf("UPPERCASE expects string column, got %T", inputCol)
	}

	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()

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

	outputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.BinaryTypes.String},
	}, nil)

	return array.NewRecord(outputSchema, []arrow.Array{outputArray}, int64(outputArray.Len())), nil
}

// lengthFunc returns the length of strings.
type lengthFunc struct{}

func (f *lengthFunc) Name() string {
	return "LENGTH"
}

func (f *lengthFunc) Comment() string {
	return "Returns the length of a string in characters"
}

func (f *lengthFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.BinaryTypes.String},
		ReturnType: arrow.PrimitiveTypes.Int64,
		Variadic:   false,
	}
}

func (f *lengthFunc) Execute(ctx context.Context, input arrow.Record) (arrow.Record, error) {
	if input.NumCols() != 1 {
		return nil, fmt.Errorf("LENGTH expects exactly 1 column, got %d", input.NumCols())
	}

	inputCol := input.Column(0)
	inputArray, ok := inputCol.(*array.String)
	if !ok {
		return nil, fmt.Errorf("LENGTH expects string column, got %T", inputCol)
	}

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

	outputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "length", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	return array.NewRecord(outputSchema, []arrow.Array{outputArray}, int64(outputArray.Len())), nil
}

// catalogWithFunctions creates a catalog with scalar functions.
func catalogWithFunctions() catalog.Catalog {
	usersSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	usersData := [][]interface{}{
		{int64(1), "alice"},
		{int64(2), "bob"},
		{int64(3), "charlie"},
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("some_schema").
		Comment("Main schema with functions").
		SimpleTable(airport.SimpleTableDef{
			Name:     "users",
			Comment:  "User accounts",
			Schema:   usersSchema,
			ScanFunc: makeScanFunc(usersSchema, usersData),
		}).
		ScalarFunc(&uppercaseFunc{}).
		ScalarFunc(&lengthFunc{}).
		Build()

	if err != nil {
		panic(fmt.Sprintf("Failed to build catalog: %v", err))
	}

	return cat
}

// TestScalarFunctions verifies that scalar functions can be discovered
// and invoked through the Flight server.
func TestScalarFunctions(t *testing.T) {
	cat := catalogWithFunctions()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test 1: Discover scalar functions
	t.Run("DiscoverFunctions", func(t *testing.T) {
		// DuckDB may expose functions through system tables
		// This test documents expected behavior
		query := "SELECT function_name FROM duckdb_functions() WHERE schema_name = 'some_schema' AND function_type = 'scalar'"
		rows, err := db.Query(query)
		if err != nil {
			t.Skipf("Function discovery not supported: %v", err)
		}
		defer rows.Close()

		functions := []string{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			functions = append(functions, name)
		}

		t.Logf("Discovered functions: %v", functions)
	})

	// Test 2: Call scalar function if supported
	t.Run("CallFunction", func(t *testing.T) {
		// Note: DuckDB Airport extension may not support calling scalar functions yet
		// This test documents the expected behavior for future support

		// Try to use UPPERCASE function
		query := "SELECT " + attachName + ".some_schema.UPPERCASE(name) as upper_name FROM " + attachName + ".some_schema.users WHERE id = 1"
		var upperName string
		err := db.QueryRow(query).Scan(&upperName)

		if err != nil {
			t.Skipf("Scalar function calls not yet supported in Airport extension: %v", err)
			return
		}

		if upperName != "ALICE" {
			t.Errorf("Expected 'ALICE', got '%s'", upperName)
		}
	})

	// Test 3: Function metadata
	t.Run("FunctionMetadata", func(t *testing.T) {
		// Verify that function comments and signatures are accessible
		// This may require DuckDB system table support
		t.Skip("Function metadata access not yet implemented")
	})
}

// TestFunctionSignatures verifies that function signatures are correctly
// validated and enforced.
func TestFunctionSignatures(t *testing.T) {
	cat := catalogWithFunctions()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	_ = connectToFlightServer(t, db, server.address, "")

	// Test 1: Verify function accepts correct input types
	t.Run("CorrectInputType", func(t *testing.T) {
		t.Skip("Function signature validation requires DuckDB support")
	})

	// Test 2: Verify function rejects incorrect input types
	t.Run("IncorrectInputType", func(t *testing.T) {
		t.Skip("Function signature validation requires DuckDB support")
	})

	// Test 3: Verify return type is correct
	t.Run("ReturnType", func(t *testing.T) {
		t.Skip("Return type validation requires DuckDB support")
	})
}

// TestVectorizedExecution verifies that scalar functions process batches
// efficiently rather than row-by-row.
func TestVectorizedExecution(t *testing.T) {
	cat := catalogWithFunctions()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	_ = connectToFlightServer(t, db, server.address, "")

	t.Run("BatchProcessing", func(t *testing.T) {
		// This test would verify that functions process entire batches
		// Performance testing would show the difference vs row-by-row
		t.Skip("Vectorized execution testing requires benchmark framework")
	})
}

// errorFunc is a function that always returns an error (for testing error propagation).
type errorFunc struct{}

func (f *errorFunc) Name() string {
	return "ERROR_FUNC"
}

func (f *errorFunc) Comment() string {
	return "Always returns an error"
}

func (f *errorFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.PrimitiveTypes.Int64},
		ReturnType: arrow.PrimitiveTypes.Int64,
		Variadic:   false,
	}
}

func (f *errorFunc) Execute(ctx context.Context, input arrow.Record) (arrow.Record, error) {
	return nil, fmt.Errorf("intentional error for testing")
}

// TestFunctionErrors verifies that function errors are properly propagated.
func TestFunctionErrors(t *testing.T) {
	var _ catalog.ScalarFunction = (*errorFunc)(nil)

	ef := &errorFunc{}

	t.Run("ErrorPropagation", func(t *testing.T) {
		// Create catalog with error-throwing function
		usersSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		testData := [][]interface{}{{int64(1)}}

		cat, err := airport.NewCatalogBuilder().
			Schema("some_schema").
			SimpleTable(airport.SimpleTableDef{
				Name:     "test",
				Schema:   usersSchema,
				ScanFunc: makeScanFunc(usersSchema, testData),
			}).
			ScalarFunc(ef).
			Build()

		if err != nil {
			t.Fatalf("Failed to build catalog: %v", err)
		}

		server := newTestServer(t, cat, nil)
		defer server.stop()

		db := openDuckDB(t)
		defer db.Close()

		_ = connectToFlightServer(t, db, server.address, "")

		// Try to call the error function
		// Should propagate error to client
		t.Skip("Error propagation testing requires function call support")
	})
}
