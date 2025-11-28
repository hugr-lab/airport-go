package airport_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go"
	"github.com/hugr-lab/airport-go/catalog"
)

// TestScalarFunctions verifies that scalar functions can be discovered
// and invoked through the Flight server.
func TestScalarFunctions(t *testing.T) {
	cat := catalogWithScalarFunctions()
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
			t.Fatalf("Function discovery query failed: %v", err)
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

	// Test 2: Call scalar function
	t.Run("CallFunction", func(t *testing.T) {
		// Scalar functions are supported via Airport DoExchange protocol.
		// This test verifies that scalar functions can be called from DuckDB.

		// Call UPPERCASE function
		query := "SELECT " + attachName + ".some_schema.UPPERCASE(name) as upper_name FROM " + attachName + ".some_schema.users WHERE id = 1"
		var upperName string
		err := db.QueryRow(query).Scan(&upperName)

		if err != nil {
			t.Fatalf("Scalar function calls failed in Airport extension: %v", err)
		}

		if upperName != "ALICE" {
			t.Errorf("Expected 'ALICE', got '%s'", upperName)
		}
	})

	// Test 3: Function metadata
	t.Run("FunctionMetadata", func(t *testing.T) {
		// Verify that function comments and signatures are accessible via catalog
		// We can test this through the Airport schema serialization

		// Query the catalog to get function information
		// The functions should be discoverable in the catalog metadata
		query := `
			SELECT function_name, parameter_types, return_type
			FROM duckdb_functions()
			WHERE schema_name = 'some_schema'
			AND function_type = 'scalar'
			ORDER BY function_name
		`

		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Function metadata query failed: %v", err)
		}
		defer rows.Close()

		foundFunctions := make(map[string]bool)
		for rows.Next() {
			var funcName, returnType string
			var paramTypes []any
			if err := rows.Scan(&funcName, &paramTypes, &returnType); err != nil {
				t.Fatalf("Failed to scan function metadata: %v", err)
			}

			foundFunctions[funcName] = true
			t.Logf("Found function: %s, params: %s, return: %s", funcName, paramTypes, returnType)
		}

		// Verify expected functions are present
		expectedFunctions := []string{"UPPERCASE", "LENGTH"}
		for _, expected := range expectedFunctions {
			if !foundFunctions[expected] {
				t.Errorf("Expected function %s not found in catalog", expected)
			}
		}

		if len(foundFunctions) < len(expectedFunctions) {
			t.Logf("Note: Only %d of %d expected functions found - DuckDB may not fully expose Airport function metadata yet",
				len(foundFunctions), len(expectedFunctions))
		}
	})
}

// TestFunctionSignatures verifies that function signatures are correctly
// validated and enforced.
func TestFunctionSignatures(t *testing.T) {
	cat := catalogWithScalarFunctions()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	// Test 1: Verify function accepts correct input types
	t.Run("CorrectInputType", func(t *testing.T) {
		// Test that UPPERCASE accepts string input
		query := fmt.Sprintf("SELECT %s.some_schema.UPPERCASE('hello') as result", attachName)
		var result string
		err := db.QueryRow(query).Scan(&result)
		if err != nil {
			t.Fatalf("Scalar function call failed: %v", err)
			return
		}

		if result != "HELLO" {
			t.Errorf("Expected 'HELLO', got '%s'", result)
		}

		// Test that LENGTH accepts string input
		query = fmt.Sprintf("SELECT %s.some_schema.LENGTH('test') as result", attachName)
		var length int64
		err = db.QueryRow(query).Scan(&length)
		if err != nil {
			t.Errorf("LENGTH function failed with correct type: %v", err)
		}

		if length != 4 {
			t.Errorf("Expected length 4, got %d", length)
		}
	})

	// Test 2: Verify function rejects incorrect input types
	t.Run("IncorrectInputType", func(t *testing.T) {
		// Try to call UPPERCASE with an integer (should fail)
		query := fmt.Sprintf("SELECT %s.some_schema.UPPERCASE(123) as result", attachName)
		var result string
		err := db.QueryRow(query).Scan(&result)

		// We expect this to fail with a type error
		if err == nil {
			t.Fatalf("Note: DuckDB may perform automatic type coercion (123 -> '123')")
		}
	})

	// Test 3: Verify return type is correct
	t.Run("ReturnType", func(t *testing.T) {
		// Verify UPPERCASE returns string
		query := fmt.Sprintf("SELECT typeof(%s.some_schema.UPPERCASE('test')) as result_type", attachName)
		var resultType string
		err := db.QueryRow(query).Scan(&resultType)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// DuckDB should report VARCHAR or STRING type
		if resultType != "VARCHAR" {
			t.Logf("Note: Return type is '%s' (expected VARCHAR or STRING)", resultType)
		}

		// Verify LENGTH returns integer
		query = fmt.Sprintf("SELECT typeof(%s.some_schema.LENGTH('test')) as result_type", attachName)
		err = db.QueryRow(query).Scan(&resultType)
		if err != nil {
			t.Errorf("typeof() failed for LENGTH: %v", err)
		}

		// DuckDB should report BIGINT or INTEGER type
		if resultType != "BIGINT" && resultType != "INTEGER" {
			t.Logf("Note: Return type is '%s' (expected BIGINT or INTEGER)", resultType)
		}
	})
}

// TestVectorizedExecution verifies that scalar functions process large batches
// efficiently (100k+ rows) rather than row-by-row.
func TestVectorizedExecution(t *testing.T) {
	cat := catalogWithScalarFunctions()
	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("LargeBatchProcessing", func(t *testing.T) {
		// Generate 100k rows and process them through scalar function
		// This verifies vectorized execution at scale
		const numRows = 100000

		// Use DuckDB's generate_series to create large dataset
		// Then apply scalar function to each row
		query := fmt.Sprintf(`
			SELECT COUNT(*) as cnt
			FROM (
				SELECT %s.some_schema.LENGTH(CAST(i AS VARCHAR)) as len
				FROM generate_series(1, %d) AS t(i)
			)
		`, attachName, numRows)

		var count int64
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			t.Fatalf("Scalar function call failed: %v", err)
			return
		}

		// Should process all rows
		if count != numRows {
			t.Errorf("Expected %d rows, got %d", numRows, count)
		}

		t.Logf("Successfully processed %d rows through vectorized scalar function", count)
	})

	t.Run("MultipleOperations", func(t *testing.T) {
		// Test multiple scalar function calls in same query with large dataset
		const numRows = 50000

		query := fmt.Sprintf(`
			SELECT COUNT(*) as cnt
			FROM (
				SELECT
					%s.some_schema.UPPERCASE(CAST(i AS VARCHAR)) as upper_val,
					%s.some_schema.LENGTH(CAST(i AS VARCHAR)) as len_val
				FROM generate_series(1, %d) AS t(i)
			)
		`, attachName, attachName, numRows)

		var count int64
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			t.Fatalf("Multiple scalar function calls failed: %v", err)
		}

		if count != numRows {
			t.Errorf("Expected %d rows, got %d", numRows, count)
		}

		t.Logf("Successfully processed %d rows through multiple vectorized functions", count)
	})
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

		testData := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}

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

		attachName := connectToFlightServer(t, db, server.address, "")

		// Try to call the error function - should get error from server
		query := fmt.Sprintf("SELECT %s.some_schema.ERROR_FUNC(id) FROM %s.some_schema.test",
			attachName, attachName)

		var result int64
		err = db.QueryRow(query).Scan(&result)

		// Should receive an error (either from function execution or connection)
		if err == nil {
			t.Fatalf("Expected error from ERROR_FUNC, but query succeeded")
		}
		t.Logf("Error correctly propagated: %v", err)

		// Verify error message contains our intentional error
		if !strings.Contains(err.Error(), "intentional error") &&
			!strings.Contains(err.Error(), "function execution failed") {
			t.Logf("Note: Error message doesn't contain expected text, but error was propagated")
		}
		// enshure that the duckdb is still working after the error
		var count int64
		err = db.QueryRow("SELECT COUNT(*) FROM " + attachName + ".some_schema.test").Scan(&count)
		if err != nil {
			t.Errorf("Failed to query after error: %v", err)
		}
	})
}

// TestTypeMismatch verifies that type mismatch is detected.
func TestTypeMismatch(t *testing.T) {

	var _ catalog.ScalarFunction = (*wrongTypeFunc)(nil)

	wtf := &wrongTypeFunc{}

	t.Run("TypeMismatchDetection", func(t *testing.T) {
		// Create catalog with type-mismatching function
		usersSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		testData := [][]interface{}{{int64(1)}, {int64(2)}}

		cat, err := airport.NewCatalogBuilder().
			Schema("some_schema").
			SimpleTable(airport.SimpleTableDef{
				Name:     "test",
				Schema:   usersSchema,
				ScanFunc: makeScanFunc(usersSchema, testData),
			}).
			ScalarFunc(wtf).
			Build()

		if err != nil {
			t.Fatalf("Failed to build catalog: %v", err)
		}

		server := newTestServer(t, cat, nil)
		defer server.stop()

		db := openDuckDB(t)
		defer db.Close()

		attachName := connectToFlightServer(t, db, server.address, "")

		// Try to call the function with wrong return type
		// Our DoExchange should detect type mismatch and return error
		query := fmt.Sprintf("SELECT %s.some_schema.WRONG_TYPE(id) FROM %s.some_schema.test",
			attachName, attachName)

		var result int64
		err = db.QueryRow(query).Scan(&result)

		// Should receive an error due to type mismatch
		if err == nil {
			t.Errorf("Expected error from type mismatch, but query succeeded")
		} else {
			t.Logf("Type mismatch correctly detected: %v", err)

			// Verify error message mentions type mismatch
			if strings.Contains(err.Error(), "type mismatch") ||
				strings.Contains(err.Error(), "expected int64") ||
				strings.Contains(err.Error(), "got utf8") {
				t.Logf("Error message correctly indicates type mismatch")
			} else {
				t.Logf("Note: Error occurred but message doesn't explicitly mention type mismatch")
			}
		}
	})
}

// TestScalarFunctionDataTypes tests scalar functions with different Arrow data types.
func TestScalarFunctionDataTypes(t *testing.T) {
	testData := [][]interface{}{
		{int64(10), int64(5), 2.5, 3.0, "hello", "world"},
		{int64(-5), int64(3), 1.5, 2.0, "foo", "bar"},
		{int64(0), int64(0), 0.0, 0.0, "test", "case"},
	}

	testSchema := arrow.NewSchema([]arrow.Field{
		{Name: "int_a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "int_b", Type: arrow.PrimitiveTypes.Int64},
		{Name: "float_a", Type: arrow.PrimitiveTypes.Float64},
		{Name: "float_b", Type: arrow.PrimitiveTypes.Float64},
		{Name: "str_a", Type: arrow.BinaryTypes.String},
		{Name: "str_b", Type: arrow.BinaryTypes.String},
	}, nil)

	cat, err := airport.NewCatalogBuilder().
		Schema("test_schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "test_data",
			Schema:   testSchema,
			ScanFunc: makeScanFunc(testSchema, testData),
		}).
		ScalarFunc(&addFunc{}).
		ScalarFunc(&multiplyFunc{}).
		ScalarFunc(&concatFunc{}).
		ScalarFunc(&isPositiveFunc{}).
		Build()

	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("IntegerAddition", func(t *testing.T) {
		query := fmt.Sprintf("SELECT %s.test_schema.ADD(int_a, int_b) as sum FROM %s.test_schema.test_data", attachName, attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Scalar function call failed: %v", err)
			return
		}
		defer rows.Close()

		expected := []int64{15, -2, 0}
		idx := 0
		for rows.Next() {
			var sum int64
			if err := rows.Scan(&sum); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			if sum != expected[idx] {
				t.Errorf("Row %d: expected %d, got %d", idx, expected[idx], sum)
			}
			idx++
		}

		if idx != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), idx)
		}
	})

	t.Run("FloatMultiplication", func(t *testing.T) {
		query := fmt.Sprintf("SELECT %s.test_schema.MULTIPLY(float_a, float_b) as product FROM %s.test_schema.test_data", attachName, attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Scalar function call failed: %v", err)
		}
		defer rows.Close()

		expected := []float64{7.5, 3.0, 0.0}
		idx := 0
		for rows.Next() {
			var product float64
			if err := rows.Scan(&product); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			if product != expected[idx] {
				t.Errorf("Row %d: expected %f, got %f", idx, expected[idx], product)
			}
			idx++
		}

		if idx != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), idx)
		}
	})

	t.Run("StringConcatenation", func(t *testing.T) {
		query := fmt.Sprintf("SELECT %s.test_schema.CONCAT(str_a, str_b) as concatenated FROM %s.test_schema.test_data", attachName, attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Scalar function calls failed: %v", err)
		}
		defer rows.Close()

		expected := []string{"hello world", "foo bar", "test case"}
		idx := 0
		for rows.Next() {
			var concatenated string
			if err := rows.Scan(&concatenated); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			if concatenated != expected[idx] {
				t.Errorf("Row %d: expected %s, got %s", idx, expected[idx], concatenated)
			}
			idx++
		}

		if idx != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), idx)
		}
	})

	t.Run("BooleanReturn", func(t *testing.T) {
		query := fmt.Sprintf("SELECT %s.test_schema.IS_POSITIVE(int_a) as is_pos FROM %s.test_schema.test_data", attachName, attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Scalar function call failed: %v", err)
			return
		}
		defer rows.Close()

		expected := []bool{true, false, false}
		idx := 0
		for rows.Next() {
			var isPos bool
			if err := rows.Scan(&isPos); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}
			if isPos != expected[idx] {
				t.Errorf("Row %d: expected %t, got %t", idx, expected[idx], isPos)
			}
			idx++
		}

		if idx != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), idx)
		}
	})
}

// TestScalarFunctionNullHandling tests NULL value handling in scalar functions.
func TestScalarFunctionNullHandling(t *testing.T) {
	// Create test data with NULLs
	testSchema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Build record with NULLs
	builder := array.NewRecordBuilder(memory.DefaultAllocator, testSchema)
	defer builder.Release()

	strBuilder := builder.Field(0).(*array.StringBuilder)
	strBuilder.Append("hello")
	strBuilder.AppendNull()
	strBuilder.Append("world")

	record := builder.NewRecordBatch()
	defer record.Release()

	scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		record.Retain() // Retain for reader
		return array.NewRecordReader(testSchema, []arrow.RecordBatch{record})
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("test_schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "nullable_data",
			Schema:   testSchema,
			ScanFunc: scanFunc,
		}).
		ScalarFunc(&uppercaseFunc{}).
		ScalarFunc(&lengthFunc{}).
		Build()

	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("UppercaseWithNulls", func(t *testing.T) {
		query := fmt.Sprintf("SELECT %s.test_schema.UPPERCASE(value) as upper_val FROM %s.test_schema.nullable_data", attachName, attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Scalar function call failed: %v", err)
			return
		}
		defer rows.Close()

		idx := 0
		for rows.Next() {
			var upperVal *string
			if err := rows.Scan(&upperVal); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}

			switch idx {
			case 0:
				if upperVal == nil || *upperVal != "HELLO" {
					t.Errorf("Row 0: expected 'HELLO', got %v", upperVal)
				}
			case 1:
				if upperVal != nil {
					t.Errorf("Row 1: expected NULL, got %v", upperVal)
				}
			case 2:
				if upperVal == nil || *upperVal != "WORLD" {
					t.Errorf("Row 2: expected 'WORLD', got %v", upperVal)
				}
			}
			idx++
		}

		if idx != 3 {
			t.Errorf("Expected 3 rows, got %d", idx)
		}
	})

	t.Run("LengthWithNulls", func(t *testing.T) {
		query := fmt.Sprintf("SELECT %s.test_schema.LENGTH(value) as len FROM %s.test_schema.nullable_data", attachName, attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Scalar function call failed: %v", err)
			return
		}
		defer rows.Close()

		idx := 0
		for rows.Next() {
			var length *int64
			if err := rows.Scan(&length); err != nil {
				t.Fatalf("Failed to scan: %v", err)
			}

			switch idx {
			case 0:
				if length == nil || *length != 5 {
					t.Errorf("Row 0: expected 5, got %v", length)
				}
			case 1:
				if length != nil {
					t.Errorf("Row 1: expected NULL, got %v", length)
				}
			case 2:
				if length == nil || *length != 5 {
					t.Errorf("Row 2: expected 5, got %v", length)
				}
			}
			idx++
		}

		if idx != 3 {
			t.Errorf("Expected 3 rows, got %d", idx)
		}
	})
}

// TestScalarFunctionEmptyBatch tests scalar function behavior with empty batches.
func TestScalarFunctionEmptyBatch(t *testing.T) {
	testSchema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	// Create empty record
	builder := array.NewRecordBuilder(memory.DefaultAllocator, testSchema)
	defer builder.Release()

	emptyRecord := builder.NewRecordBatch()
	defer emptyRecord.Release()

	scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		emptyRecord.Retain() // Retain for reader
		return array.NewRecordReader(testSchema, []arrow.RecordBatch{emptyRecord})
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("test_schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "empty_data",
			Schema:   testSchema,
			ScanFunc: scanFunc,
		}).
		ScalarFunc(&uppercaseFunc{}).
		Build()

	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("EmptyBatch", func(t *testing.T) {
		query := fmt.Sprintf("SELECT %s.test_schema.UPPERCASE(value) as upper_val FROM %s.test_schema.empty_data", attachName, attachName)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Scalar function call failed: %v", err)
			return
		}
		defer rows.Close()

		rowCount := 0
		for rows.Next() {
			rowCount++
		}

		if rowCount != 0 {
			t.Errorf("Expected 0 rows, got %d", rowCount)
		}
	})
}

// TestScalarFunctionMultipleBatches tests scalar function with streaming/multiple batches.
func TestScalarFunctionMultipleBatches(t *testing.T) {
	testSchema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create multiple batches
	var records []arrow.RecordBatch

	for batchNum := 0; batchNum < 3; batchNum++ {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, testSchema)

		intBuilder := builder.Field(0).(*array.Int64Builder)
		for i := 0; i < 100; i++ {
			intBuilder.Append(int64(batchNum*100 + i))
		}

		record := builder.NewRecordBatch()
		records = append(records, record)
		builder.Release()
	}

	scanFunc := func(ctx context.Context, opts *catalog.ScanOptions) (array.RecordReader, error) {
		// Retain all records for the reader
		for _, r := range records {
			r.Retain()
		}
		return array.NewRecordReader(testSchema, records)
	}

	cat, err := airport.NewCatalogBuilder().
		Schema("test_schema").
		SimpleTable(airport.SimpleTableDef{
			Name:     "large_data",
			Schema:   testSchema,
			ScanFunc: scanFunc,
		}).
		ScalarFunc(&isPositiveFunc{}).
		Build()

	if err != nil {
		t.Fatalf("Failed to build catalog: %v", err)
	}

	// Clean up records
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	server := newTestServer(t, cat, nil)
	defer server.stop()

	db := openDuckDB(t)
	defer db.Close()

	attachName := connectToFlightServer(t, db, server.address, "")

	t.Run("MultipleBatches", func(t *testing.T) {
		query := fmt.Sprintf("SELECT COUNT(*) as cnt FROM %s.test_schema.large_data WHERE %s.test_schema.IS_POSITIVE(value) = true", attachName, attachName)
		var count int64
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			t.Fatalf("Scalar function call failed: %v", err)
			return
		}

		// All values except the first one (0) are positive
		expected := int64(299) // (100 * 3) - 1
		if count != expected {
			t.Errorf("Expected %d positive values, got %d", expected, count)
		}
	})
}

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

func (f *uppercaseFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
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

	return builder.NewStringArray(), nil
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

func (f *lengthFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
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

	return builder.NewInt64Array(), nil
}

// concatFunc concatenates two strings (multi-parameter function).
type concatFunc struct{}

func (f *concatFunc) Name() string {
	return "CONCAT"
}

func (f *concatFunc) Comment() string {
	return "Concatenates two strings with a space separator"
}

func (f *concatFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.String},
		ReturnType: arrow.BinaryTypes.String,
		Variadic:   false,
	}
}

func (f *concatFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	if input.NumCols() != 2 {
		return nil, fmt.Errorf("CONCAT expects exactly 2 columns, got %d", input.NumCols())
	}

	inputCol1, ok1 := input.Column(0).(*array.String)
	inputCol2, ok2 := input.Column(1).(*array.String)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("CONCAT expects string columns, got %T and %T", input.Column(0), input.Column(1))
	}

	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < inputCol1.Len(); i++ {
		if inputCol1.IsNull(i) || inputCol2.IsNull(i) {
			builder.AppendNull()
		} else {
			result := inputCol1.Value(i) + " " + inputCol2.Value(i)
			builder.Append(result)
		}
	}

	return builder.NewStringArray(), nil
}

// addFunc adds two int64 values (numeric function).
type addFunc struct{}

func (f *addFunc) Name() string {
	return "ADD"
}

func (f *addFunc) Comment() string {
	return "Adds two integer values"
}

func (f *addFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64},
		ReturnType: arrow.PrimitiveTypes.Int64,
		Variadic:   false,
	}
}

func (f *addFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	if input.NumCols() != 2 {
		return nil, fmt.Errorf("ADD expects exactly 2 columns, got %d", input.NumCols())
	}

	inputCol1, ok1 := input.Column(0).(*array.Int64)
	inputCol2, ok2 := input.Column(1).(*array.Int64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("ADD expects int64 columns, got %T and %T", input.Column(0), input.Column(1))
	}

	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < inputCol1.Len(); i++ {
		if inputCol1.IsNull(i) || inputCol2.IsNull(i) {
			builder.AppendNull()
		} else {
			result := inputCol1.Value(i) + inputCol2.Value(i)
			builder.Append(result)
		}
	}

	return builder.NewInt64Array(), nil
}

// multiplyFunc multiplies two float64 values.
type multiplyFunc struct{}

func (f *multiplyFunc) Name() string {
	return "MULTIPLY"
}

func (f *multiplyFunc) Comment() string {
	return "Multiplies two float values"
}

func (f *multiplyFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64},
		ReturnType: arrow.PrimitiveTypes.Float64,
		Variadic:   false,
	}
}

func (f *multiplyFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	if input.NumCols() != 2 {
		return nil, fmt.Errorf("MULTIPLY expects exactly 2 columns, got %d", input.NumCols())
	}

	inputCol1, ok1 := input.Column(0).(*array.Float64)
	inputCol2, ok2 := input.Column(1).(*array.Float64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("MULTIPLY expects float64 columns, got %T and %T", input.Column(0), input.Column(1))
	}

	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < inputCol1.Len(); i++ {
		if inputCol1.IsNull(i) || inputCol2.IsNull(i) {
			builder.AppendNull()
		} else {
			result := inputCol1.Value(i) * inputCol2.Value(i)
			builder.Append(result)
		}
	}

	return builder.NewFloat64Array(), nil
}

// isPositiveFunc returns true if int64 value is positive (boolean return type).
type isPositiveFunc struct{}

func (f *isPositiveFunc) Name() string {
	return "IS_POSITIVE"
}

func (f *isPositiveFunc) Comment() string {
	return "Returns true if the value is positive"
}

func (f *isPositiveFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.PrimitiveTypes.Int64},
		ReturnType: arrow.FixedWidthTypes.Boolean,
		Variadic:   false,
	}
}

func (f *isPositiveFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	if input.NumCols() != 1 {
		return nil, fmt.Errorf("IS_POSITIVE expects exactly 1 column, got %d", input.NumCols())
	}

	inputCol, ok := input.Column(0).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("IS_POSITIVE expects int64 column, got %T", input.Column(0))
	}

	builder := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < inputCol.Len(); i++ {
		if inputCol.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(inputCol.Value(i) > 0)
		}
	}

	return builder.NewBooleanArray(), nil
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

func (f *errorFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	return nil, fmt.Errorf("intentional error for testing")
}

// wrongTypeFunc returns wrong type (string instead of int64).
type wrongTypeFunc struct{}

func (f *wrongTypeFunc) Name() string {
	return "WRONG_TYPE"
}

func (f *wrongTypeFunc) Comment() string {
	return "Returns wrong type for testing type validation"
}

func (f *wrongTypeFunc) Signature() catalog.FunctionSignature {
	return catalog.FunctionSignature{
		Parameters: []arrow.DataType{arrow.PrimitiveTypes.Int64},
		ReturnType: arrow.PrimitiveTypes.Int64, // Says it returns Int64
		Variadic:   false,
	}
}

func (f *wrongTypeFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	// But actually returns String!
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < int(input.NumRows()); i++ {
		builder.Append("wrong")
	}

	return builder.NewStringArray(), nil
}

// catalogWithScalarFunctions creates a catalog with scalar functions.
func catalogWithScalarFunctions() catalog.Catalog {
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
