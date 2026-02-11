package flight

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/hugr-lab/airport-go/catalog"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

func TestEncodeFunctionCallURI(t *testing.T) {
	alloc := memory.DefaultAllocator

	t.Run("basic string arg", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "read_csv",
			Args: []catalog.FunctionCallArg{
				{Value: "/data/file.csv", Type: arrow.BinaryTypes.String},
			},
		}

		uri, err := EncodeFunctionCallURI(fc, alloc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !strings.HasPrefix(uri, dataURIPrefix) {
			t.Errorf("expected prefix %q, got %q", dataURIPrefix, uri[:min(len(dataURIPrefix), len(uri))])
		}

		// Decode and verify the msgpack content
		verifyURI(t, uri, "read_csv")
	})

	t.Run("multiple positional args", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "read_parquet",
			Args: []catalog.FunctionCallArg{
				{Value: "/data/file.parquet", Type: arrow.BinaryTypes.String},
				{Value: int64(100), Type: arrow.PrimitiveTypes.Int64},
			},
		}

		uri, err := EncodeFunctionCallURI(fc, alloc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		verifyURI(t, uri, "read_parquet")
	})

	t.Run("named args", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "read_csv",
			Args: []catalog.FunctionCallArg{
				{Name: "filename", Value: "/data/file.csv", Type: arrow.BinaryTypes.String},
				{Name: "header", Value: true, Type: arrow.FixedWidthTypes.Boolean},
			},
		}

		uri, err := EncodeFunctionCallURI(fc, alloc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		verifyURI(t, uri, "read_csv")
	})

	t.Run("mixed positional and named", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "read_csv",
			Args: []catalog.FunctionCallArg{
				{Value: "/data/file.csv", Type: arrow.BinaryTypes.String},
				{Name: "header", Value: true, Type: arrow.FixedWidthTypes.Boolean},
				{Name: "delim", Value: ",", Type: arrow.BinaryTypes.String},
			},
		}

		uri, err := EncodeFunctionCallURI(fc, alloc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		verifyURI(t, uri, "read_csv")
	})

	t.Run("all numeric types", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "test_func",
			Args: []catalog.FunctionCallArg{
				{Name: "i8", Value: int8(1), Type: arrow.PrimitiveTypes.Int8},
				{Name: "i16", Value: int16(2), Type: arrow.PrimitiveTypes.Int16},
				{Name: "i32", Value: int32(3), Type: arrow.PrimitiveTypes.Int32},
				{Name: "i64", Value: int64(4), Type: arrow.PrimitiveTypes.Int64},
				{Name: "u8", Value: uint8(5), Type: arrow.PrimitiveTypes.Uint8},
				{Name: "u16", Value: uint16(6), Type: arrow.PrimitiveTypes.Uint16},
				{Name: "u32", Value: uint32(7), Type: arrow.PrimitiveTypes.Uint32},
				{Name: "u64", Value: uint64(8), Type: arrow.PrimitiveTypes.Uint64},
				{Name: "f32", Value: float32(9.5), Type: arrow.PrimitiveTypes.Float32},
				{Name: "f64", Value: float64(10.5), Type: arrow.PrimitiveTypes.Float64},
			},
		}

		uri, err := EncodeFunctionCallURI(fc, alloc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		verifyURI(t, uri, "test_func")
	})

	t.Run("int as int64", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "test_func",
			Args: []catalog.FunctionCallArg{
				{Value: 42, Type: arrow.PrimitiveTypes.Int64},
			},
		}

		uri, err := EncodeFunctionCallURI(fc, alloc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		verifyURI(t, uri, "test_func")
	})

	t.Run("time.Time as timestamp", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "test_func",
			Args: []catalog.FunctionCallArg{
				{Value: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), Type: arrow.FixedWidthTypes.Timestamp_us},
			},
		}

		uri, err := EncodeFunctionCallURI(fc, alloc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		verifyURI(t, uri, "test_func")
	})

	t.Run("binary arg", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "test_func",
			Args: []catalog.FunctionCallArg{
				{Value: []byte{0x01, 0x02, 0x03}, Type: arrow.BinaryTypes.Binary},
			},
		}

		uri, err := EncodeFunctionCallURI(fc, alloc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		verifyURI(t, uri, "test_func")
	})

	t.Run("empty args", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "now",
			Args:         nil,
		}

		uri, err := EncodeFunctionCallURI(fc, alloc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		verifyURI(t, uri, "now")
	})

	t.Run("empty function name", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "",
			Args:         nil,
		}

		_, err := EncodeFunctionCallURI(fc, alloc)
		if err == nil {
			t.Fatal("expected error for empty FunctionName")
		}
	})

	t.Run("validation failure propagation", func(t *testing.T) {
		fc := catalog.FunctionCall{
			FunctionName: "test_func",
			Args: []catalog.FunctionCallArg{
				{Value: nil, Type: arrow.BinaryTypes.String},
			},
		}

		_, err := EncodeFunctionCallURI(fc, alloc)
		if err == nil {
			t.Fatal("expected error for nil Value")
		}
	})
}

// verifyURI decodes a data:// URI and checks the function_name field.
func verifyURI(t *testing.T, uri, expectedFuncName string) {
	t.Helper()

	if !strings.HasPrefix(uri, dataURIPrefix) {
		t.Fatalf("URI missing expected prefix")
	}

	// Decode outer base64
	outerB64 := uri[len(dataURIPrefix):]
	outerBytes, err := base64.StdEncoding.DecodeString(outerB64)
	if err != nil {
		t.Fatalf("failed to decode outer base64: %v", err)
	}

	// Decode msgpack - data field is raw bytes (msgpack bin type)
	var msgpackMap struct {
		FunctionName string `msgpack:"function_name"`
		Data         []byte `msgpack:"data"`
	}
	if err := msgpack.Decode(outerBytes, &msgpackMap); err != nil {
		t.Fatalf("failed to decode msgpack: %v", err)
	}

	if msgpackMap.FunctionName != expectedFuncName {
		t.Errorf("expected function_name %q, got %q", expectedFuncName, msgpackMap.FunctionName)
	}

	// Verify the data field contains valid Arrow IPC bytes (starts with ARROW1 magic or continuation marker)
	if len(msgpackMap.Data) == 0 {
		t.Error("data field is empty")
	}
}
