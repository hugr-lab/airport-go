package airport_test

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDMLDelete(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	// Connect Flight client
	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test DELETE action
	action := &flight.Action{
		Type: "Delete",
		Body: []byte(`{
			"schema_name": "some_schema",
			"table_name": "users",
			"row_ids": [1, 2, 3]
		}`),
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("DoAction(Delete) failed: %v", err)
	}

	// Read response
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv() failed: %v", err)
	}

	// Parse response
	var response map[string]interface{}
	if err := json.Unmarshal(result.Body, &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if response["status"] != "success" {
		t.Errorf("expected status='success', got %v", response["status"])
	}
	if response["affected_rows"] != float64(3) {
		t.Errorf("expected affected_rows=3, got %v", response["affected_rows"])
	}

	// Verify no more results
	_, err = stream.Recv()
	if err != io.EOF {
		t.Errorf("expected EOF after result, got %v", err)
	}
}

func TestDMLDeleteInvalidPayload(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test DELETE with missing row_ids
	action := &flight.Action{
		Type: "Delete",
		Body: []byte(`{
			"schema_name": "some_schema",
			"table_name": "users"
		}`),
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		// Error during action creation - this is acceptable
		return
	}

	// Try to receive result - should get error
	_, err = stream.Recv()
	if err == nil {
		t.Error("expected error for missing row_ids in response, got nil")
	}
}

func TestDMLInsert(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Create test data
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// Create INSERT descriptor
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd: []byte(`{
			"operation": "insert",
			"schema_name": "some_schema",
			"table_name": "users"
		}`),
	}

	// Start DoPut stream
	stream, err := client.DoPut(context.Background())
	if err != nil {
		t.Fatalf("DoPut() failed: %v", err)
	}

	// Create IPC writer to encode record batch
	var buf []byte
	writer := ipc.NewWriter(&byteSliceWriter{data: &buf}, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
	defer writer.Close()

	if err := writer.Write(record); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}

	// Send first message with descriptor and data
	if err := stream.Send(&flight.FlightData{
		FlightDescriptor: descriptor,
		DataBody:         buf,
	}); err != nil {
		t.Fatalf("stream.Send() failed: %v", err)
	}

	// Close send side
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() failed: %v", err)
	}

	// Receive result
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv() failed: %v", err)
	}

	// Result should be sent (placeholder implementation doesn't validate much)
	_ = result
}

func TestDMLUpdate(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Create test data with updated values
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).AppendValues([]string{"Updated Alice", "Updated Bob"}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// Create UPDATE descriptor with row_ids
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd: []byte(`{
			"operation": "update",
			"schema_name": "some_schema",
			"table_name": "users",
			"row_ids": [1, 2]
		}`),
	}

	// Start DoPut stream
	stream, err := client.DoPut(context.Background())
	if err != nil {
		t.Fatalf("DoPut() failed: %v", err)
	}

	// Create IPC writer to encode record batch
	var buf []byte
	writer := ipc.NewWriter(&byteSliceWriter{data: &buf}, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
	defer writer.Close()

	if err := writer.Write(record); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}

	// Send first message with descriptor and data
	if err := stream.Send(&flight.FlightData{
		FlightDescriptor: descriptor,
		DataBody:         buf,
	}); err != nil {
		t.Fatalf("stream.Send() failed: %v", err)
	}

	// Close send side
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() failed: %v", err)
	}

	// Receive result
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv() failed: %v", err)
	}

	_ = result
}

func TestDMLInsertWithGeometry(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Create test data with geometry column (WKB encoded)
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "location", Type: arrow.BinaryTypes.Binary, Nullable: true}, // WKB geometry
		},
		nil,
	)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Place A", "Place B"}, nil)

	// Simple WKB-encoded POINT(1.0 2.0) and POINT(3.0 4.0)
	// WKB format: byte order (1 byte) + type (4 bytes) + x (8 bytes) + y (8 bytes)
	// This is a simplified example - in real use, would use orb library
	point1WKB := []byte{
		0x01,                                           // Little endian
		0x01, 0x00, 0x00, 0x00,                         // Point type (1)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
	}
	point2WKB := []byte{
		0x01,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // y = 4.0
	}

	builder.Field(2).(*array.BinaryBuilder).AppendValues([][]byte{point1WKB, point2WKB}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// Create INSERT descriptor
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd: []byte(`{
			"operation": "insert",
			"schema_name": "some_schema",
			"table_name": "places"
		}`),
	}

	// Start DoPut stream
	stream, err := client.DoPut(context.Background())
	if err != nil {
		t.Fatalf("DoPut() failed: %v", err)
	}

	// Create IPC writer to encode record batch
	var buf []byte
	writer := ipc.NewWriter(&byteSliceWriter{data: &buf}, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
	defer writer.Close()

	if err := writer.Write(record); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}

	// Send first message with descriptor and data
	if err := stream.Send(&flight.FlightData{
		FlightDescriptor: descriptor,
		DataBody:         buf,
	}); err != nil {
		t.Fatalf("stream.Send() failed: %v", err)
	}

	// Close send side
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() failed: %v", err)
	}

	// Receive result
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv() failed: %v", err)
	}

	_ = result
}

// byteSliceWriter implements io.Writer by appending to a byte slice pointer
type byteSliceWriter struct {
	data *[]byte
}

func (w *byteSliceWriter) Write(p []byte) (n int, err error) {
	*w.data = append(*w.data, p...)
	return len(p), nil
}
