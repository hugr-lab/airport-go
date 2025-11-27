package airport_test

import (
	"context"
	"io"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hugr-lab/airport-go/internal/msgpack"
)

func TestListSchemas(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test list_schemas action
	action := &flight.Action{
		Type: "list_schemas",
		Body: []byte{},
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("DoAction(list_schemas) failed: %v", err)
	}

	// Read response
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv() failed: %v", err)
	}

	// Parse compressed response (Airport format)
	// AirportSerializedCompressedContent is encoded as ARRAY: [length, data]
	var compressedContent []interface{}
	if err := msgpack.Decode(result.Body, &compressedContent); err != nil {
		t.Fatalf("failed to decode compressed content: %v", err)
	}

	// Verify array structure
	if len(compressedContent) != 2 {
		t.Fatalf("expected array of 2 elements, got %d", len(compressedContent))
	}

	// Element 0 is length (uint32), element 1 is data (string)
	dataStr, ok := compressedContent[1].(string)
	if !ok {
		t.Fatalf("data element is not a string: %T", compressedContent[1])
	}

	// Verify we got compressed data
	if len(dataStr) == 0 {
		t.Error("expected non-empty compressed data")
	}

	t.Logf("Received compressed data: %d bytes", len(dataStr))

	// Verify no more results
	_, err = stream.Recv()
	if err != io.EOF {
		t.Errorf("expected EOF after result, got %v", err)
	}
}

func TestListTables(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test list_tables action with schema_name parameter
	params := map[string]interface{}{
		"schema_name": "some_schema",
	}
	paramsBytes, err := msgpack.Encode(params)
	if err != nil {
		t.Fatalf("failed to encode params: %v", err)
	}

	action := &flight.Action{
		Type: "list_tables",
		Body: paramsBytes,
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("DoAction(list_tables) failed: %v", err)
	}

	// Read response
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv() failed: %v", err)
	}

	// Parse response (MessagePack)
	var response map[string]interface{}
	if err := msgpack.Decode(result.Body, &response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify schema field
	schemaField, ok := response["schema"]
	if !ok {
		t.Fatal("response missing 'schema' field")
	}
	if schemaField != "some_schema" {
		t.Errorf("schema = %v, want 'some_schema'", schemaField)
	}

	// Verify tables list
	tablesIface, ok := response["tables"]
	if !ok {
		t.Fatal("response missing 'tables' field")
	}

	tables, ok := tablesIface.([]interface{})
	if !ok {
		t.Fatalf("tables is not an array: %T", tablesIface)
	}

	// Should have at least "users" table
	found := false
	for _, tbl := range tables {
		if tableName, ok := tbl.(string); ok && tableName == "users" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected to find 'users' table in list")
	}

	// Verify no more results
	_, err = stream.Recv()
	if err != io.EOF {
		t.Errorf("expected EOF after result, got %v", err)
	}
}

func TestListTablesAllSchemas(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test list_tables action without schema_name (should list all)
	action := &flight.Action{
		Type: "list_tables",
		Body: []byte{},
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("DoAction(list_tables) failed: %v", err)
	}

	// Read response
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv() failed: %v", err)
	}

	// Parse response (MessagePack)
	var response map[string]interface{}
	if err := msgpack.Decode(result.Body, &response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify tables map
	tablesIface, ok := response["tables"]
	if !ok {
		t.Fatal("response missing 'tables' field")
	}

	// Should be a map[string][]string
	tablesMap, ok := tablesIface.(map[string]interface{})
	if !ok {
		t.Fatalf("tables is not a map: %T", tablesIface)
	}

	// Should have at least "some_schema" schema
	if _, ok := tablesMap["some_schema"]; !ok {
		t.Error("expected to find 'some_schema' schema in tables map")
	}

	// Verify no more results
	_, err = stream.Recv()
	if err != io.EOF {
		t.Errorf("expected EOF after result, got %v", err)
	}
}

func TestListTablesInvalidSchema(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test list_tables action with nonexistent schema
	params := map[string]interface{}{
		"schema_name": "nonexistent",
	}
	paramsBytes, err := msgpack.Encode(params)
	if err != nil {
		t.Fatalf("failed to encode params: %v", err)
	}

	action := &flight.Action{
		Type: "list_tables",
		Body: paramsBytes,
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		// Error at DoAction call - acceptable
		return
	}

	// Error should occur when trying to receive
	_, err = stream.Recv()
	if err == nil {
		t.Error("expected error for nonexistent schema, got nil")
	}
}
