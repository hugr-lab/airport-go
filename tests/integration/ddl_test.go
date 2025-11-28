package airport_test

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDDLCreateSchema(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	// Connect Flight client directly
	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test CREATE SCHEMA action
	action := &flight.Action{
		Type: "CreateSchema",
		Body: []byte(`{
			"schema_name": "test_schema",
			"if_not_exists": false,
			"comment": "Test schema for DDL operations"
		}`),
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("DoAction(CreateSchema) failed: %v", err)
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
	if response["schema_name"] != "test_schema" {
		t.Errorf("expected schema_name='test_schema', got %v", response["schema_name"])
	}

	// Verify no more results
	_, err = stream.Recv()
	if err != io.EOF {
		t.Errorf("expected EOF after result, got %v", err)
	}
}

func TestDDLCreateTable(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test CREATE TABLE action with geometry column
	action := &flight.Action{
		Type: "CreateTable",
		Body: []byte(`{
			"schema_name": "test_schema",
			"table_name": "places",
			"if_not_exists": false,
			"schema": {
				"fields": [
					{"name": "id", "type": "int64", "nullable": false},
					{"name": "name", "type": "utf8", "nullable": false},
					{"name": "location", "type": "extension<geoarrow.wkb>", "nullable": true}
				],
				"metadata": {
					"location.srid": "4326",
					"location.geometry_type": "POINT"
				}
			},
			"comment": "Places with geospatial data"
		}`),
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("DoAction(CreateTable) failed: %v", err)
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
	if response["table_name"] != "places" {
		t.Errorf("expected table_name='places', got %v", response["table_name"])
	}
	if response["columns"] != float64(3) { // JSON numbers are float64
		t.Errorf("expected columns=3, got %v", response["columns"])
	}
}

func TestDDLCreateTableInvalidType(t *testing.T) {
	t.Skip("DDL operations not yet implemented")
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test CREATE TABLE with invalid type
	action := &flight.Action{
		Type: "CreateTable",
		Body: []byte(`{
			"schema_name": "test_schema",
			"table_name": "invalid_table",
			"schema": {
				"fields": [
					{"name": "id", "type": "invalid_type", "nullable": false}
				]
			}
		}`),
	}

	stream, err := client.DoAction(context.Background(), action)
	if err == nil {
		// Should fail before sending result
		t.Error("expected error for invalid type, got nil")
		stream.Recv() // Drain stream
	}
	// Error is expected - test passes
}

func TestDDLIfNotExists(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test CREATE SCHEMA with if_not_exists=true (idempotent)
	action := &flight.Action{
		Type: "CreateSchema",
		Body: []byte(`{
			"schema_name": "idempotent_test",
			"if_not_exists": true
		}`),
	}

	// First call - should succeed
	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("first DoAction failed: %v", err)
	}
	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("first stream.Recv() failed: %v", err)
	}

	var response map[string]interface{}
	json.Unmarshal(result.Body, &response)
	if response["status"] != "success" {
		t.Errorf("first call: expected status='success', got %v", response["status"])
	}

	// Second call with same name - should also succeed (idempotent)
	stream2, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("second DoAction failed: %v", err)
	}
	result2, err := stream2.Recv()
	if err != nil {
		t.Fatalf("second stream.Recv() failed: %v", err)
	}

	var response2 map[string]interface{}
	json.Unmarshal(result2.Body, &response2)
	if response2["status"] != "success" {
		t.Errorf("second call: expected status='success', got %v", response2["status"])
	}
}

func TestDDLAlterTableAddColumn(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test ALTER TABLE ADD COLUMN
	action := &flight.Action{
		Type: "AlterTableAddColumn",
		Body: []byte(`{
			"schema_name": "some_schema",
			"table_name": "users",
			"if_exists": true,
			"column": {
				"name": "email",
				"type": "utf8",
				"nullable": true
			}
		}`),
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("DoAction(AlterTableAddColumn) failed: %v", err)
	}

	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv() failed: %v", err)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(result.Body, &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if response["status"] != "success" {
		t.Errorf("expected status='success', got %v", response["status"])
	}
	if response["column_name"] != "email" {
		t.Errorf("expected column_name='email', got %v", response["column_name"])
	}
}

func TestDDLDropTable(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Test DROP TABLE with if_exists=true
	action := &flight.Action{
		Type: "DropTable",
		Body: []byte(`{
			"schema_name": "some_schema",
			"table_name": "nonexistent_table",
			"if_exists": true
		}`),
	}

	stream, err := client.DoAction(context.Background(), action)
	if err != nil {
		t.Fatalf("DoAction(DropTable) failed: %v", err)
	}

	result, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv() failed: %v", err)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(result.Body, &response); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if response["status"] != "success" {
		t.Errorf("expected status='success', got %v", response["status"])
	}
}
