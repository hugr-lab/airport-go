package airport_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestPointInTimeQueryWithTs(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Create ticket with ts parameter (Unix timestamp for 2024-01-01 00:00:00 UTC)
	ticketData := map[string]interface{}{
		"schema": "some_schema",
		"table":  "users",
		"ts":     int64(1704067200),
	}
	ticketBytes, err := json.Marshal(ticketData)
	if err != nil {
		t.Fatalf("failed to marshal ticket: %v", err)
	}

	ticket := &flight.Ticket{
		Ticket: ticketBytes,
	}

	// Execute DoGet with point-in-time ticket
	stream, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		t.Fatalf("DoGet() failed: %v", err)
	}

	// Read all batches (point-in-time query should work like normal query)
	batchCount := 0
	for {
		_, err := stream.Recv()
		if err != nil {
			break // EOF or error
		}
		batchCount++
	}

	// Verify we got data (placeholder catalog returns data regardless of timestamp)
	if batchCount == 0 {
		t.Error("expected at least one batch from point-in-time query")
	}
}

func TestPointInTimeQueryWithTsNs(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Create ticket with ts_ns parameter (nanosecond precision)
	ticketData := map[string]interface{}{
		"schema": "some_schema",
		"table":  "users",
		"ts_ns":  int64(1704067200000000000),
	}
	ticketBytes, err := json.Marshal(ticketData)
	if err != nil {
		t.Fatalf("failed to marshal ticket: %v", err)
	}

	ticket := &flight.Ticket{
		Ticket: ticketBytes,
	}

	// Execute DoGet with point-in-time ticket
	stream, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		t.Fatalf("DoGet() failed: %v", err)
	}

	// Read all batches
	batchCount := 0
	for {
		_, err := stream.Recv()
		if err != nil {
			break // EOF or error
		}
		batchCount++
	}

	// Verify we got data
	if batchCount == 0 {
		t.Error("expected at least one batch from point-in-time query with ts_ns")
	}
}

func TestPointInTimeQueryWithoutTimestamp(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Create ticket WITHOUT timestamp (should return current data)
	ticketData := map[string]interface{}{
		"schema": "some_schema",
		"table":  "users",
	}
	ticketBytes, err := json.Marshal(ticketData)
	if err != nil {
		t.Fatalf("failed to marshal ticket: %v", err)
	}

	ticket := &flight.Ticket{
		Ticket: ticketBytes,
	}

	// Execute DoGet
	stream, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		t.Fatalf("DoGet() failed: %v", err)
	}

	// Read all batches
	batchCount := 0
	for {
		_, err := stream.Recv()
		if err != nil {
			break // EOF or error
		}
		batchCount++
	}

	// Verify we got data (current data query)
	if batchCount == 0 {
		t.Error("expected at least one batch from current data query")
	}
}

func TestPointInTimeQueryBothTimestamps(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Create ticket with BOTH ts and ts_ns (should fail validation)
	ticketData := map[string]interface{}{
		"schema": "some_schema",
		"table":  "users",
		"ts":     int64(1704067200),
		"ts_ns":  int64(1704067200000000000),
	}
	ticketBytes, err := json.Marshal(ticketData)
	if err != nil {
		t.Fatalf("failed to marshal ticket: %v", err)
	}

	ticket := &flight.Ticket{
		Ticket: ticketBytes,
	}

	// Execute DoGet - should fail during stream processing
	stream, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		// Error at DoGet call - this is acceptable
		return
	}

	// Error should occur when trying to receive
	_, err = stream.Recv()
	if err == nil {
		t.Error("expected error for ticket with both ts and ts_ns, got nil")
	}
}

func TestPointInTimeQueryNegativeTimestamp(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	tests := []struct {
		name       string
		ticketData map[string]interface{}
	}{
		{
			name: "negative ts",
			ticketData: map[string]interface{}{
				"schema": "some_schema",
				"table":  "users",
				"ts":     int64(-1),
			},
		},
		{
			name: "negative ts_ns",
			ticketData: map[string]interface{}{
				"schema": "some_schema",
				"table":  "users",
				"ts_ns":  int64(-1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ticketBytes, err := json.Marshal(tt.ticketData)
			if err != nil {
				t.Fatalf("failed to marshal ticket: %v", err)
			}

			ticket := &flight.Ticket{
				Ticket: ticketBytes,
			}

			// Execute DoGet - should fail during stream processing
			stream, err := client.DoGet(context.Background(), ticket)
			if err != nil {
				// Error at DoGet call - this is acceptable
				return
			}

			// Error should occur when trying to receive
			_, err = stream.Recv()
			if err == nil {
				t.Error("expected error for negative timestamp, got nil")
			}
		})
	}
}

func TestPointInTimeQueryWithColumns(t *testing.T) {
	server := newTestServer(t, simpleCatalog(), nil)
	defer server.stop()

	conn, err := grpc.NewClient(server.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)

	// Create ticket with ts and column projection
	ticketData := map[string]interface{}{
		"schema":  "some_schema",
		"table":   "users",
		"ts":      int64(1704067200),
		"columns": []string{"id", "name"},
	}
	ticketBytes, err := json.Marshal(ticketData)
	if err != nil {
		t.Fatalf("failed to marshal ticket: %v", err)
	}

	ticket := &flight.Ticket{
		Ticket: ticketBytes,
	}

	// Execute DoGet
	stream, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		t.Fatalf("DoGet() failed: %v", err)
	}

	// Read all batches
	batchCount := 0
	for {
		_, err := stream.Recv()
		if err != nil {
			break // EOF or error
		}
		batchCount++
	}

	// Verify we got data (point-in-time with column projection)
	if batchCount == 0 {
		t.Error("expected at least one batch from point-in-time query with column projection")
	}
}
