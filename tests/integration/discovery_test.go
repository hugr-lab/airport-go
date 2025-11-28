package airport_test

import (
	"context"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
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

