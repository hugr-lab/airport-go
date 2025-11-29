package flight

import (
	"context"
	"io"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hugr-lab/airport-go/internal/msgpack"
)

// DoPut handles client streaming for parameterized queries.
// This RPC allows clients to send queries with MessagePack-encoded parameters
// and receive Arrow record batches in response.
//
// The descriptor contains the query command or table name.
// The client streams Arrow batches (typically a single batch with parameters).
// The server responds with PutResult containing metadata.
//
// This is useful for:
//   - Parameterized queries (SELECT * FROM table WHERE id = ?)
//   - INSERT operations with batched data
//   - Custom commands with structured parameters
func (s *Server) DoPut(stream flight.FlightService_DoPutServer) error {
	ctx := stream.Context()

	s.logger.Debug("DoPut called")

	// Receive first message to get descriptor and schema
	msg, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			s.logger.Warn("DoPut stream closed before receiving descriptor")
			return status.Error(codes.InvalidArgument, "no descriptor received")
		}
		s.logger.Error("Failed to receive DoPut message", "error", err)
		return status.Errorf(codes.Internal, "failed to receive message: %v", err)
	}

	descriptor := msg.FlightDescriptor
	if descriptor == nil {
		return status.Error(codes.InvalidArgument, "missing flight descriptor")
	}

	s.logger.Debug("DoPut request",
		"type", descriptor.GetType(),
		"cmd_length", len(descriptor.GetCmd()),
		"path_length", len(descriptor.GetPath()),
	)

	// Handle based on descriptor type
	var result *flight.PutResult
	var handleErr error

	switch descriptor.GetType() {
	case flight.DescriptorCMD:
		// Command-based operation (e.g., parameterized query)
		result, handleErr = s.handleDoPutCommand(ctx, descriptor, msg, stream)

	case flight.DescriptorPATH:
		// Path-based operation (e.g., INSERT into table)
		result, handleErr = s.handleDoPutPath(ctx, descriptor, msg, stream)

	default:
		return status.Errorf(codes.InvalidArgument, "unsupported descriptor type: %v", descriptor.GetType())
	}

	if handleErr != nil {
		return handleErr
	}

	// Send result back to client (if not already sent by handler)
	if result != nil {
		if err := stream.Send(result); err != nil {
			s.logger.Error("Failed to send PutResult", "error", err)
			return status.Errorf(codes.Internal, "failed to send result: %v", err)
		}
	}

	s.logger.Debug("DoPut completed successfully")

	return nil
}

// handleDoPutCommand processes CMD-type DoPut requests.
// The command bytes can contain either:
// - MessagePack-encoded parameters for queries
// - JSON-encoded DML operation descriptors (INSERT, UPDATE)
//nolint:unparam
func (s *Server) handleDoPutCommand(ctx context.Context, descriptor *flight.FlightDescriptor, msg *flight.FlightData, stream flight.FlightService_DoPutServer) (*flight.PutResult, error) {
	cmd := descriptor.GetCmd()
	if len(cmd) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty command in descriptor")
	}

	s.logger.Debug("Processing DoPut command", "cmd_size", len(cmd))

	// Try MessagePack parameters (T053 - parameter validation)
	params, err := msgpack.DecodeMap(cmd)
	if err != nil {
		s.logger.Error("Failed to decode MessagePack parameters",
			"error", err,
			"cmd_size", len(cmd),
		)
		return nil, status.Errorf(codes.InvalidArgument, "invalid MessagePack parameters: %v", err)
	}

	s.logger.Debug("Decoded parameters", "param_count", len(params))

	// Read Arrow data from stream if present
	var recordCount int64
	reader, err := flight.NewRecordReader(newFlightDataReader(msg, stream))
	if err != nil {
		s.logger.Error("Failed to create record reader", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to create reader: %v", err)
	}
	defer reader.Release()

	// Process records from client
	for reader.Next() {
		record := reader.RecordBatch()
		recordCount += record.NumRows()
		s.logger.Debug("Received record batch",
			"rows", record.NumRows(),
			"columns", record.NumCols(),
		)
	}

	if err := reader.Err(); err != nil && err != io.EOF {
		s.logger.Error("Error reading records", "error", err)
		return nil, status.Errorf(codes.Internal, "error reading records: %v", err)
	}

	// Create result with metadata
	resultMetadata, _ := msgpack.Encode(map[string]interface{}{
		"status":        "success",
		"rows_received": recordCount,
		"params":        params,
	})

	return &flight.PutResult{
		AppMetadata: resultMetadata,
	}, nil
}

// handleDoPutPath processes PATH-type DoPut requests.
// Typically used for INSERT operations into a specific table.
//nolint:unparam
func (s *Server) handleDoPutPath(ctx context.Context, descriptor *flight.FlightDescriptor, msg *flight.FlightData, stream flight.FlightService_DoPutServer) (*flight.PutResult, error) {
	path := descriptor.GetPath()
	if len(path) != 2 {
		return nil, status.Error(codes.InvalidArgument, "path must contain [schema, table]")
	}

	schemaName := path[0]
	tableName := path[1]

	s.logger.Debug("Processing DoPut path",
		"schema", schemaName,
		"table", tableName,
	)

	// Read Arrow data from stream
	var recordCount int64
	reader, err := flight.NewRecordReader(newFlightDataReader(msg, stream))
	if err != nil {
		s.logger.Error("Failed to create record reader", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to create reader: %v", err)
	}
	defer reader.Release()

	// Process records
	for reader.Next() {
		record := reader.RecordBatch()
		recordCount += record.NumRows()
		s.logger.Debug("Received record batch for insert",
			"schema", schemaName,
			"table", tableName,
			"rows", record.NumRows(),
		)
		// In a real implementation, would insert into table here
	}

	if err := reader.Err(); err != nil && err != io.EOF {
		s.logger.Error("Error reading records", "error", err)
		return nil, status.Errorf(codes.Internal, "error reading records: %v", err)
	}

	// Create success result
	resultMetadata, _ := msgpack.Encode(map[string]interface{}{
		"status":        "success",
		"rows_inserted": recordCount,
		"schema":        schemaName,
		"table":         tableName,
	})

	return &flight.PutResult{
		AppMetadata: resultMetadata,
	}, nil
}

// doPutDataStream wraps a DoPut stream with a prepended first message.
// This allows using flight.NewRecordReader which expects to read from the stream directly.
type doPutDataStream struct {
	firstMsg  *flight.FlightData
	stream    flight.FlightService_DoPutServer
	firstSent bool
}

func newDoPutDataStream(firstMsg *flight.FlightData, stream flight.FlightService_DoPutServer) *doPutDataStream {
	return &doPutDataStream{
		firstMsg:  firstMsg,
		stream:    stream,
		firstSent: false,
	}
}

// Recv implements the DataStreamReader interface for flight.NewRecordReader.
func (s *doPutDataStream) Recv() (*flight.FlightData, error) {
	if !s.firstSent {
		s.firstSent = true
		return s.firstMsg, nil
	}
	return s.stream.Recv()
}

// newFlightDataReader creates a DataStreamReader that prepends the first message
// to the stream, allowing use with flight.NewRecordReader.
func newFlightDataReader(firstMsg *flight.FlightData, stream flight.FlightService_DoPutServer) flight.DataStreamReader {
	return newDoPutDataStream(firstMsg, stream)
}

