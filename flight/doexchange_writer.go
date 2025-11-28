package flight

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type dataStreamWriter interface {
	Send(*flight.FlightData) error
}

const (
	streamStateInit = iota
	streamStateSchemaSent
	streamStateWorking
)

type schemaPayloadWriter struct {
	stream dataStreamWriter
	state  int
}

func (w *schemaPayloadWriter) Start() error {
	return nil
}

func (w *schemaPayloadWriter) WritePayload(p ipc.Payload) error {
	switch w.state {
	case streamStateInit:
		w.state = streamStateSchemaSent
	case streamStateSchemaSent:
		w.state = streamStateWorking
		return nil
	}
	meta := p.Meta()
	defer meta.Release()

	var body bytes.Buffer
	p.SerializeBody(&body)

	fd := &flight.FlightData{
		DataHeader: meta.Bytes(), // flatbuffer Message (SCHEMA)
		DataBody:   body.Bytes(), // IPC body (empty for schema)
	}

	return w.stream.Send(fd)
}

func (w *schemaPayloadWriter) Close() error {
	return nil
}

type SchemaWriter struct {
	*ipc.Writer
	pw        *schemaPayloadWriter
	allocator memory.Allocator
	schema    *arrow.Schema
}

func NewSchemaWriter(stream dataStreamWriter, schema *arrow.Schema, allocator memory.Allocator, opts ...ipc.Option) *SchemaWriter {
	opts = append(opts, ipc.WithAllocator(allocator), ipc.WithSchema(schema))
	payloadWriter := &schemaPayloadWriter{stream: stream}
	writer := ipc.NewWriterWithPayloadWriter(payloadWriter, opts...)

	return &SchemaWriter{
		Writer:    writer,
		pw:        payloadWriter,
		allocator: allocator,
		schema:    schema,
	}
}

func (w *SchemaWriter) Begin() error {
	// Create empty batch to send schema upfront
	b := array.NewRecordBuilder(w.allocator, w.schema)
	defer b.Release()
	emptyBatch := b.NewRecordBatch()
	defer emptyBatch.Release()

	return w.Write(emptyBatch)
}
