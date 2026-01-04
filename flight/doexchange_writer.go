package flight

import (
	"bytes"
	"sync/atomic"

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
	streamStateInit = int32(iota)
	streamStateSchemaSent
	streamStateWorking
	streamStateFinished
)

type schemaPayloadWriter struct {
	withChunkSyncReadWrite bool
	stream                 dataStreamWriter
	state                  atomic.Int32
}

func (w *schemaPayloadWriter) Start() error {
	return nil
}

func (w *schemaPayloadWriter) WritePayload(p ipc.Payload) error {
	writeFinished := false
	switch w.state.Load() {
	case streamStateInit:
		w.state.Store(streamStateSchemaSent)
	case streamStateSchemaSent:
		w.state.Store(streamStateWorking)
		return nil
	case streamStateFinished:
		writeFinished = true
		w.state.Store(streamStateWorking)
	}
	meta := p.Meta()
	defer meta.Release()

	var body bytes.Buffer
	p.SerializeBody(&body)

	fd := &flight.FlightData{
		DataHeader: meta.Bytes(), // flatbuffer Message (SCHEMA)
		DataBody:   body.Bytes(), // IPC body (empty for schema)
	}
	if w.withChunkSyncReadWrite && writeFinished {
		// indicate end of chunk
		fd.AppMetadata = []byte("chunk_finished")
	}
	if w.withChunkSyncReadWrite && !writeFinished {
		// indicate	continuation of chunk
		fd.AppMetadata = []byte("chunk_continues")
	}

	return w.stream.Send(fd)
}

func (w *schemaPayloadWriter) Close() error {
	return nil
}

type SchemaWriter struct {
	*ipc.Writer
	pw         *schemaPayloadWriter
	allocator  memory.Allocator
	schema     *arrow.Schema
	emptyBatch arrow.RecordBatch
}

func NewSchemaWriter(stream dataStreamWriter, schema *arrow.Schema, allocator memory.Allocator, withChunkSyncReadWrite bool, opts ...ipc.Option) *SchemaWriter {
	opts = append(opts, ipc.WithAllocator(allocator), ipc.WithSchema(schema))
	payloadWriter := &schemaPayloadWriter{stream: stream, withChunkSyncReadWrite: withChunkSyncReadWrite}
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
	w.emptyBatch = b.NewRecordBatch()

	return w.Write(w.emptyBatch)
}

func (w *SchemaWriter) WriteFinished() error {
	w.pw.state.Store(streamStateFinished)
	return w.Write(w.emptyBatch)
}
