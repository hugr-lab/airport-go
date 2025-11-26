package serialize

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Compressor handles ZStandard compression for catalog data.
// Create once and reuse to eliminate allocations.
type Compressor struct {
	encoder *zstd.Encoder
}

// NewCompressor creates a reusable ZStandard compressor.
// Uses SpeedDefault (level 3) for balanced compression ratio and speed.
// Caller must call Close() when done to release resources.
func NewCompressor() (*Compressor, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	return &Compressor{
		encoder: encoder,
	}, nil
}

// Compress compresses data using ZStandard.
// Returns compressed bytes or error.
// Safe for concurrent use from multiple goroutines.
func (c *Compressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	// Pre-allocate destination buffer with estimated size
	// ZStandard typically achieves 60-70% compression
	dst := make([]byte, 0, len(data)/2)

	// EncodeAll is goroutine-safe
	compressed := c.encoder.EncodeAll(data, dst)

	return compressed, nil
}

// Close releases compressor resources.
// Must be called when compressor is no longer needed.
func (c *Compressor) Close() error {
	if c.encoder != nil {
		return c.encoder.Close()
	}
	return nil
}

// Decompressor handles ZStandard decompression.
// Create once and reuse to eliminate allocations.
type Decompressor struct {
	decoder *zstd.Decoder
}

// NewDecompressor creates a reusable ZStandard decompressor.
// Caller must call Close() when done to release resources.
func NewDecompressor() (*Decompressor, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &Decompressor{
		decoder: decoder,
	}, nil
}

// Decompress decompresses ZStandard data.
// Returns decompressed bytes or error.
// Safe for concurrent use from multiple goroutines.
func (d *Decompressor) Decompress(compressed []byte) ([]byte, error) {
	if len(compressed) == 0 {
		return []byte{}, nil
	}

	// DecodeAll is goroutine-safe
	decompressed, err := d.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress: %w", err)
	}

	return decompressed, nil
}

// Close releases decompressor resources.
// Must be called when decompressor is no longer needed.
func (d *Decompressor) Close() {
	if d.decoder != nil {
		d.decoder.Close()
	}
}
