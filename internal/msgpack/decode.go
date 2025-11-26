// Package msgpack provides MessagePack encoding/decoding for Flight parameters.
// Used by DoPut RPC to deserialize query parameters.
package msgpack

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// Decode deserializes MessagePack data into a Go value.
// The v parameter should be a pointer to the target structure.
//
// Example:
//
//	type QueryParams struct {
//	    Schema  string   `msgpack:"schema"`
//	    Table   string   `msgpack:"table"`
//	    Columns []string `msgpack:"columns,omitempty"`
//	}
//
//	var params QueryParams
//	err := msgpack.Decode(data, &params)
func Decode(data []byte, v interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("empty MessagePack data")
	}

	if err := msgpack.Unmarshal(data, v); err != nil {
		return fmt.Errorf("failed to decode MessagePack: %w", err)
	}

	return nil
}

// Encode serializes a Go value into MessagePack format.
// Returns the serialized bytes or error.
//
// Example:
//
//	params := QueryParams{
//	    Schema: "main",
//	    Table:  "users",
//	}
//	data, err := msgpack.Encode(params)
func Encode(v interface{}) ([]byte, error) {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to encode MessagePack: %w", err)
	}

	return data, nil
}

// DecodeMap deserializes MessagePack data into a map[string]interface{}.
// This is useful when the structure is not known at compile time.
func DecodeMap(data []byte) (map[string]interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty MessagePack data")
	}

	var result map[string]interface{}
	if err := msgpack.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to decode MessagePack map: %w", err)
	}

	return result, nil
}

// DecodeSlice deserializes MessagePack data into a []interface{}.
// This is useful for positional parameters.
func DecodeSlice(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty MessagePack data")
	}

	var result []interface{}
	if err := msgpack.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to decode MessagePack slice: %w", err)
	}

	return result, nil
}
