package flight

import (
	"bytes"
	"fmt"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/hugr-lab/airport-go/internal/msgpack"
)

// decodeTableFunctionParams extracts function parameters from msgpack-encoded metadata.
// The metadata contains a map with a "parameters" field that can be in multiple formats:
// - []byte: msgpack-encoded array
// - []any: direct array
// - string: Arrow IPC-encoded RecordBatch
func decodeTableFunctionParams(appMetadata []byte, logger *slog.Logger) []any {
	if len(appMetadata) == 0 {
		return []any{}
	}

	// Decode top-level msgpack map
	paramMap, err := decodeMsgpackMap(appMetadata, logger)
	if err != nil {
		return []any{}
	}

	// Extract "parameters" field
	paramsVal, ok := paramMap["parameters"]
	if !ok {
		logger.Warn("No 'parameters' field in metadata map")
		return []any{}
	}

	logger.Debug("Parameters field found",
		"type", fmt.Sprintf("%T", paramsVal),
		"value_len", len(fmt.Sprintf("%v", paramsVal)),
	)

	// Handle different parameter formats
	return extractParameters(paramsVal, logger)
}

// decodeMsgpackMap decodes msgpack-encoded bytes into a map.
func decodeMsgpackMap(data []byte, logger *slog.Logger) (map[string]any, error) {
	var paramMap map[string]any
	if err := msgpack.Decode(data, &paramMap); err != nil {
		logger.Warn("Failed to decode parameters as map",
			"error", err,
			"metadata_len", len(data),
		)
		return nil, err
	}

	// Log map keys for debugging
	mapKeys := make([]string, 0, len(paramMap))
	for k := range paramMap {
		mapKeys = append(mapKeys, k)
	}
	logger.Debug("Decoded parameter map", "keys", mapKeys)

	return paramMap, nil
}

// extractParameters extracts parameter array from various formats.
func extractParameters(paramsVal any, logger *slog.Logger) []any {
	switch v := paramsVal.(type) {
	case []byte:
		return extractParamsFromBytes(v, logger)
	case []any:
		return extractParamsFromArray(v, logger)
	case string:
		return extractParamsFromArrowIPC(v, logger)
	default:
		logger.Warn("Parameters field has unexpected type",
			"type", fmt.Sprintf("%T", paramsVal),
		)
		return []any{}
	}
}

// extractParamsFromBytes decodes msgpack-encoded parameter bytes.
func extractParamsFromBytes(paramsBytes []byte, logger *slog.Logger) []any {
	logger.Debug("Parameters is bytes, attempting msgpack decode", "len", len(paramsBytes))

	var paramArray []any
	if err := msgpack.Decode(paramsBytes, &paramArray); err != nil {
		logger.Warn("Failed to decode parameters bytes as msgpack array", "error", err)
		return []any{}
	}

	logger.Debug("Decoded parameters from bytes",
		"param_count", len(paramArray),
		"params", paramArray,
	)
	return paramArray
}

// extractParamsFromArray returns the parameter array directly.
func extractParamsFromArray(paramsArray []any, logger *slog.Logger) []any {
	logger.Debug("Extracted parameters array",
		"param_count", len(paramsArray),
		"params", paramsArray,
	)
	return paramsArray
}

// extractParamsFromArrowIPC decodes Arrow IPC-encoded parameter string.
func extractParamsFromArrowIPC(paramsStr string, logger *slog.Logger) []any {
	logger.Debug("Parameters is string, treating as Arrow IPC", "len", len(paramsStr))

	paramsBytes := []byte(paramsStr)
	paramReader, err := ipc.NewReader(bytes.NewReader(paramsBytes))
	if err != nil {
		logger.Warn("Failed to create IPC reader from string", "error", err)
		return []any{}
	}
	defer paramReader.Release()

	if !paramReader.Next() {
		return []any{}
	}

	paramRecord := paramReader.RecordBatch()
	params := make([]any, paramRecord.NumCols())
	for i := 0; i < int(paramRecord.NumCols()); i++ {
		col := paramRecord.Column(i)
		if col.Len() > 0 {
			params[i] = extractScalarValue(col, 0)
		} else {
			params[i] = nil
		}
	}

	logger.Debug("Decoded parameters from Arrow IPC string",
		"param_count", len(params),
		"params", params,
	)
	return params
}
