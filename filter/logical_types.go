package filter

// LogicalTypeID identifies DuckDB data types.
type LogicalTypeID string

const (
	TypeIDInvalid      LogicalTypeID = "INVALID"
	TypeIDSQLNull      LogicalTypeID = "SQLNULL"
	TypeIDUnknown      LogicalTypeID = "UNKNOWN"
	TypeIDAny          LogicalTypeID = "ANY"
	TypeIDBoolean      LogicalTypeID = "BOOLEAN"
	TypeIDTinyInt      LogicalTypeID = "TINYINT"
	TypeIDSmallInt     LogicalTypeID = "SMALLINT"
	TypeIDInteger      LogicalTypeID = "INTEGER"
	TypeIDBigInt       LogicalTypeID = "BIGINT"
	TypeIDDate         LogicalTypeID = "DATE"
	TypeIDTime         LogicalTypeID = "TIME"
	TypeIDTimestampSec LogicalTypeID = "TIMESTAMP_SEC"
	TypeIDTimestampMs  LogicalTypeID = "TIMESTAMP_MS"
	TypeIDTimestamp    LogicalTypeID = "TIMESTAMP"
	TypeIDTimestampNs  LogicalTypeID = "TIMESTAMP_NS"
	TypeIDDecimal      LogicalTypeID = "DECIMAL"
	TypeIDFloat        LogicalTypeID = "FLOAT"
	TypeIDDouble       LogicalTypeID = "DOUBLE"
	TypeIDChar         LogicalTypeID = "CHAR"
	TypeIDVarchar      LogicalTypeID = "VARCHAR"
	TypeIDBlob         LogicalTypeID = "BLOB"
	TypeIDInterval     LogicalTypeID = "INTERVAL"
	TypeIDUTinyInt     LogicalTypeID = "UTINYINT"
	TypeIDUSmallInt    LogicalTypeID = "USMALLINT"
	TypeIDUInteger     LogicalTypeID = "UINTEGER"
	TypeIDUBigInt      LogicalTypeID = "UBIGINT"
	TypeIDTimestampTZ  LogicalTypeID = "TIMESTAMP_TZ"
	TypeIDTimeTZ       LogicalTypeID = "TIME_TZ"
	TypeIDHugeInt      LogicalTypeID = "HUGEINT"
	TypeIDUHugeInt     LogicalTypeID = "UHUGEINT"
	TypeIDUUID         LogicalTypeID = "UUID"
	TypeIDStruct       LogicalTypeID = "STRUCT"
	TypeIDList         LogicalTypeID = "LIST"
	TypeIDMap          LogicalTypeID = "MAP"
	TypeIDEnum         LogicalTypeID = "ENUM"
	TypeIDArray        LogicalTypeID = "ARRAY"
)

// typeIDMapping maps DuckDB full type names to normalized short names.
// DuckDB may send either the short form (e.g., "TIMESTAMP_TZ") or
// the full SQL form (e.g., "TIMESTAMP WITH TIME ZONE").
var typeIDMapping = map[LogicalTypeID]LogicalTypeID{
	// Timestamp types - full SQL names
	"TIMESTAMP WITH TIME ZONE":    TypeIDTimestampTZ,
	"TIMESTAMP_TZ":                TypeIDTimestampTZ,
	"TIMESTAMPTZ":                 TypeIDTimestampTZ,
	"TIME WITH TIME ZONE":         TypeIDTimeTZ,
	"TIMETZ":                      TypeIDTimeTZ,
	"TIMESTAMP_S":                 TypeIDTimestampSec,
	"TIMESTAMP_SEC":               TypeIDTimestampSec,
	"TIMESTAMP_MS":                TypeIDTimestampMs,
	"TIMESTAMP_NS":                TypeIDTimestampNs,
	"TIMESTAMP WITHOUT TIME ZONE": TypeIDTimestamp,
	// Integer types - aliases
	"INT":     TypeIDInteger,
	"INT4":    TypeIDInteger,
	"INT8":    TypeIDBigInt,
	"INT2":    TypeIDSmallInt,
	"INT1":    TypeIDTinyInt,
	"UINT8":   TypeIDUBigInt,
	"UINT4":   TypeIDUInteger,
	"UINT2":   TypeIDUSmallInt,
	"UINT1":   TypeIDUTinyInt,
	"INT128":  TypeIDHugeInt,
	"UINT128": TypeIDUHugeInt,
	// Float types - aliases
	"FLOAT4": TypeIDFloat,
	"FLOAT8": TypeIDDouble,
	"REAL":   TypeIDFloat,
	// String types - aliases
	"STRING": TypeIDVarchar,
	"TEXT":   TypeIDVarchar,
	// Boolean aliases
	"BOOL": TypeIDBoolean,
}

// Normalize returns the canonical LogicalTypeID for the given type ID.
// This handles DuckDB type aliases and full SQL names.
func (t LogicalTypeID) Normalize() LogicalTypeID {
	if mapped, ok := typeIDMapping[t]; ok {
		return mapped
	}
	return t
}

// LogicalType represents DuckDB logical types with optional extra type information.
type LogicalType struct {
	ID       LogicalTypeID `json:"id"`
	TypeInfo ExtraTypeInfo `json:"type_info"`
}

// ExtraTypeInfo is the interface for additional type information.
type ExtraTypeInfo interface {
	extraTypeInfoMarker()
}

// DecimalTypeInfo contains precision and scale for DECIMAL types.
type DecimalTypeInfo struct {
	Type  string `json:"type"` // "DECIMAL_TYPE_INFO"
	Alias string `json:"alias"`
	Width int    `json:"width"` // Total digits
	Scale int    `json:"scale"` // Decimal places
}

func (d *DecimalTypeInfo) extraTypeInfoMarker() {}

// ListTypeInfo contains element type for LIST types.
type ListTypeInfo struct {
	Type      string      `json:"type"` // "LIST_TYPE_INFO"
	Alias     string      `json:"alias"`
	ChildType LogicalType `json:"child_type"`
}

func (l *ListTypeInfo) extraTypeInfoMarker() {}

// StructTypeInfo contains field definitions for STRUCT types.
type StructTypeInfo struct {
	Type       string        `json:"type"` // "STRUCT_TYPE_INFO"
	Alias      string        `json:"alias"`
	ChildTypes []StructField `json:"child_types"`
}

func (s *StructTypeInfo) extraTypeInfoMarker() {}

// StructField represents a field in a STRUCT type.
type StructField struct {
	Name string      `json:"first"`
	Type LogicalType `json:"second"`
}

// ArrayTypeInfo contains element type and size for fixed-size ARRAY types.
type ArrayTypeInfo struct {
	Type      string      `json:"type"` // "ARRAY_TYPE_INFO"
	Alias     string      `json:"alias"`
	ChildType LogicalType `json:"child_type"`
	Size      int         `json:"size"`
}

func (a *ArrayTypeInfo) extraTypeInfoMarker() {}

// EnumTypeInfo contains enum type information.
type EnumTypeInfo struct {
	Type   string   `json:"type"` // "ENUM_TYPE_INFO"
	Alias  string   `json:"alias"`
	Values []string `json:"values"`
}

func (e *EnumTypeInfo) extraTypeInfoMarker() {}

// MapTypeInfo contains key and value types for MAP types.
type MapTypeInfo struct {
	Type      string      `json:"type"` // "MAP_TYPE_INFO"
	Alias     string      `json:"alias"`
	KeyType   LogicalType `json:"key_type"`
	ValueType LogicalType `json:"value_type"`
}

func (m *MapTypeInfo) extraTypeInfoMarker() {}

// Value represents a typed constant value.
type Value struct {
	Type   LogicalType `json:"type"`
	IsNull bool        `json:"is_null"`
	Data   any         `json:"value"` // Type-specific data
}

// HugeInt represents a 128-bit signed integer.
type HugeInt struct {
	Upper int64  `json:"upper"`
	Lower uint64 `json:"lower"`
}

// UHugeInt represents a 128-bit unsigned integer.
type UHugeInt struct {
	Upper uint64 `json:"upper"`
	Lower uint64 `json:"lower"`
}

// Interval represents a time interval.
type Interval struct {
	Months int32 `json:"months"`
	Days   int32 `json:"days"`
	Micros int64 `json:"micros"`
}

// ListValue represents a list/array value.
type ListValue struct {
	Children []Value `json:"children"`
}

// StructValue represents a struct value.
type StructValue struct {
	Children []Value `json:"children"`
}

// MapValue represents a map value.
type MapValue struct {
	Keys   []Value `json:"keys"`
	Values []Value `json:"values"`
}

// Base64String represents a non-UTF8 string encoded as base64.
type Base64String struct {
	Base64 string `json:"base64"`
}

// IsNumeric returns true if the type is a numeric type.
func (t LogicalTypeID) IsNumeric() bool {
	switch t {
	case TypeIDTinyInt, TypeIDSmallInt, TypeIDInteger, TypeIDBigInt,
		TypeIDUTinyInt, TypeIDUSmallInt, TypeIDUInteger, TypeIDUBigInt,
		TypeIDHugeInt, TypeIDUHugeInt, TypeIDFloat, TypeIDDouble, TypeIDDecimal:
		return true
	}
	return false
}

// IsInteger returns true if the type is an integer type.
func (t LogicalTypeID) IsInteger() bool {
	switch t {
	case TypeIDTinyInt, TypeIDSmallInt, TypeIDInteger, TypeIDBigInt,
		TypeIDUTinyInt, TypeIDUSmallInt, TypeIDUInteger, TypeIDUBigInt,
		TypeIDHugeInt, TypeIDUHugeInt:
		return true
	}
	return false
}

// IsSigned returns true if the type is a signed integer type.
func (t LogicalTypeID) IsSigned() bool {
	switch t {
	case TypeIDTinyInt, TypeIDSmallInt, TypeIDInteger, TypeIDBigInt, TypeIDHugeInt:
		return true
	}
	return false
}

// IsUnsigned returns true if the type is an unsigned integer type.
func (t LogicalTypeID) IsUnsigned() bool {
	switch t {
	case TypeIDUTinyInt, TypeIDUSmallInt, TypeIDUInteger, TypeIDUBigInt, TypeIDUHugeInt:
		return true
	}
	return false
}

// IsTemporal returns true if the type is a date/time type.
func (t LogicalTypeID) IsTemporal() bool {
	switch t {
	case TypeIDDate, TypeIDTime, TypeIDTimeTZ,
		TypeIDTimestamp, TypeIDTimestampTZ, TypeIDTimestampMs, TypeIDTimestampNs, TypeIDTimestampSec,
		TypeIDInterval:
		return true
	}
	return false
}

// IsString returns true if the type is a string type.
func (t LogicalTypeID) IsString() bool {
	switch t {
	case TypeIDVarchar, TypeIDChar, TypeIDBlob:
		return true
	}
	return false
}

// IsComplex returns true if the type is a complex/nested type.
func (t LogicalTypeID) IsComplex() bool {
	switch t {
	case TypeIDList, TypeIDStruct, TypeIDMap, TypeIDArray:
		return true
	}
	return false
}
