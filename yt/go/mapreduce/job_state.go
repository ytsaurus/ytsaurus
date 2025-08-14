package mapreduce

import (
	"encoding/gob"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
)

// jobState is transferred from the client to the job.
//
// NOTE: all fields must be public and support gob encoding.
type jobState struct {
	Job any

	InputTablesInfo  []TableInfo
	OutputTablesInfo []TableInfo
}

// TableInfo contains metadata information for input or output table.
type TableInfo struct {
	SkiffSchema *skiff.Schema

	// May contain nil values if schema is weak or unknown.
	TableSchema *schema.Schema
}

func init() {
	gob.Register(schema.TypeBoolean)
	gob.Register(schema.TypeInt8)
	gob.Register(schema.TypeInt16)
	gob.Register(schema.TypeInt32)
	gob.Register(schema.TypeInt64)
	gob.Register(schema.TypeUint8)
	gob.Register(schema.TypeUint16)
	gob.Register(schema.TypeUint32)
	gob.Register(schema.TypeUint64)
	gob.Register(schema.TypeFloat32)
	gob.Register(schema.TypeFloat64)
	gob.Register(schema.TypeBytes)
	gob.Register(schema.TypeString)
	gob.Register(schema.TypeAny)
	gob.Register(schema.TypeDate)
	gob.Register(schema.TypeDatetime)
	gob.Register(schema.TypeTimestamp)
	gob.Register(schema.TypeInterval)

	gob.Register(schema.Decimal{})
	gob.Register(schema.Optional{})
	gob.Register(schema.List{})
	gob.Register(schema.Struct{})
	gob.Register(schema.Tuple{})
	gob.Register(schema.Variant{})
	gob.Register(schema.Dict{})
	gob.Register(schema.Tagged{})
	gob.Register(schema.StructMember{})
	gob.Register(schema.TupleElement{})
}
