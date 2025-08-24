// Package schema defines schema of YT tables.
package schema

import (
	"fmt"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/yson"
)

func ConvertFromRPCProxy(rpcSchema *rpc_proxy.TTableSchema) (Schema, error) {
	schema := Schema{
		Strict:     ptr.Bool(*rpcSchema.Strict),
		UniqueKeys: *rpcSchema.UniqueKeys,
		Columns:    make([]Column, len(rpcSchema.Columns)),
	}

	for i, col := range rpcSchema.Columns {
		var complexType ComplexType
		if len(col.GetTypeV3()) > 0 {
			if err := yson.Unmarshal(col.GetTypeV3(), &complexType); err != nil {
				return Schema{}, fmt.Errorf("failed to unmarshal complex type: %w", err)
			}
		}

		schema.Columns[i] = Column{
			Name:        *col.Name,
			Type:        convertType(col.Type),
			ComplexType: complexType,
			Required:    *col.Required,
			Group:       *col.Group,
			Expression:  *col.Expression,
			Aggregate:   AggregateFunction(*col.Aggregate),
			Lock:        *col.Lock,
			SortOrder:   convertSortOrder(col.SortOrder),
		}
	}

	return schema, nil
}

func convertSortOrder(sortOrder *int32) SortOrder {
	if sortOrder == nil {
		return SortNone
	}

	switch *sortOrder {
	case 0:
		return SortAscending
	case 1:
		return SortDescending
	}

	return SortNone
}

func convertType(valueType *int32) Type {
	if valueType == nil {
		return TypeAny
	}

	// Map RPC Schema enum values to schema.Type
	// Based on ESimpleLogicalValueType from yt/yt/client/table_client/row_base.h
	switch *valueType {
	case 0x02: // Null
		return TypeAny // No direct null type in schema, use Any
	case 0x03: // Int64
		return TypeInt64
	case 0x04: // Uint64
		return TypeUint64
	case 0x05: // Double
		return TypeFloat64
	case 0x06: // Boolean
		return TypeBoolean
	case 0x10: // String
		return TypeBytes
	case 0x11: // Any
		return TypeAny
	case 0x1000: // Int8
		return TypeInt8
	case 0x1001: // Uint8
		return TypeUint8
	case 0x1003: // Int16
		return TypeInt16
	case 0x1004: // Uint16
		return TypeUint16
	case 0x1005: // Int32
		return TypeInt32
	case 0x1006: // Uint32
		return TypeUint32
	case 0x1007: // Utf8
		return TypeString
	case 0x1008: // Date
		return TypeDate
	case 0x1009: // Datetime
		return TypeDatetime
	case 0x100a: // Timestamp
		return TypeTimestamp
	case 0x100b: // Interval
		return TypeInterval
	case 0x100d: // Float
		return TypeFloat32
	default:
		// Unknown type, return Any as fallback
		return TypeAny
	}
}
