package schema

import (
	"fmt"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/yson"
)

func ConvertFromRPCProxy(rpcSchema *rpc_proxy.TTableSchema) (Schema, error) {
	schema := Schema{
		Strict:     ptr.Bool(rpcSchema.GetStrict()),
		UniqueKeys: rpcSchema.GetUniqueKeys(),
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
			Name:        col.GetName(),
			Type:        convertType(col.Type),
			ComplexType: complexType,
			Required:    col.GetRequired(),
			Group:       col.GetGroup(),
			Expression:  col.GetExpression(),
			Aggregate:   AggregateFunction(col.GetAggregate()),
			Lock:        col.GetLock(),
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
	case 0x03:
		return TypeInt64
	case 0x04:
		return TypeUint64
	case 0x05:
		return TypeFloat64
	case 0x06:
		return TypeBoolean
	case 0x10:
		return TypeBytes
	case 0x11:
		return TypeAny
	case 0x1000:
		return TypeInt8
	case 0x1001:
		return TypeUint8
	case 0x1003:
		return TypeInt16
	case 0x1004:
		return TypeUint16
	case 0x1005:
		return TypeInt32
	case 0x1006:
		return TypeUint32
	case 0x1007:
		return TypeString
	case 0x1008:
		return TypeDate
	case 0x1009:
		return TypeDatetime
	case 0x100a:
		return TypeTimestamp
	case 0x100b:
		return TypeInterval
	case 0x100d:
		return TypeFloat32
	default:
		return TypeAny
	}
}
