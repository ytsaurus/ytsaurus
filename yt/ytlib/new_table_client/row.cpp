#include "stdafx.h"
#include "row.h"

#include <core/misc/varint.h>
#include <core/misc/string.h>

#include <core/yson/consumer.h>

#include <core/ytree/node.h>
#include <core/ytree/attribute_helpers.h>

#include <util/stream/str.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

int CompareRowValues(const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    if (UNLIKELY(lhs.Type != rhs.Type)) {
        return lhs.Type - rhs.Type;
    }

    switch (lhs.Type) {
        case EValueType::Integer: {
            auto lhsValue = lhs.Data.Integer;
            auto rhsValue = rhs.Data.Integer;
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
        }

        case EValueType::Double: {
            auto lhsValue = lhs.Data.Double;
            auto rhsValue = lhs.Data.Double;
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
        }

        case EValueType::String: {
            size_t lhsLength = lhs.Length;
            size_t rhsLength = rhs.Length;
            size_t minLength = std::min(lhsLength, rhsLength);
            int result = ::memcmp(lhs.Data.String, rhs.Data.String, minLength);
            if (result == 0) {
                if (lhsLength < rhsLength) {
                    return -1;
                } else if (lhsLength > rhsLength) {
                    return +1;
                } else {
                    return 0;
                }
            } else {
                return result;
            }
        }

        case EValueType::Any:
            return 0; // NB: Cannot actually compare composite values.

        case EValueType::Null:
            return 0;

        default:
            YUNREACHABLE();
    }
}

int CompareRows(TUnversionedRow lhs, TUnversionedRow rhs, int prefixLength)
{
    TKeyComparer comparer(prefixLength);
    return comparer(lhs, rhs);
}

size_t GetHash(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::String:
            return TStringBuf(value.Data.String, value.Length).hash();

        case EValueType::Integer:
        case EValueType::Double:
            // Integer and Double are aliased.
            return (value.Data.Integer & 0xffff) + 17 * (value.Data.Integer >> 32);

        default:
            // No idea how to hash other types.
            return 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

TOwningKey GetKeySuccessorImpl(const TOwningKey& key, int prefixLength, EValueType sentinelType)
{
    auto rowData = TSharedRef::Allocate<TOwningRowTag>(GetRowDataSize<TUnversionedValue>(prefixLength + 1), false);
    ::memcpy(rowData.Begin(), key.RowData.Begin(), GetRowDataSize<TUnversionedValue>(prefixLength));
    TKey result(reinterpret_cast<TRowHeader*>(rowData.Begin()));
    result[prefixLength] = MakeSentinelValue<TUnversionedValue>(sentinelType);
    return TOwningKey(std::move(rowData), key.StringData);
}

TOwningKey GetKeySuccessor(const TOwningKey& key)
{
    return GetKeySuccessorImpl(
        key,
        key.GetValueCount(),
        EValueType::Min);
}

TOwningKey GetKeyPrefixSuccessor(const TOwningKey& key, int prefixLength)
{
    YASSERT(prefixLength <= key.GetValueCount());
    return GetKeySuccessorImpl(
        key,
        prefixLength,
        EValueType::Max);
}

////////////////////////////////////////////////////////////////////////////////

static TOwningKey MakeSentinelKey(EValueType type)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedSentinelValue(type, 0));
    return builder.Finish();
}

static TOwningKey CachedMinKey = MakeSentinelKey(EValueType::Min);
static TOwningKey CachedMaxKey = MakeSentinelKey(EValueType::Max);

TKey MinKey()
{
    return CachedMinKey;
}

TKey MaxKey()
{
    return CachedMinKey;
}

static TOwningKey MakeEmptyKey()
{
    TUnversionedOwningRowBuilder builder;
    return builder.Finish();
}

static TOwningKey CachedEmptyKey = MakeEmptyKey();

TKey EmptyKey()
{
    return CachedEmptyKey;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row)
{
    size_t variableSize = 0;
    for (int index = 0; index < row.GetValueCount(); ++index) {
        const auto& otherValue = row[index];
        if (otherValue.Type == EValueType::String || otherValue.Type == EValueType::Any) {
            variableSize += otherValue.Length;
        }
    }

    Stroka buffer;
    buffer.resize(
        64 +                           // encoded TRowHeader  (approx)
        16 * row.GetValueCount() +     // encoded TUnversionedValue-s (approx)
        variableSize);                 // strings
    char* current = const_cast<char*>(buffer.data());

    current += WriteVarUInt32(current, 0); // format version
    current += WriteVarUInt32(current, static_cast<ui32>(row.GetValueCount()));

    for (int index = 0; index < row.GetValueCount(); ++index) {
        const auto& value = row[index];
        current += WriteVarUInt32(current, value.Id);
        current += WriteVarUInt32(current, value.Type);
        switch (value.Type) {
            case EValueType::Null:
                break;

            case EValueType::Integer:
                current += WriteVarInt64(current, value.Data.Integer);
                break;
            
            case EValueType::Double:
                ::memcpy(current, &value.Data.Double, sizeof (double));
                current += sizeof (double);
                break;
            
            case EValueType::String:
            case EValueType::Any:
                current += WriteVarUInt32(current, value.Length);
                ::memcpy(current, value.Data.String, value.Length);
                current += value.Length;
                break;

            default:
                YUNREACHABLE();
        }
    }

    buffer.resize(current - buffer.data());
    *protoRow = buffer;
}

void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow)
{
    char* current = const_cast<char*>(protoRow.data());

    ui32 version;
    current += ReadVarUInt32(current, &version);
    YCHECK(version == 0);

    ui32 valueCount;
    current += ReadVarUInt32(current, &valueCount);
    
    size_t fixedSize = GetRowDataSize<TUnversionedValue>(valueCount);
    auto rowData = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
    auto* header = reinterpret_cast<TRowHeader*>(rowData.Begin());
    
    header->ValueCount = static_cast<i32>(valueCount);

    auto* values = reinterpret_cast<TUnversionedValue*>(header + 1);
    for (int index = 0; index < valueCount; ++index) {
        auto& value = values[index];
        
        ui32 id;
        current += ReadVarUInt32(current, &id);
        YCHECK(id <= std::numeric_limits<ui16>::max());
        value.Id = static_cast<ui16>(id);
        
        ui32 type;
        current += ReadVarUInt32(current, &type);
        YCHECK(type <= std::numeric_limits<ui16>::max());
        value.Type = static_cast<ui16>(type);

        switch (value.Type) {
            case EValueType::Null:
                break;

            case EValueType::Integer:
                current += ReadVarInt64(current, &value.Data.Integer);
                break;
            
            case EValueType::Double:
                ::memcpy(&value.Data.Double, current, sizeof (double));
                current += sizeof (double);
                break;
            
            case EValueType::String:
            case EValueType::Any:
                current += ReadVarUInt32(current, &value.Length);
                value.Data.String = current;
                current += value.Length;
                break;

            default:
                YUNREACHABLE();
        }
    }

    *row = TUnversionedOwningRow(std::move(rowData), protoRow);
}

void Serialize(TKey key, IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    for (int index = 0; index < key.GetValueCount(); ++index) {
        consumer->OnListItem();
        const auto& value = key[index];
        if (value.Id != index) {
            THROW_ERROR_EXCEPTION("Invalid key component id: expected %d, found %d",
                index,
                static_cast<int>(value.Id));
        }
        auto type = EValueType(value.Type);
        switch (type) {
            case EValueType::Integer:
                consumer->OnIntegerScalar(value.Data.Integer);
                break;

            case EValueType::Double:
                consumer->OnDoubleScalar(value.Data.Double);
                break;

            case EValueType::String:
                consumer->OnStringScalar(TStringBuf(value.Data.String, value.Length));
                break;

            case EValueType::Any:
                THROW_ERROR_EXCEPTION("Key cannot contain \"any\" components");
                break;

            default:
                consumer->OnBeginAttributes();
                consumer->OnKeyedItem("type");
                consumer->OnStringScalar(FormatEnum(type));
                consumer->OnEndAttributes();
                break;
        }
    }
    consumer->OnEndList();
}

void Deserialize(TOwningKey& key, INodePtr node)
{
    if (node->GetType() != ENodeType::List) {
        THROW_ERROR_EXCEPTION("Key can only be parsed from a list");
    }

    TUnversionedOwningRowBuilder builder;
    int id = 0;
    for (const auto& item : node->AsList()->GetChildren()) {
        switch (item->GetType()) {
            case ENodeType::Integer:
                builder.AddValue(MakeUnversionedIntegerValue(item->GetValue<i64>(), id));
                break;
            
            case ENodeType::Double:
                builder.AddValue(MakeUnversionedDoubleValue(item->GetValue<double>(), id));
                break;
            
            case ENodeType::String:
                builder.AddValue(MakeUnversionedStringValue(item->GetValue<Stroka>(), id));
                break;
            
            case ENodeType::Entity: {
                auto valueType = item->Attributes().Get<EValueType>("type");
                builder.AddValue(MakeUnversionedSentinelValue(valueType, id));
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Key cannot contain %s components",
                    ~FormatEnum(item->GetType()).Quote());
        }
        ++id;
    }
    key = builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

