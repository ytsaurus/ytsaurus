#include "stdafx.h"
#include "unversioned_row.h"

#include <ytlib/table_client/private.h>

#include <core/misc/varint.h>
#include <core/misc/string.h>

#include <core/yson/consumer.h>

#include <core/ytree/node.h>
#include <core/ytree/attribute_helpers.h>

#include <util/stream/str.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

int GetByteSize(const TUnversionedValue& value)
{
    int result = MaxVarUInt32Size * 2; // id and type

    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;

        case EValueType::Integer:
            result += MaxVarInt64Size;
            break;

        case EValueType::Double:
            result += sizeof(double);
            break;

        case EValueType::String:
        case EValueType::Any:
            result += MaxVarUInt32Size + value.Length;
            break;

        default:
            YUNREACHABLE();
    }

    return result;
}

int WriteValue(char* output, const TUnversionedValue& value)
{
    char* current = output;
    current += WriteVarUInt32(current, value.Id);
    current += WriteVarUInt32(current, value.Type);
    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
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
    return current - output;
}

int ReadValue(const char* input, TUnversionedValue* value)
{
    char* current = const_cast<char *>(input);
    ui32 id;
    current += ReadVarUInt32(current, &id);
    value->Id = static_cast<ui16>(id);

    ui32 type;
    current += ReadVarUInt32(current, &type);
    value->Type = static_cast<ui16>(type);

    switch (value->Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;

        case EValueType::Integer:
            current += ReadVarInt64(current, &value->Data.Integer);
            break;

        case EValueType::Double:
            ::memcpy(&value->Data.Double, current, sizeof (double));
            current += sizeof (double);
            break;

        case EValueType::String:
        case EValueType::Any:
            current += ReadVarUInt32(current, &value->Length);
            value->Data.String = current;
            current += value->Length;
            break;

        default:
            YUNREACHABLE();
    }
    return current - input;
}

Stroka ToString(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            return Sprintf("{Type: %s}", ~FormatEnum(EValueType(value.Type)));

        case EValueType::Integer:
           return Sprintf("{Type: %s, Value: %" PRId64 "}",
                ~FormatEnum(EValueType(value.Type)),
                value.Data.Integer);

        case EValueType::Double:
            return Sprintf("{Type: %s, Value: %f}",
                ~FormatEnum(EValueType(value.Type)),
                value.Data.Double);

        case EValueType::String:
        case EValueType::Any:
            return Sprintf("{Type: %s, Value: %s}",
                ~FormatEnum(EValueType(value.Type)),
                value.Data.String);

        default:
            YUNREACHABLE();
    }
}

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

bool operator == (const TUnversionedRow& lhs, const TUnversionedRow& rhs)
{
    return !CompareRows(lhs, rhs);
}

bool operator != (const TUnversionedRow& lhs, const TUnversionedRow& rhs)
{
    return CompareRows(lhs, rhs);
}

bool operator <= (const TUnversionedRow& lhs, const TUnversionedRow& rhs)
{
    return CompareRows(lhs, rhs) <= 0;
}

bool operator < (const TUnversionedRow& lhs, const TUnversionedRow& rhs)
{
    return CompareRows(lhs, rhs) < 0;
}

bool operator >= (const TUnversionedRow& lhs, const TUnversionedRow& rhs)
{
    return CompareRows(lhs, rhs) >= 0;
}

bool operator > (const TUnversionedRow& lhs, const TUnversionedRow& rhs)
{
    return CompareRows(lhs, rhs) > 0;
}

void ResetRowValues(TUnversionedRow* row)
{
    for (int index = 0; index < row->GetValueCount(); ++index) {
        (*row)[index].Type = EValueType::Null;
    }
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

size_t GetUnversionedRowDataSize(int valueCount)
{
    return sizeof(TUnversionedRowHeader) + sizeof(TUnversionedValue) * valueCount;
}

////////////////////////////////////////////////////////////////////////////////

TOwningKey GetKeySuccessorImpl(const TOwningKey& key, int prefixLength, EValueType sentinelType)
{
    TUnversionedOwningRowBuilder builder;
    for (int index = 0; index < prefixLength; ++index) {
        builder.AddValue(key[index]);
    }
    builder.AddValue(MakeUnversionedSentinelValue(sentinelType));
    return builder.Finish();
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
    builder.AddValue(MakeUnversionedSentinelValue(type));
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

Stroka SerializeToString(const TUnversionedRow& row)
{
    int size = 2 * MaxVarUInt32Size; // header size
    for (int i = 0; i < row.GetValueCount(); ++i) {
        size += GetByteSize(row[i]);
    }

    Stroka buffer;
    buffer.resize(size);

    char* current = const_cast<char*>(buffer.data());
    current += WriteVarUInt32(current, 0); // format version
    current += WriteVarUInt32(current, static_cast<ui32>(row.GetValueCount()));
    current += WriteVarUInt32(current, static_cast<ui32>(row.GetKeyCount()));

    for (int i = 0; i < row.GetValueCount(); ++i) {
        current += WriteValue(current, row[i]);
    }
    buffer.resize(current - buffer.data());
    return buffer;
}

TUnversionedOwningRow DeserializeFromString(const Stroka& data)
{
    const char* current = ~data;

    ui32 version;
    current += ReadVarUInt32(current, &version);
    YCHECK(version == 0);

    ui32 valueCount;
    current += ReadVarUInt32(current, &valueCount);

    ui32 keyCount;
    current += ReadVarUInt32(current, &keyCount);

    size_t fixedSize = GetUnversionedRowDataSize(valueCount);
    auto rowData = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
    auto* header = reinterpret_cast<TUnversionedRowHeader*>(rowData.Begin());

    header->ValueCount = static_cast<i32>(valueCount);
    header->KeyCount = static_cast<i16>(keyCount);

    auto* values = reinterpret_cast<TUnversionedValue*>(header + 1);
    for (int index = 0; index < valueCount; ++index) {
        TUnversionedValue* value = values + index;
        current += ReadValue(current, value);
    }

    return TUnversionedOwningRow(std::move(rowData), data);
}

void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row)
{
    *protoRow = SerializeToString(row);
}

void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow)
{
    *row = DeserializeFromString(protoRow);
}

Stroka ToString(const TUnversionedRow& row)
{
    auto begin = &row[0];
    auto* end = begin + row.GetValueCount();
    return JoinToString(begin, end);
}

Stroka ToString(const TUnversionedOwningRow& row)
{
    return ToString(TUnversionedRow(row));
}

void FromProto(TUnversionedOwningRow* row, const NChunkClient::NProto::TKey& protoKey)
{
    TUnversionedOwningRowBuilder rowBuilder(protoKey.parts_size());
    for (int id = 0; id < protoKey.parts_size(); ++id) {
        auto& keyPart = protoKey.parts(id);
        switch (keyPart.type()) {
            case EKeyPartType::Null:
                rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
                break;

            case EKeyPartType::MinSentinel:
                rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Min, id));
                break;

            case EKeyPartType::MaxSentinel:
                rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Max, id));
                break;

            case EKeyPartType::Integer:
                rowBuilder.AddValue(MakeUnversionedIntegerValue(keyPart.int_value(), id));
                break;

            case EKeyPartType::Double:
                rowBuilder.AddValue(MakeUnversionedDoubleValue(keyPart.double_value(), id));
                break;

            case EKeyPartType::String:
                rowBuilder.AddValue(MakeUnversionedStringValue(keyPart.str_value(), id));
                break;

            case EKeyPartType::Composite:
                rowBuilder.AddValue(MakeUnversionedAnyValue(TStringBuf(), id));
                break;

            default:
                YUNREACHABLE();
        }
    }

    *row = rowBuilder.Finish();
}

void Serialize(TKey key, IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    for (int index = 0; index < key.GetValueCount(); ++index) {
        consumer->OnListItem();
        const auto& value = key[index];
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
                consumer->OnEntity();
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

