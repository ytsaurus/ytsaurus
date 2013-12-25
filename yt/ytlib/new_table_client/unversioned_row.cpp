#include "stdafx.h"
#include "unversioned_row.h"

#include <ytlib/table_client/private.h>

#include <core/misc/varint.h>
#include <core/misc/string.h>

#include <core/yson/consumer.h>

#include <core/ytree/node.h>
#include <core/ytree/attribute_helpers.h>

#include <util/stream/str.h>

#include <cmath>

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
    const char* current = input;

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
            return Sprintf("{%s}", ~EValueType(value.Type).ToString());

        case EValueType::Integer:
            return Sprintf("%" PRId64 "i", value.Data.Integer);

        case EValueType::Double:
            return Sprintf("%lfd", value.Data.Double);

        case EValueType::String:
        case EValueType::Any:
            return Stroka(value.Data.String, value.Length).Quote();

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
            double lhsValue = lhs.Data.Double;
            double rhsValue = rhs.Data.Double;
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

        // NB: Cannot actually compare composite values.
        case EValueType::Any:
            return 0;

        // NB: All singleton types are equal.
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
            return 0;

        default:
            YUNREACHABLE();
    }
}

void AdvanceToValueSuccessor(TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Integer: {
            auto& inner = value.Data.Integer;
            constexpr const auto maximum = std::numeric_limits<i64>::max();
            if (LIKELY(inner != maximum)) {
                ++inner;
            } else {
                value.Type = EValueType::Max;
            }
            break;
        }

        case EValueType::Double: {
            auto& inner = value.Data.Double;
            constexpr const auto maximum = std::numeric_limits<double>::max();
            if (LIKELY(inner != maximum)) {
                inner = std::nextafter(inner, maximum);
            } else {
                value.Type = EValueType::Max;
            }
            break;
        }

        case EValueType::String:
            // TODO(sandello): A proper way to get a successor is to append
            // zero byte. However, it requires us to mess with underlying
            // memory storage. I do not want to deal with it right now.
            YUNIMPLEMENTED();

        default:
            YUNREACHABLE();
    }
}

bool IsValueSuccessor(
    const TUnversionedValue& value,
    const TUnversionedValue& successor)
{
    switch (value.Type) {
        case EValueType::Integer: {
            const auto& inner = value.Data.Integer;
            constexpr const auto maximum = std::numeric_limits<i64>::max();
            if (LIKELY(inner != maximum)) {
                return
                    successor.Type == EValueType::Integer &&
                    successor.Data.Integer == inner + 1;
            } else {
                return
                    successor.Type == EValueType::Max;
            }
            break;
        }

        case EValueType::Double: {
            const auto& inner = value.Data.Double;
            constexpr const auto maximum = std::numeric_limits<double>::max();
            if (LIKELY(inner != maximum)) {
                return
                    successor.Type == EValueType::Double &&
                    successor.Data.Double == std::nextafter(inner, maximum);
            } else {
                return
                    successor.Type == EValueType::Max;
            }
        }

        case EValueType::String:
            return
                successor.Type == EValueType::String &&
                successor.Length == value.Length + 1 &&
                successor.Data.String[successor.Length - 1] == 0 &&
                ::memcmp(successor.Data.String, value.Data.String, value.Length) == 0;
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
    return CompareRows(lhs, rhs) == 0;
}

bool operator != (const TUnversionedRow& lhs, const TUnversionedRow& rhs)
{
    return CompareRows(lhs, rhs) != 0;
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

int CompareRows(const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs, int prefixLength)
{
    return CompareRows(lhs.Get(), rhs.Get(), prefixLength);
}

bool operator == (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) == 0;
}

bool operator != (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) != 0;
}

bool operator <= (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) <= 0;
}

bool operator < (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) < 0;
}

bool operator >= (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) >= 0;
}

bool operator > (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) > 0;
}

void ResetRowValues(TUnversionedRow* row)
{
    for (int index = 0; index < row->GetCount(); ++index) {
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

size_t GetHash(TUnversionedRow row)
{
    size_t result = 0xdeadc0de;
    int partCount = row.GetCount();
    for (int i = 0; i < row.GetCount(); ++i) {
        result = (result * 1000003) ^ GetHash(row[i]);
    }
    return result ^ partCount;
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
        key.GetCount(),
        EValueType::Min);
}

TOwningKey GetKeyPrefixSuccessor(const TOwningKey& key, int prefixLength)
{
    YASSERT(prefixLength <= key.GetCount());
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

TOwningKey MinKey()
{
    return CachedMinKey;
}

TOwningKey MaxKey()
{
    return CachedMaxKey;
}

static TOwningKey MakeEmptyKey()
{
    TUnversionedOwningRowBuilder builder;
    return builder.Finish();
}

static TOwningKey CachedEmptyKey = MakeEmptyKey();

TOwningKey EmptyKey()
{
    return CachedEmptyKey;
}

const TOwningKey& ChooseMinKey(const TOwningKey& a, const TOwningKey& b)
{
    int result = CompareRows(a, b);
    return result <= 0 ? a : b;
}

const TOwningKey& ChooseMaxKey(const TOwningKey& a, const TOwningKey& b)
{
    int result = CompareRows(a, b);
    return result >= 0 ? a : b;
}

////////////////////////////////////////////////////////////////////////////////

Stroka SerializeToString(TUnversionedRow row)
{
    int size = 2 * MaxVarUInt32Size; // header size
    for (int i = 0; i < row.GetCount(); ++i) {
        size += GetByteSize(row[i]);
    }

    Stroka buffer;
    buffer.resize(size);

    char* current = const_cast<char*>(buffer.data());
    current += WriteVarUInt32(current, 0); // format version
    current += WriteVarUInt32(current, static_cast<ui32>(row.GetCount()));

    for (int i = 0; i < row.GetCount(); ++i) {
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

    size_t fixedSize = GetUnversionedRowDataSize(valueCount);
    auto rowData = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
    auto* header = reinterpret_cast<TUnversionedRowHeader*>(rowData.Begin());

    header->Count = static_cast<i32>(valueCount);

    auto* values = reinterpret_cast<TUnversionedValue*>(header + 1);
    for (int index = 0; index < valueCount; ++index) {
        TUnversionedValue* value = values + index;
        current += ReadValue(current, value);
    }

    return TUnversionedOwningRow(std::move(rowData), data);
}

void ToProto(TProtoStringType* protoRow, TUnversionedRow row)
{
    *protoRow = SerializeToString(row);
}

void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row)
{
    ToProto(protoRow, row.Get());
}

void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow)
{
    *row = DeserializeFromString(protoRow);
}

Stroka ToString(TUnversionedRow row)
{
    return "[" + JoinToString(row.Begin(), row.End()) + "]";
}

Stroka ToString(const TUnversionedOwningRow& row)
{
    return ToString(row.Get());
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

void Serialize(const TKey& key, IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    for (int index = 0; index < key.GetCount(); ++index) {
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

void Serialize(const TOwningKey& key, IYsonConsumer* consumer)
{
    return Serialize(key.Get(), consumer);
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

void TUnversionedOwningRow::Save(TStreamSaveContext& context) const
{
    ::NYT::Save(context, SerializeToString(Get()));
}

void TUnversionedOwningRow::Load(TStreamLoadContext& context)
{
    Stroka data;
    ::NYT::Load(context, data);
    *this = DeserializeFromString(data);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

