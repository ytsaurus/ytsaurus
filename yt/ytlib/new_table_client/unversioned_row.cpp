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

////////////////////////////////////////////////////////////////////////////////

void Save(TStreamSaveContext& context, const TUnversionedValue& value)
{
    auto* output = context.GetOutput();
    if (value.Type == EValueType::String || value.Type == EValueType::Any) {
        output->Write(&value, sizeof (ui16) + sizeof (ui16) + sizeof (ui32)); // Id, Type, Length
        if (value.Length != 0) {
            output->Write(value.Data.String, value.Length);
        }
    } else {
        output->Write(&value, sizeof (TUnversionedValue));
    }
}

void Load(TStreamLoadContext& context, TUnversionedValue& value, TChunkedMemoryPool* pool)
{
    auto* input = context.GetInput();
    const size_t fixedSize = sizeof (ui16) + sizeof (ui16) + sizeof (ui32); // Id, Type, Length
    YCHECK(input->Load(&value, fixedSize) == fixedSize); 
    if (value.Type == EValueType::String || value.Type == EValueType::Any) {
        if (value.Length != 0) {
            value.Data.String = pool->Allocate(value.Length);
            YCHECK(input->Load(const_cast<char*>(value.Data.String), value.Length) == value.Length);
        } else {
            value.Data.String = nullptr;
        }
    } else {
        YCHECK(input->Load(&value.Data, sizeof (value.Data)) == sizeof (value.Data));
    }
}

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            return Sprintf("<%s>", ~ToString(EValueType(value.Type)));

        case EValueType::Integer:
            return Sprintf("%" PRId64 "i", value.Data.Integer);

        case EValueType::Double:
            return Sprintf("%lfd", value.Data.Double);

        case EValueType::String:
        case EValueType::Any:
            // TODO(babenko): handle Any separately
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
            const auto maximum = std::numeric_limits<i64>::max();
            if (LIKELY(inner != maximum)) {
                ++inner;
            } else {
                value.Type = EValueType::Max;
            }
            break;
        }

        case EValueType::Double: {
            auto& inner = value.Data.Double;
            const auto maximum = std::numeric_limits<double>::max();
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
            const auto maximum = std::numeric_limits<i64>::max();
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
            const auto maximum = std::numeric_limits<double>::max();
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

int CompareRows(
    const TUnversionedValue* lhsBegin,
    const TUnversionedValue* lhsEnd,
    const TUnversionedValue* rhsBegin,
    const TUnversionedValue* rhsEnd)
{
    auto* lhsCurrent = lhsBegin;
    auto* rhsCurrent = rhsBegin;
    while (lhsCurrent != lhsEnd && rhsCurrent != rhsEnd) {
        int result = CompareRowValues(*lhsCurrent++, *rhsCurrent++);
        if (result != 0) {
            return result;
        }
    }
    return (lhsEnd - lhsBegin) - (rhsEnd - rhsBegin);
}

int CompareRows(TUnversionedRow lhs, TUnversionedRow rhs, int prefixLength)
{
    if (!lhs && !rhs) {
        return 0;
    }

    if (lhs && !rhs) {
        return +1;
    }

    if (!lhs && rhs) {
        return -1;
    }

    return CompareRows(
        lhs.Begin(),
        lhs.Begin() + std::min(lhs.GetCount(), prefixLength),
        rhs.Begin(),
        rhs.Begin() + std::min(rhs.GetCount(), prefixLength));
}

bool operator == (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) == 0;
}

bool operator != (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) != 0;
}

bool operator <= (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) <= 0;
}

bool operator < (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) < 0;
}

bool operator >= (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) >= 0;
}

bool operator > (TUnversionedRow lhs, TUnversionedRow rhs)
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

TUnversionedRow TUnversionedRow::Allocate(TChunkedMemoryPool* alignedPool, int valueCount)
{
    auto* header = reinterpret_cast<TUnversionedRowHeader*>(alignedPool->Allocate(GetUnversionedRowDataSize(valueCount)));
    header->Count = valueCount;
    header->Padding = 0;
    return TUnversionedRow(header);
}

////////////////////////////////////////////////////////////////////////////////

TOwningKey GetKeySuccessorImpl(TKey key, int prefixLength, EValueType sentinelType)
{
    TUnversionedOwningRowBuilder builder(key.GetCount() + 1);
    for (int index = 0; index < prefixLength; ++index) {
        builder.AddValue(key[index]);
    }
    builder.AddValue(MakeUnversionedSentinelValue(sentinelType));
    return builder.GetRowAndReset();
}

TOwningKey GetKeySuccessor(TKey key)
{
    return GetKeySuccessorImpl(
        key,
        key.GetCount(),
        EValueType::Min);
}

TOwningKey GetKeyPrefixSuccessor(TKey key, int prefixLength)
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
    return builder.GetRowAndReset();
}

static const TOwningKey CachedMinKey = MakeSentinelKey(EValueType::Min);
static const TOwningKey CachedMaxKey = MakeSentinelKey(EValueType::Max);

const TOwningKey MinKey()
{
    return CachedMinKey;
}

const TOwningKey MaxKey()
{
    return CachedMaxKey;
}

static TOwningKey MakeEmptyKey()
{
    TUnversionedOwningRowBuilder builder;
    return builder.GetRowAndReset();
}

static const TOwningKey CachedEmptyKey = MakeEmptyKey();

const TOwningKey EmptyKey()
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

static Stroka SerializedNullRow("");

Stroka SerializeToString(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    int size = 2 * MaxVarUInt32Size; // header size
    for (auto* it = begin; it != end; ++it) {
        size += GetByteSize(*it);
    }

    Stroka buffer;
    buffer.resize(size);

    char* current = const_cast<char*>(buffer.data());
    current += WriteVarUInt32(current, 0); // format version
    current += WriteVarUInt32(current, static_cast<ui32>(std::distance(begin, end)));

    for (auto* it = begin; it != end; ++it) {
        current += WriteValue(current, *it);
    }

    buffer.resize(current - buffer.data());

    return buffer;
}

Stroka SerializeToString(TUnversionedRow row)
{
    return row
        ? SerializeToString(row.Begin(), row.End())
        : SerializedNullRow;
}

TUnversionedOwningRow DeserializeFromString(const Stroka& data)
{
    if (data == SerializedNullRow) {
        return TUnversionedOwningRow();
    }

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

void ToProto(
    TProtoStringType* protoRow,
    const TUnversionedValue* begin,
    const TUnversionedValue* end)
{
    *protoRow = SerializeToString(begin, end);
}

void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow)
{
    *row = DeserializeFromString(protoRow);
}

Stroka ToString(TUnversionedRow row)
{
    return row
        ? "[" + JoinToString(row.Begin(), row.End()) + "]"
        : "<Null>";
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

    *row = rowBuilder.GetRowAndReset();
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
    key = builder.GetRowAndReset();
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

TUnversionedRowBuilder::TUnversionedRowBuilder(int initialValueCapacity /*= 16*/)
{
    ValueCapacity_ = initialValueCapacity;
    RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));
    Reset();
}

void TUnversionedRowBuilder::AddValue(const TUnversionedValue& value)
{
    if (GetHeader()->Count == ValueCapacity_) {
        ValueCapacity_ = 2 * std::max(1, ValueCapacity_);
        RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));
    }

    auto* header = GetHeader();
    *GetValue(header->Count) = value;
    ++header->Count;
}

TUnversionedRow TUnversionedRowBuilder::GetRow()
{
    return TUnversionedRow(GetHeader());
}

void TUnversionedRowBuilder::Reset()
{
    auto* header = GetHeader();
    header->Count = 0;   
}

TUnversionedRowHeader* TUnversionedRowBuilder::GetHeader()
{
    return reinterpret_cast<TUnversionedRowHeader*>(RowData_.Begin());
}

TUnversionedValue* TUnversionedRowBuilder::GetValue(int index)
{
    return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1) + index;
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRowBuilder::TUnversionedOwningRowBuilder(int initialValueCapacity /*= 16*/)
    : InitialValueCapacity_(initialValueCapacity)
{
    Reset();
}

void TUnversionedOwningRowBuilder::AddValue(const TUnversionedValue& value)
{
    if (GetHeader()->Count == ValueCapacity_) {
        ValueCapacity_ *= 2;
        RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));
    }

    auto* header = GetHeader();
    auto* newValue = GetValue(header->Count);
    *newValue = value;

    if (value.Type == EValueType::String || value.Type == EValueType::Any) {
        if (StringData_.length() + value.Length > StringData_.capacity()) {
            char* oldStringData = const_cast<char*>(StringData_.begin());
            StringData_.reserve(std::max(
                StringData_.capacity() * 2,
                StringData_.length() + value.Length));
            char* newStringData = const_cast<char*>(StringData_.begin());
            for (int index = 0; index < header->Count; ++index) {
                auto* existingValue = GetValue(index);
                if (existingValue->Type == EValueType::String || existingValue->Type == EValueType::Any) {
                    existingValue->Data.String = newStringData + (existingValue->Data.String - oldStringData);
                }
            }
        }
        newValue->Data.String = const_cast<char*>(StringData_.end());
        StringData_.append(value.Data.String, value.Data.String + value.Length);
    }

    ++header->Count;
}

TUnversionedOwningRow TUnversionedOwningRowBuilder::GetRowAndReset()
{
    auto row = TUnversionedOwningRow(
        TSharedRef::FromBlob<TOwningRowTag>(std::move(RowData_)),
        std::move(StringData_));
    Reset();
    return row;
}


void TUnversionedOwningRowBuilder::Reset()
{
    ValueCapacity_ = InitialValueCapacity_;
    RowData_.Resize(GetUnversionedRowDataSize(ValueCapacity_));

    auto* header = GetHeader();
    header->Count = 0;   
}

TUnversionedRowHeader* TUnversionedOwningRowBuilder::GetHeader()
{
    return reinterpret_cast<TUnversionedRowHeader*>(RowData_.Begin());
}

TUnversionedValue* TUnversionedOwningRowBuilder::GetValue(int index)
{
    return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1) + index;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

