#include "stdafx.h"
#include "row.h"

#include <core/misc/varint.h>

#include <util/stream/str.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;

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
    int lhsLength = std::min(lhs.GetValueCount(), prefixLength);
    int rhsLength = std::min(rhs.GetValueCount(), prefixLength);
    int minLength = std::min(lhsLength, rhsLength);
    for (int index = 0; index < minLength; ++index) {
        int result = CompareRowValues(lhs[index], rhs[index]);
        if (result != 0) {
            return result;
        }
    }
    return lhsLength - rhsLength;
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

/*
void FromProto(TOwningRow* row, const NChunkClient::NProto::TKey& protoKey)
{
    size_t fixedSize = sizeof (TRowHeader) + sizeof (TRowValue) * protoKey.parts_size();
    auto rowData = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
    auto* header = reinterpret_cast<TRowHeader*>(rowData.Begin());

    header->ValueCount = static_cast<i32>(protoKey.parts_size());
    header->Deleted = false;
    header->Timestamp = NullTimestamp;

    Stroka stringData;

    auto* values = reinterpret_cast<TRowValue*>(header + 1);
    for (int index = 0; index < protoKey.parts_size(); ++index) {
        auto& value = values[index];

        // No idea, what name table to use, and what column names really are.
        value.Id = -1;

        auto& keyPart = protoKey.parts(index);
        switch (keyPart.type()) {
            case EKeyPartType::Null:
                value.Type = EValueType::Null;
                break;

            case EKeyPartType::Integer:
                value.Type = EValueType::Integer;
                value.Data.Integer = keyPart.int_value();
                break;

            case EKeyPartType::Double:
                value.Type = EValueType::Double;
                value.Data.Double = keyPart.double_value();
                break;

            case EKeyPartType::String:
                value.Type = EValueType::String;
                // Remember offset, we cannot store pointer right now,
                // because stringData may be reallocated.
                value.Data.Integer = stringData.length();
                value.Length = keyPart.str_value().length();
                stringData.append(keyPart.str_value());
                break;

            case EKeyPartType::MinSentinel:
                value.Type = EValueType::Min;
                break;

            case EKeyPartType::MaxSentinel:
                value.Type = EValueType::Max;
                break;

            default:
                YUNREACHABLE();
        }
    }

    for (int index = 0; index < protoKey.parts_size(); ++index) {
        auto& value = values[index];
        if (value.Type == EValueType::String) {
            // Convert offset to pointer.
            value.Data.String = ~stringData + value.Data.Integer;
        }
    }

    *row = TOwningRow(std::move(rowData), stringData);
}
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

