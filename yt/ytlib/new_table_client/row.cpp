#include "stdafx.h"
#include "row.h"

#include <core/misc/varint.h>

#include <util/stream/str.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

int CompareRowValues(TRowValue lhs, TRowValue rhs)
{
    if (LIKELY(lhs.Type == rhs.Type)) {
        return CompareSameTypeValues(lhs, rhs);
    } else {
        return lhs.Type - rhs.Type;
    }
}

int CompareSameTypeValues(TRowValue lhs, TRowValue rhs)
{
    switch (lhs.Type) {
        case EColumnType::Integer: {
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

        case EColumnType::Double: {
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

        case EColumnType::String: {
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

        case EColumnType::Any:
            return 0; // NB: Cannot actually compare composite values.

        case EColumnType::Null:
            return 0;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TOwningRowTag { };

TOwningRow::TOwningRow(TRow other)
{
    if (!other)
        return;

    size_t fixedSize = sizeof (TRowHeader) + sizeof (TRowValue) * other.GetValueCount();
    RowData = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
    ::memcpy(RowData.Begin(), other.GetHeader(), fixedSize);

    size_t variableSize = 0;
    for (int index = 0; index < other.GetValueCount(); ++index) {
        const auto& otherValue = other[index];
        if (otherValue.Type == EColumnType::String || otherValue.Type == EColumnType::Any) {
            variableSize += otherValue.Length;
        }
    }

    StringData.resize(
        64 +                           // encoded TRowHeader  (approx)
        16 * other.GetValueCount() +   // encoded TRowValue-s (approx)
        variableSize);                 // strings

    char* current = const_cast<char*>(StringData.data());

    current += WriteVarUInt32(current, 0); // format version
    current += WriteVarUInt32(current, static_cast<ui32>(other.GetValueCount()));
    current += WriteVarUInt32(current, other.GetDeleted() ? 1 : 0);
    current += WriteVarInt64(current, other.GetTimestamp());

    for (int index = 0; index < other.GetValueCount(); ++index) {
        const auto& value = other[index];
        current += WriteVarUInt32(current, value.Id);
        current += WriteVarUInt32(current, value.Type);
        switch (value.Type) {
            case EColumnType::Null:
                break;

            case EColumnType::Integer:
                current += WriteVarInt64(current, value.Data.Integer);
                break;
            
            case EColumnType::Double:
                ::memcpy(current, &value.Data.Double, sizeof (double));
                current += sizeof (double);
                break;
            
            case EColumnType::String:           
            case EColumnType::Any:
                current += WriteVarUInt32(current, value.Length);
                ::memcpy(current, value.Data.String, value.Length);
                current += value.Length;
                break;

            default:
                YUNREACHABLE();
        }
    }

    StringData.resize(current - StringData.data());
}

void ToProto(TProtoStringType* protoRow, const TOwningRow& row)
{
    *protoRow = row.StringData;
}

void FromProto(TOwningRow* row, const TProtoStringType& protoRow)
{
    char* current = const_cast<char*>(protoRow.data());

    ui32 version;
    current += ReadVarUInt32(current, &version);
    YCHECK(version == 0);

    ui32 valueCount;
    current += ReadVarUInt32(current, &valueCount);
    
    size_t fixedSize = sizeof (TRowHeader) + sizeof (TRowValue) * valueCount;
    auto rowData = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
    auto* header = reinterpret_cast<TRowHeader*>(rowData.Begin());
    
    header->ValueCount = static_cast<i32>(valueCount);

    ui32 deleted;
    current += ReadVarUInt32(current, &deleted);
    YCHECK(deleted == 0 || deleted == 1);
    header->Deleted = (deleted == 1);

    current += ReadVarInt64(current, &header->Timestamp);

    auto* values = reinterpret_cast<TRowValue*>(header + 1);
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
            case EColumnType::Null:
                break;

            case EColumnType::Integer:
                current += ReadVarInt64(current, &value.Data.Integer);
                break;
            
            case EColumnType::Double:
                ::memcpy(&value.Data.Double, current, sizeof (double));
                current += sizeof (double);
                break;
            
            case EColumnType::String:           
            case EColumnType::Any:
                current += ReadVarUInt32(current, &value.Length);
                value.Data.String = current;
                current += value.Length;
                break;

            default:
                YUNREACHABLE();
        }
    }

    *row = TOwningRow(std::move(rowData), protoRow);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

