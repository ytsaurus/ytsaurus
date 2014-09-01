#include "stdafx.h"
#include "unversioned_row.h"

#include <util/stream/str.h>

#include <core/misc/varint.h>
#include <core/misc/string.h>

#include <core/yson/consumer.h>

#include <core/ytree/node.h>
#include <core/ytree/attribute_helpers.h>

#include <ytlib/table_client/private.h>

#include <ytlib/new_table_client/row_buffer.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schema.h>

#include <cmath>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

bool IsIntegralType(EValueType type)
{
    return type == EValueType::Int64 || type == EValueType::Uint64;
}

bool IsArithmeticType(EValueType type)
{
    return IsIntegralType(type) || type == EValueType::Double;
}

EValueType InferCommonType(EValueType lhsType, EValueType rhsType, TStringBuf expression /*= TStringBuf()*/)
{
    if (lhsType == rhsType) {
        return lhsType;
    } else if (IsArithmeticType(lhsType) && IsArithmeticType(rhsType)) {
        return std::max(lhsType, rhsType);
    } else {
        THROW_ERROR_EXCEPTION("Types in expression %Qv are incompatible", expression)
            << TErrorAttribute("lhs_type", ToString(lhsType))
            << TErrorAttribute("rhs_type", ToString(rhsType));
    }
}

////////////////////////////////////////////////////////////////////////////////


int GetByteSize(const TUnversionedValue& value)
{
    int result = MaxVarUint32Size * 2; // id and type

    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;

        case EValueType::Int64:
        case EValueType::Uint64:
            result += MaxVarInt64Size;
            break;

        case EValueType::Double:
            result += sizeof(double);
            break;

        case EValueType::Boolean:
            result += 1;
            break;

        case EValueType::String:
        case EValueType::Any:
            result += MaxVarUint32Size + value.Length;
            break;

        default:
            YUNREACHABLE();
    }

    return result;
}

int GetDataWeight(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            return 0;

        case EValueType::Int64:
            return sizeof(i64);

        case EValueType::Uint64:
            return sizeof(ui64);

        case EValueType::Double:
            return sizeof(double);

        case EValueType::Boolean:
            return 1;

        case EValueType::String:
        case EValueType::Any:
            return value.Length;

        default:
            YUNREACHABLE();
    }
}

int WriteValue(char* output, const TUnversionedValue& value)
{
    char* current = output;

    current += WriteVarUint32(current, value.Id);
    current += WriteVarUint32(current, value.Type);

    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;

        case EValueType::Int64:
            current += WriteVarInt64(current, value.Data.Int64);
            break;

        case EValueType::Uint64:
            current += WriteVarUint64(current, value.Data.Uint64);
            break;

        case EValueType::Double:
            ::memcpy(current, &value.Data.Double, sizeof (double));
            current += sizeof (double);
            break;

        case EValueType::Boolean:
            *current++ = value.Data.Boolean ? '\x01' : '\x00';
            break;

        case EValueType::String:
        case EValueType::Any:
            current += WriteVarUint32(current, value.Length);
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
    current += ReadVarUint32(current, &id);
    value->Id = static_cast<ui16>(id);

    ui32 type;
    current += ReadVarUint32(current, &type);
    value->Type = static_cast<ui16>(type);

    switch (value->Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;

        case EValueType::Int64:
            current += ReadVarInt64(current, &value->Data.Int64);
            break;

        case EValueType::Uint64:
            current += ReadVarUint64(current, &value->Data.Uint64);
            break;

        case EValueType::Double:
            ::memcpy(&value->Data.Double, current, sizeof (double));
            current += sizeof (double);
            break;

        case EValueType::Boolean:
            value->Data.Boolean = (*current) == 1;
            current += 1;
            break;

        case EValueType::String:
        case EValueType::Any:
            current += ReadVarUint32(current, &value->Length);
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
            return Format("<%v>", EValueType(value.Type));

        case EValueType::Int64:
            return Format("%vi", value.Data.Int64);

        case EValueType::Uint64:
            return Format("%vu", value.Data.Uint64);

        case EValueType::Double:
            return Format("%v", value.Data.Double);

        case EValueType::Boolean:
            return Format("%v", value.Data.Boolean);

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
        case EValueType::Int64: {
            auto lhsValue = lhs.Data.Int64;
            auto rhsValue = rhs.Data.Int64;
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
        }

        case EValueType::Uint64: {
            auto lhsValue = lhs.Data.Uint64;
            auto rhsValue = rhs.Data.Uint64;
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

        case EValueType::Boolean: {
            bool lhsValue = lhs.Data.Boolean;
            bool rhsValue = rhs.Data.Boolean;
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

        // NB: All sentinel types are equal.
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
            return 0;

        default:
            YUNREACHABLE();
    }
}

bool operator == (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) == 0;
}

bool operator != (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) != 0;
}

bool operator <= (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) <= 0;
}

bool operator < (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) < 0;
}

bool operator >= (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) >= 0;
}

bool operator > (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) > 0;
}

TUnversionedValue GetValueSuccessor(TUnversionedValue value, TRowBuffer* rowBuffer)
{
    auto unalignedPool = rowBuffer->GetUnalignedPool();

    switch (value.Type) {
        case EValueType::Int64: {
            auto& inner = value.Data.Int64;
            const auto maximum = std::numeric_limits<i64>::max();
            if (LIKELY(inner != maximum)) {
                ++inner;
            } else {
                value.Type = EValueType::Max;
            }
            break;
        }

        case EValueType::Uint64: {
            auto& inner = value.Data.Uint64;
            const auto maximum = std::numeric_limits<ui64>::max();
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

        case EValueType::Boolean: {
            auto& inner = value.Data.Boolean;
            if (inner) {
                value.Type = EValueType::Max;
            } else {
                value.Data.Boolean = true;
            }
            break;
        }

        case EValueType::String: {
            char* newValue = unalignedPool->AllocateUnaligned(value.Length + 1);
            memcpy(newValue, value.Data.String, value.Length);
            newValue[value.Length] = 0;
            value.Data.String = newValue;
            break;
        }

        default:
            YUNREACHABLE();
    }

    return value;
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

        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
            // These types are aliased.
            return (value.Data.Int64 & 0xffff) + 17 * (value.Data.Int64 >> 32);

        case EValueType::Boolean:
            return value.Data.Boolean;

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

i64 GetDataWeight(TUnversionedRow row)
{
    return std::accumulate(
        row.Begin(),
        row.End(),
        0ll,
        [] (i64 x, const TUnversionedValue& value) {
            return GetDataWeight(value) + x;
        });
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

static void ValidateValueLength(const TUnversionedValue& value)
{
    if (value.Type == EValueType::String ||
        value.Type == EValueType::Any)
    {
        if (value.Length > MaxStringValueLength) {
            THROW_ERROR_EXCEPTION("Value is too long: length %" PRId64 ", limit %" PRId64,
                static_cast<i64>(value.Length),
                MaxStringValueLength);
        }
    }
}

void ValidateDataValue(const TUnversionedValue& value)
{
    ValidateDataValueType(EValueType(value.Type));
    ValidateValueLength(value);
}

void ValidateKeyValue(const TUnversionedValue& value)
{
    ValidateKeyValueType(EValueType(value.Type));
    ValidateValueLength(value);
}

void ValidateRowValueCount(int count)
{
    if (count < 0) {
        THROW_ERROR_EXCEPTION("Negative number of values in row");
    }
    if (count > MaxValuesPerRow) {
        THROW_ERROR_EXCEPTION("Too many values in row: actual %d, limit %d",
            count,
            MaxValuesPerRow);
    }
}

void ValidateKeyColumnCount(int count)
{
    if (count < 1) {
        THROW_ERROR_EXCEPTION("Non-positive number of key columns");
    }
    if (count > MaxKeyColumnCount) {
        THROW_ERROR_EXCEPTION("Too many columns in key: actual %d, limit %d",
            count,
            MaxKeyColumnCount);
    }
}

void ValidateRowCount(int count)
{
    if (count < 0) {
        THROW_ERROR_EXCEPTION("Negative number of rows in rowset");
    }
    if (count > MaxRowsPerRowset) {
        THROW_ERROR_EXCEPTION("Too many rows in rowset: actual %d, limit %d",
            count,
            MaxRowsPerRowset);
    }
}

void ValidateClientDataRow(
    TUnversionedRow row,
    int keyColumnCount,
    const TNameTableToSchemaIdMapping& idMapping,
    const TTableSchema& schema)
{
    ValidateRowValueCount(row.GetCount());

    bool keyColumnFlags[MaxKeyColumnCount] = {};
    int keyColumnSeen = 0;
    for (const auto* it = row.Begin(); it != row.End(); ++it) {
        const auto& value = *it;
        ValidateDataValue(value);
        int valueId = value.Id;
        if (valueId >= idMapping.size()) {
            THROW_ERROR_EXCEPTION("Invalid column id: actual %v, expected in range [0,%v]",
                valueId,
                idMapping.size() - 1);
        }
        int schemaId = idMapping[valueId];
        if (schemaId < keyColumnCount) {
            if (keyColumnFlags[schemaId]) {
                THROW_ERROR_EXCEPTION("Duplicate key component with id %d",
                    schemaId);
            }
            keyColumnFlags[schemaId] = true;
            ++keyColumnSeen;
        }
        if (value.Type != EValueType::Null && value.Type != schema.Columns()[schemaId].Type) {
            THROW_ERROR_EXCEPTION("Invalid type of column %Qv: expected %Qlv or %Qlv but got %Qlv",
                schema.Columns()[schemaId].Name,
                schema.Columns()[schemaId].Type,
                EValueType(EValueType::Null),
                EValueType(value.Type));
        }
    }

    if (keyColumnSeen != keyColumnCount) {
        THROW_ERROR_EXCEPTION("Some key components are missing: actual %d, expected %d",
            keyColumnSeen,
            keyColumnCount);
    }
}

void ValidateServerDataRow(
    TUnversionedRow row,
    int keyColumnCount,
    const TTableSchema& schema)
{
    ValidateRowValueCount(row.GetCount());

    int schemaColumnCount = schema.Columns().size();
    for (int index = 0; index < row.GetCount(); ++index) {
        const auto& value = row[index];
        ValidateDataValue(value);
        int id = value.Id;
        if (index < keyColumnCount) {
            if (id != index) {
                THROW_ERROR_EXCEPTION("Invalid key component id: actual %d, expected %d",
                    id,
                    index);
            }
        } else {
            if (id < keyColumnCount) {
                THROW_ERROR_EXCEPTION("Misplaced key component: id %d, position %d",
                    id,
                    index);
            }
            if (id >= schemaColumnCount) {
                THROW_ERROR_EXCEPTION("Invalid column id: actual %v, expected in range [0,%v]",
                    id,
                    schemaColumnCount - 1);
            }
            if (value.Type != EValueType::Null && value.Type != schema.Columns()[id].Type) {
                THROW_ERROR_EXCEPTION("Invalid type of column %Qv: expected %Qlv or %Qlv but got %Qlv",
                    schema.Columns()[id].Name,
                    schema.Columns()[id].Type,
                    EValueType(EValueType::Null),
                    EValueType(value.Type));
            }
        }
    }
}

void ValidateClientKey(
    TKey key,
    int keyColumnCount,
    const TTableSchema& schema)
{
    if (!key) {
        THROW_ERROR_EXCEPTION("Key cannot be null");
    }

    if (key.GetCount() != keyColumnCount) {
        THROW_ERROR_EXCEPTION("Invalid number of key components: expected %d, actual %d",
            keyColumnCount,
            key.GetCount());
    }

    bool keyColumnFlags[MaxKeyColumnCount] = {};
    for (int index = 0; index < keyColumnCount; ++index) {
        const auto& value = key[index];
        ValidateKeyValue(value);
        int id = value.Id;
        if (id >= keyColumnCount) {
            THROW_ERROR_EXCEPTION("Invalid value id: actual %d, expected in range [0, %d]",
                id,
                keyColumnCount - 1);
        }
        if (keyColumnFlags[id]) {
            THROW_ERROR_EXCEPTION("Duplicate key component with id %d",
                id);
        }
        if (value.Type != EValueType::Null && value.Type != schema.Columns()[id].Type) {
            THROW_ERROR_EXCEPTION("Invalid type of column %Qv: expected %Qlv or %Qlv but got %Qlv",
                schema.Columns()[id].Name,
                schema.Columns()[id].Type,
                EValueType(EValueType::Null),
                EValueType(value.Type));
        }
        keyColumnFlags[id] = true;
    }
}

void ValidateServerKey(
    TKey key,
    int keyColumnCount,
    const TTableSchema& schema)
{
    if (!key) {
        THROW_ERROR_EXCEPTION("Key cannot be null");
    }

    if (key.GetCount() != keyColumnCount) {
        THROW_ERROR_EXCEPTION("Invalid number of key components: expected %d, actual %d",
                keyColumnCount,
                key.GetCount());
    }

    for (int index = 0; index < keyColumnCount; ++index) {
        const auto& value = key[index];
        ValidateKeyValue(value);
        int id = value.Id;
        if (id != index) {
            THROW_ERROR_EXCEPTION("Invalid key component id: actual %d, expected %d",
                id,
                index);
        }
        if (value.Type != EValueType::Null && value.Type != schema.Columns()[id].Type) {
            THROW_ERROR_EXCEPTION("Invalid type of column %Qv: expected %Qlv or %Qlv but got %Qlv",
                schema.Columns()[id].Name,
                schema.Columns()[id].Type,
                EValueType(EValueType::Null),
                EValueType(value.Type));
        }
    }
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
    int size = 2 * MaxVarUint32Size; // header size
    for (auto* it = begin; it != end; ++it) {
        size += GetByteSize(*it);
    }

    Stroka buffer;
    buffer.resize(size);

    char* current = const_cast<char*>(buffer.data());
    current += WriteVarUint32(current, 0); // format version
    current += WriteVarUint32(current, static_cast<ui32>(std::distance(begin, end)));

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

    const char* current = data.data();

    ui32 version;
    current += ReadVarUint32(current, &version);
    YCHECK(version == 0);

    ui32 valueCount;
    current += ReadVarUint32(current, &valueCount);

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

            case EKeyPartType::Int64:
                rowBuilder.AddValue(MakeUnversionedInt64Value(keyPart.int64_value(), id));
                break;

            case EKeyPartType::Uint64:
                rowBuilder.AddValue(MakeUnversionedUint64Value(keyPart.uint64_value(), id));
                break;

            case EKeyPartType::Double:
                rowBuilder.AddValue(MakeUnversionedDoubleValue(keyPart.double_value(), id));
                break;

            case EKeyPartType::Boolean:
                rowBuilder.AddValue(MakeUnversionedBooleanValue(keyPart.boolean_value(), id));
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
            case EValueType::Int64:
                consumer->OnInt64Scalar(value.Data.Int64);
                break;

            case EValueType::Uint64:
                consumer->OnUint64Scalar(value.Data.Uint64);
                break;

            case EValueType::Double:
                consumer->OnDoubleScalar(value.Data.Double);
                break;

            case EValueType::Boolean:
                consumer->OnBooleanScalar(value.Data.Boolean);
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
            case ENodeType::Int64:
                builder.AddValue(MakeUnversionedInt64Value(item->GetValue<i64>(), id));
                break;

            case ENodeType::Uint64:
                builder.AddValue(MakeUnversionedUint64Value(item->GetValue<ui64>(), id));
                break;
            
            case ENodeType::Double:
                builder.AddValue(MakeUnversionedDoubleValue(item->GetValue<double>(), id));
                break;

            case ENodeType::Boolean:
                builder.AddValue(MakeUnversionedBooleanValue(item->GetValue<bool>(), id));
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
                THROW_ERROR_EXCEPTION("Key cannot contain %Qv components",
                    item->GetType());
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
    RowData_.resize(GetUnversionedRowDataSize(ValueCapacity_));
    Reset();
}

void TUnversionedRowBuilder::AddValue(const TUnversionedValue& value)
{
    if (GetHeader()->Count == ValueCapacity_) {
        ValueCapacity_ = 2 * std::max(1, ValueCapacity_);
        RowData_.resize(GetUnversionedRowDataSize(ValueCapacity_));
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
    return reinterpret_cast<TUnversionedRowHeader*>(RowData_.data());
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

int TUnversionedOwningRowBuilder::AddValue(const TUnversionedValue& value)
{
    if (GetHeader()->Count == ValueCapacity_) {
        ValueCapacity_ = ValueCapacity_ == 0 ? 1 : ValueCapacity_ * 2;
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

    return header->Count++;
}

TUnversionedValue* TUnversionedOwningRowBuilder::BeginValues()
{
    return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1);
}

TUnversionedValue* TUnversionedOwningRowBuilder::EndValues()
{
    return BeginValues() + GetHeader()->Count;
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

void TUnversionedOwningRow::Init(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    int count = std::distance(begin, end);

    size_t fixedSize = GetUnversionedRowDataSize(count);
    RowData_ = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
    auto* header = GetHeader();

    header->Count = count;
    header->Padding = 0;
    ::memcpy(header + 1, begin, reinterpret_cast<const char*>(end) - reinterpret_cast<const char*>(begin));

    size_t variableSize = 0;
    for (auto it = begin; it != end; ++it) {
        const auto& otherValue = *it;
        if (otherValue.Type == EValueType::String || otherValue.Type == EValueType::Any) {
            variableSize += otherValue.Length;
        }
    }

    if (variableSize > 0) {
        StringData_.resize(variableSize);
        char* current = const_cast<char*>(StringData_.data());

        for (int index = 0; index < count; ++index) {
            const auto& otherValue = begin[index];
            auto& value = reinterpret_cast<TUnversionedValue*>(header + 1)[index];;
            if (otherValue.Type == EValueType::String || otherValue.Type == EValueType::Any) {
                ::memcpy(current, otherValue.Data.String, otherValue.Length);
                value.Data.String = current;
                current += otherValue.Length;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow BuildRow(
    const Stroka& yson,
    const TKeyColumns& keyColumns,
    const TTableSchema& tableSchema,
    bool treatMissingAsNull /*= true*/)
{
    auto nameTable = TNameTable::FromSchema(tableSchema);

    auto rowParts = ConvertTo<yhash_map<Stroka, INodePtr>>(
        TYsonString(yson, EYsonType::MapFragment));

    TUnversionedOwningRowBuilder rowBuilder;
    auto addValue = [&] (int id, INodePtr value) {
        switch (value->GetType()) {
            case ENodeType::Int64:
                rowBuilder.AddValue(MakeUnversionedInt64Value(value->GetValue<i64>(), id));
                break;
            case ENodeType::Uint64:
                rowBuilder.AddValue(MakeUnversionedUint64Value(value->GetValue<ui64>(), id));
                break;
            case ENodeType::Double:
                rowBuilder.AddValue(MakeUnversionedDoubleValue(value->GetValue<double>(), id));
                break;
            case ENodeType::Boolean:
                rowBuilder.AddValue(MakeUnversionedBooleanValue(value->GetValue<bool>(), id));
                break;
            case ENodeType::String:
                rowBuilder.AddValue(MakeUnversionedStringValue(value->GetValue<Stroka>(), id));
                break;
            default:
                rowBuilder.AddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).Data(), id));
                break;
        }
    };

    // Key
    for (int id = 0; id < static_cast<int>(keyColumns.size()); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        YCHECK(it != rowParts.end());
        addValue(id, it->second);
    }

    // Fixed values
    for (int id = static_cast<int>(keyColumns.size()); id < static_cast<int>(tableSchema.Columns().size()); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        if (it != rowParts.end()) {
            addValue(id, it->second);
        } else if (treatMissingAsNull) {
            rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
        }
    }

    // Variable values
    for (const auto& pair : rowParts) {
        int id = nameTable->GetIdOrRegisterName(pair.first);
        if (id >= tableSchema.Columns().size()) {
            addValue(id, pair.second);
        }
    }

    return rowBuilder.GetRowAndReset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

