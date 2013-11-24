#pragma once

#include "public.h"

#include <ytlib/chunk_client/schema.pb.h>
#include <core/misc/chunked_memory_pool.h>
#include <core/misc/varint.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TUnversionedValue
{
    //! Column id obtained from a name table.
    ui16 Id;
    //! Column type from EValueType.
    ui16 Type;
    //! Length of a variable-sized value (only meaningful for |String| and |Any| types).
    ui32 Length;

    union
    {
        //! Integral value.
        i64 Integer;
        //! Floating-point value.
        double Double;
        //! String value for |String| type or YSON-encoded value for |Any| type.
        const char* String;
    } Data;
};

static_assert(
    sizeof(TUnversionedValue) == 16,
    "TUnversionedValue has to be exactly 16 bytes.");
static_assert(
    std::is_pod<TUnversionedValue>::value,
    "TUnversionedValue must be a POD type.");

////////////////////////////////////////////////////////////////////////////////

struct TVersionedValue
    : TUnversionedValue
{
    TTimestamp Timestamp;

    // TODO(sandello): Remove me after migrating to TUnversionedValue in query_client.
    static FORCED_INLINE TVersionedValue MakeInteger(i64 value, int id = 0)
    {
        TVersionedValue result;
        result.Id = id;
        result.Type = EValueType::Integer;
        result.Data.Integer = value;
        result.Timestamp = 0;
        return result;
    }

    // TODO(sandello): Remove me after migrating to TUnversionedValue in query_client.
    static FORCED_INLINE TVersionedValue MakeDouble(double value, int id = 0)
    {
        TVersionedValue result;
        result.Id = id;
        result.Type = EValueType::Integer;
        result.Data.Double = value;
        result.Timestamp = 0;
        return result;
    }

};

static_assert(
    sizeof(TVersionedValue) == 24,
    "TVersionedValue has to be exactly 24 bytes.");

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TValue MakeSentinelValue(EValueType type, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Null;
    return result;
}

template <class TValue>
TValue MakeIntegerValue(i64 value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Integer;
    result.Data.Integer = value;
    return result;
}

template <class TValue>
TValue MakeDoubleValue(double value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Double;
    result.Data.Double = value;
    return result;
}

template <class TValue>
TValue MakeStringValue(const TStringBuf& value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::String;
    result.Length = value.length();
    result.Data.String = value.begin();
    return result;
}

template <class TValue>
TValue MakeAnyValue(const TStringBuf& value, int id = 0)
{
    TValue result;
    result.Id = id;
    result.Type = EValueType::Any;
    result.Length = value.length();
    result.Data.String = value.begin();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

//! Header which precedes row values in memory layout.
struct TRowHeader
{
    ui32 ValueCount;
    ui32 Padding;
};

static_assert(
    sizeof(TRowHeader) == 8,
    "TRowHeader has to be exactly 8 bytes.");

////////////////////////////////////////////////////////////////////////////////

//! Ternary comparison predicate for TUnversionedValue-s.
//! Returns zero, positive or negative value depending on the outcome.
int CompareRowValues(const TUnversionedValue& lhs, const TUnversionedValue& rhs);

//! Ternary comparison predicate for TRow-s stripped to a given number of
//! (leading) values.
int CompareRows(TUnversionedRow lhs, TUnversionedRow rhs, int prefixLength = std::numeric_limits<int>::max());

//! Computes hash for a given TUnversionedValue.
size_t GetHash(const TUnversionedValue& value);

//! Returns the number of bytes needed to store the fixed part of the row (header + values).
template <class TValue>
FORCED_INLINE size_t GetRowDataSize(int valueCount)
{
    return sizeof(TRowHeader) + sizeof(TValue) * valueCount;
}

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TRowHeader* plus an array of values.
template <class TValue>
class TRow
{
public:
    FORCED_INLINE explicit TRow(TRowHeader* header)
        : Header(header)
    { }

    FORCED_INLINE TRow(
        TChunkedMemoryPool* pool, 
        int valueCount)
        : Header(reinterpret_cast<TRowHeader*>(
            pool->Allocate(GetRowDataSize<TUnversionedValue>(valueCount))))
    {
        Header->ValueCount = valueCount;
    }

    FORCED_INLINE explicit operator bool()
    {
        return Header != nullptr;
    }

    FORCED_INLINE TRowHeader* GetHeader()
    {
        return Header;
    }

    FORCED_INLINE const TRowHeader* GetHeader() const
    {
        return Header;
    }

    FORCED_INLINE TValue& operator[](int index)
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<TValue*>(Header + 1)[index];
    }

    FORCED_INLINE const TValue& operator[](int index) const
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<TValue*>(Header + 1)[index];
    }

    FORCED_INLINE int GetValueCount() const
    {
        return Header->ValueCount;
    }

private:
    TRowHeader* Header;

};

static_assert(
    sizeof(TVersionedRow) == sizeof(intptr_t),
    "TVersionedRow size must match that of a pointer.");
static_assert(
    sizeof(TUnversionedRow) == sizeof(intptr_t),
    "TVersionedRow size must match that of a pointer.");

////////////////////////////////////////////////////////////////////////////////

//! A helper used for constructing TRow instances.
/*!
 *  Owns TUnversionedValue array. Does not own the data.
 */
template <class TValue>
class TRowBuilder
{
public:
    explicit TRowBuilder(int capacity = 16)
        : Capacity_(std::max(capacity, 4))
        , Data_(new char[GetRowDataSize<TValue>(Capacity_)])
    {
        auto* header = GetHeader();
        header->ValueCount = 0;
        header->Padding = 0;
    }

    void AddValue(const TValue& value)
    {
        if (GetRow().GetValueCount() == Capacity_) {
            int newCapacity = Capacity_ * 2;
            std::unique_ptr<char[]> newData(new char[GetRowDataSize<TValue>(newCapacity)]);
            ::memcpy(newData.get(), Data_.get(), GetRowDataSize<TValue>(Capacity_));
            std::swap(Data_, newData);
            std::swap(Capacity_, newCapacity);
        }

        auto* header = GetHeader();
        auto row = GetRow();
        row[header->ValueCount++] = value;
    }

    TRow<TValue> GetRow() const
    {
        return TRow<TValue>(GetHeader());
    }

private:
    int Capacity_;
    std::unique_ptr<char[]> Data_;

    TRowHeader* GetHeader() const
    {
        return reinterpret_cast<TRowHeader*>(Data_.get());
    }

};

////////////////////////////////////////////////////////////////////////////////

void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row);
void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow);

//! Returns the successor of |key|, i.e. the key obtained from |key|
// by appending a |EValueType::Min| sentinel.
TOwningKey GetKeySuccessor(const TOwningKey& key);

//! Returns the successor of |key| trimmed to a given length, i.e. the key
//! obtained by triming |key| to |prefixLength| and appending
//! a |EValueType::Max| sentinel.
TOwningKey GetKeyPrefixSuccessor(const TOwningKey& key, int prefixLength);

////////////////////////////////////////////////////////////////////////////////

struct TOwningRowTag { };

//! An immutable owning version of TRow.
/*!
 *  Instances of TOwningRow are lightweight ref-counted handles.
 *  Fixed part is stored in a (shared) blob.
 *  Variable part is stored in a (shared) string.
 */
template <class TValue>
class TOwningRow
{
public:
    FORCED_INLINE TOwningRow()
    { }

    TOwningRow(TRow<TValue> other)
    {
        if (!other)
            return;

        size_t fixedSize = GetRowDataSize<TValue>(other.GetValueCount());
        RowData = TSharedRef::Allocate<TOwningRowTag>(fixedSize, false);
        ::memcpy(RowData.Begin(), other.GetHeader(), fixedSize);

        size_t variableSize = 0;
        for (int index = 0; index < other.GetValueCount(); ++index) {
            const auto& otherValue = other[index];
            if (otherValue.Type == EValueType::String || otherValue.Type == EValueType::Any) {
                variableSize += otherValue.Length;
            }
        }

        StringData.resize(variableSize);
        char* current = const_cast<char*>(StringData.data());

        for (int index = 0; index < other.GetValueCount(); ++index) {
            const auto& value = other[index];
            current += WriteVarUInt32(current, value.Id);
            current += WriteVarUInt32(current, value.Type);
            switch (value.Type) {
            case EValueType::Null:
                break;

            case EValueType::Integer:
                current += WriteVarInt64(current, value.Data.Integer);
                break;

            case EValueType::Double:
                ::memcpy(current, &value.Data.Double, sizeof(double));
                current += sizeof(double);
                break;

            case EValueType::String:
            case EValueType::Any:
                ::memcpy(current, value.Data.String, value.Length);
                current += value.Length;
                break;

            default:
                YUNREACHABLE();
            }
        }
    }

    FORCED_INLINE explicit operator bool()
    {
        return static_cast<bool>(RowData);
    }

    FORCED_INLINE int GetValueCount() const
    {
        const auto* header = GetHeader();
        return header ? static_cast<int>(header->ValueCount) : 0;
    }

    FORCED_INLINE const TUnversionedValue& operator[](int index) const
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<const TUnversionedValue*>(GetHeader() + 1)[index];
    }

    FORCED_INLINE operator TRow<TValue> () const
    {
        return TRow<TValue>(const_cast<TRowHeader*>(GetHeader()));
    }

private:
    friend void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row);
    friend void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow);
    friend TOwningKey GetKeySuccessorImpl(const TOwningKey& key, int prefixLength, EValueType sentinelType);

    TSharedRef RowData; // TRowHeader plus TUnversionedValue-s
    Stroka StringData; // Holds string data

    FORCED_INLINE TOwningRow(TSharedRef rowData, Stroka stringData)
        : RowData(std::move(rowData))
        , StringData(std::move(stringData))
    { }

    FORCED_INLINE TRowHeader* GetHeader()
    {
        return reinterpret_cast<TRowHeader*>(RowData.Begin());
    }

    FORCED_INLINE const TRowHeader* GetHeader() const
    {
        return reinterpret_cast<const TRowHeader*>(RowData.Begin());
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

//! A hasher for TUnversionedValue.
template <>
struct hash<NYT::NVersionedTableClient::TUnversionedValue>
{
    inline size_t operator()(const NYT::NVersionedTableClient::TUnversionedValue& value) const
    {
        return GetHash(value);
    }
};

