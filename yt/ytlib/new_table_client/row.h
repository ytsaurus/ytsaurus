#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

#ifdef _MSC_VER
    #define PACK
#else
    #define PACK __attribute__((aligned(16), packed))
#endif

struct TRowValue
{
    //! Column id obtained from a name table.
    ui16 Id;
    //! Column type (compact EColumnType).
    ui16 Type;
    //! Length of variable-sized value (meaningful only for |String| and |Any| types).
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

    static FORCED_INLINE TRowValue MakeNull(int id)
    {
        TRowValue result;
        result.Id = id;
        result.Type = EColumnType::Null;
        return result;
    }

    static FORCED_INLINE TRowValue MakeInteger(int id, i64 value)
    {
        TRowValue result;
        result.Id = id;
        result.Type = EColumnType::Integer;
        result.Data.Integer = value;
        return result;
    }

    static FORCED_INLINE TRowValue MakeDouble(int id, double value)
    {
        TRowValue result;
        result.Id = id;
        result.Type = EColumnType::Double;
        result.Data.Double = value;
        return result;
    }

    static FORCED_INLINE TRowValue MakeString(int id, const TStringBuf& value)
    {
        TRowValue result;
        result.Id = id;
        result.Type = EColumnType::String;
        result.Length = value.length();
        result.Data.String = value.begin();
        return result;
    }

    static FORCED_INLINE TRowValue MakeAny(int id, const TStringBuf& value)
    {
        TRowValue result;
        result.Id = id;
        result.Type = EColumnType::Any;
        result.Length = value.length();
        result.Data.String = value.begin();
        return result;
    }
} PACK;

#undef PACK

static_assert(sizeof(TRowValue) == 16, "TRowValue has to be exactly 16 bytes.");

////////////////////////////////////////////////////////////////////////////////

//! Ternary comparison predicate for TRowValue-s.
//! Returns zero, positive or negative value depending on the outcome.
int CompareRowValues(TRowValue lhs, TRowValue rhs);

//! Same as #CompareRowValues but presumes that the values are of the same type.
int CompareSameTypeValues(TRowValue lhs, TRowValue rhs);

////////////////////////////////////////////////////////////////////////////////

//! Header which precedes row values in memory layout.
struct TRowHeader
{
    i32 ValueCount;
    bool Deleted;
    TTimestamp Timestamp;
};

static_assert(sizeof(TRowHeader) == 16, "TRowHeader has to be exactly 16 bytes.");

////////////////////////////////////////////////////////////////////////////////

//! A lightweight wrapper around TRowHeader*.
class TRow
{
public:
    FORCED_INLINE explicit TRow(TRowHeader* header)
        : Header(header)
    { }

    FORCED_INLINE TRow(
        TChunkedMemoryPool* pool, 
        int valueCount,
        bool deleted = false,
        TTimestamp timestamp = NullTimestamp)
        : Header(reinterpret_cast<TRowHeader*>(
            pool->Allocate(sizeof(TRowHeader) + valueCount * sizeof(TRowValue))))
    {
        Header->ValueCount = valueCount;
        Header->Deleted = deleted;
        Header->Timestamp = timestamp;
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

    FORCED_INLINE TRowValue& operator[](int index)
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<TRowValue*>(Header + 1)[index];
    }

    FORCED_INLINE const TRowValue& operator[](int index) const
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<TRowValue*>(Header + 1)[index];
    }

    FORCED_INLINE int GetValueCount() const
    {
        return Header->ValueCount;
    }

    FORCED_INLINE bool GetDeleted() const
    {
        return Header->Deleted;
    }

    FORCED_INLINE void SetDeleted(bool deleted = true)
    {
        Header->Deleted = deleted;
    }

    FORCED_INLINE TTimestamp GetTimestamp() const
    {
        return Header->Timestamp;
    }

    FORCED_INLINE void SetTimestamp(TTimestamp timestamp)
    {
        Header->Timestamp = timestamp;
    }

private:
    TRowHeader* Header;

};

static_assert(sizeof (TRow) == sizeof (intptr_t), "TRow has to be exactly sizeof (intptr_t) bytes.");

////////////////////////////////////////////////////////////////////////////////

void ToProto(TProtoStringType* protoRow, const TOwningRow& row);
void FromProto(TOwningRow* row, const TProtoStringType& protoRow);

//! An immutable owning version of TRow.
/*!
 *  Instances of TRow are lightweight ref-counted handles.
 *  Fixed part is stored in a (shared) blob.
 *  Variable part is stored in a (shared) string.
 */
class TOwningRow
{
public:
    FORCED_INLINE TOwningRow()
    { }

    TOwningRow(TRow other);

    FORCED_INLINE explicit operator bool()
    {
        return static_cast<bool>(RowData);
    }

    FORCED_INLINE int GetValueCount() const
    {
        const auto* header = GetHeader();
        return header ? header->ValueCount : 0;
    }

    FORCED_INLINE bool GetDeleted() const
    {
        const auto* header = GetHeader();
        return header ? header->Deleted : false;
    }

    FORCED_INLINE TTimestamp GetTimestamp() const
    {
        const auto* header = GetHeader();
        return header ? header->Timestamp : NullTimestamp;
    }

    FORCED_INLINE const TRowValue& operator[](int index) const
    {
        YASSERT(index >= 0 && index < GetValueCount());
        return reinterpret_cast<const TRowValue*>(GetHeader() + 1)[index];
    }

    FORCED_INLINE operator TRow () const
    {
        return TRow(const_cast<TRowHeader*>(GetHeader()));
    }

private:
    friend void ToProto(TProtoStringType* protoRow, const TOwningRow& row);
    friend void FromProto(TOwningRow* row, const TProtoStringType& protoRow);

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

    TSharedRef RowData; // TRowHeader plus TRowValue-s
    Stroka StringData;  // Holds the string data

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
