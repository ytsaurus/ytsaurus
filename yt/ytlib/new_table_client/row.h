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
    union
    {
        i64         Integer;
        double      Double;
        const char* String;
        const char* Any;     // YSON-encoded value
    } Data;      // Holds the value
    ui32 Length; // For variable-sized values
    ui16 Type;   // EColumnType
    ui16 Index;  // Name Table index

    static FORCED_INLINE TRowValue MakeInteger(int index, i64 value)
    {
        TRowValue result;
        result.Index = index;
        result.Type = NVersionedTableClient::EColumnType::Integer;
        result.Data.Integer = value;
        return result;
    }

    static FORCED_INLINE TRowValue MakeDouble(int index, double value)
    {
        TRowValue result;
        result.Index = index;
        result.Type = NVersionedTableClient::EColumnType::Double;
        result.Data.Double = value;
        return result;
    }

    static FORCED_INLINE TRowValue MakeString(int index, const TStringBuf& value)
    {
        TRowValue result;
        result.Index = index;
        result.Type = NVersionedTableClient::EColumnType::String;
        result.Data.String = value.begin();
        result.Length = value.length();
        return result;
    }

    static FORCED_INLINE TRowValue MakeAny(int index, const TStringBuf& value)
    {
        TRowValue result;
        result.Index = index;
        result.Type = NVersionedTableClient::EColumnType::Any;
        result.Data.Any = value.begin();
        result.Length = value.length();
        return result;
    }
} PACK;

#undef PACK

static_assert(sizeof (TRowValue) == 16, "TRowValue has to be exactly 16 bytes.");

////////////////////////////////////////////////////////////////////////////////

struct TRowHeader
{
    TRowHeader(
        TTimestamp timestamp,
        int valueCount,
        bool deleted)
        : Timestamp(timestamp)
        , ValueCount(valueCount)
        , Deleted(deleted)
    { }

    TTimestamp Timestamp;
    i32 ValueCount;
    bool Deleted;
};

static_assert(sizeof (TRowHeader) == 16, "TRowHeader has to be exactly 16 bytes.");

////////////////////////////////////////////////////////////////////////////////

class TRow
{
public:
    FORCED_INLINE explicit TRow(TRowHeader* rowHeader)
        : RowHeader(rowHeader)
    { }

    FORCED_INLINE TRow(
        TChunkedMemoryPool* pool, 
        int valueCount, 
        TTimestamp timestamp = NullTimestamp, 
        bool deleted = false)
        : RowHeader(reinterpret_cast<TRowHeader*>(pool->Allocate(sizeof(TRowHeader) + valueCount * sizeof(TRowValue))))
    {
        *RowHeader = TRowHeader(timestamp, valueCount, deleted);
    }

    FORCED_INLINE TRowValue& operator[](int index)
    {
        return *reinterpret_cast<TRowValue*>(
            reinterpret_cast<char*>(RowHeader) +
            sizeof(TRowHeader) +
            index * sizeof(TRowValue));
    }

    FORCED_INLINE void SetTimestamp(TTimestamp timestamp)
    {
        RowHeader->Timestamp = timestamp;
    }

    FORCED_INLINE void SetDeletedFlag(bool deleted = true)
    {
        RowHeader->Deleted = deleted;
    }

    FORCED_INLINE const TRowValue& operator[](int index) const
    {
        return *reinterpret_cast<TRowValue*>(
            reinterpret_cast<char*>(RowHeader) +
            sizeof(TRowHeader) +
            index * sizeof(TRowValue));
    }

    FORCED_INLINE TTimestamp GetTimestamp() const
    {
        return RowHeader->Timestamp;
    }

    FORCED_INLINE int GetValueCount() const
    {
        return RowHeader->ValueCount;
    }

    FORCED_INLINE bool Deleted() const
    {
        return RowHeader->Deleted;
    }

private:
    TRowHeader* RowHeader;

};

static_assert(sizeof (TRow) == sizeof (intptr_t), "TRow has to be exactly sizeof (intptr_t) bytes.");

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
