#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TRowHeader
{
    TRowHeader(TTimestamp timestamp, int valueCount, bool deleted)
        : Timestamp(timestamp)
        , ValueCount(valueCount)
        , Deleted(deleted)
    { }

    TTimestamp Timestamp;
    i32 ValueCount;
    bool Deleted;
};

static_assert(sizeof(TRowHeader) == 16, "TRowHeader has to be exactly 16 bytes");

struct TRow
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
        : RowHeader(pool->AllocateAligned(sizeof(TRowHeader) + valueCount * sizeof(TRowValue)))
    {
        *RowHeader = TRowHeader(timestamp, valueCount, deleted);
    }

    FORCED_INLINE TRowValue& operator[](int index)
    {
        return *reinterpret_cast<TRowValue*>(reinterpret_cast<char*>(RowHeader) +
            sizeof(TRowHeader) + index * sizeof(TRowValue));
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
        return *reinterpret_cast<TRowValue*>(reinterpret_cast<char*>(RowHeader) +
            sizeof(TRowHeader) + index * sizeof(TRowValue));
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

static_assert(sizeof(TRow) == 8, "TRow has to be exactly 8 bytes");

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
