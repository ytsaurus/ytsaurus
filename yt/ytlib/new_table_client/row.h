#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TRowHeader
{
    TRowHeader(TTimestamp timestamp, int valueCount, bool isDeleted)
        : Timestamp(timestamp)
        , ValueCount(valueCount)
        , IsDeleted(isDeleted)
    { }

    TTimestamp Timestamp;
    i32 ValueCount;
    bool IsDeleted;
};

static_assert(sizeof(TRowHeader) == 16, "TRowHeader has to be exactly 16 bytes");

struct TRow
{
public:
    FORCED_INLINE explicit TRow(void* opaque)
        : Opaque(static_cast<char*>(opaque))
    { }

    FORCED_INLINE TRow(
        TChunkedMemoryPool* pool, 
        int valueCount, 
        TTimestamp timestamp = NullTimestamp, 
        bool isDeleted = false)
        : Opaque(static_cast<char*>(pool->Allocate(sizeof(TRowHeader) + valueCount * sizeof(TRowValue))))
    {
        *reinterpret_cast<TRowHeader*>(Opaque) = TRowHeader(timestamp, valueCount, isDeleted);
    }

    FORCED_INLINE TRowValue& operator[](int index)
    {
        return *reinterpret_cast<TRowValue*>(Opaque + sizeof(TRowHeader) + index * sizeof(TRowValue));
    }

    FORCED_INLINE void SetTimestamp(TTimestamp timestamp)
    {
        reinterpret_cast<TRowHeader*>(Opaque)->Timestamp = timestamp;
    }

    FORCED_INLINE void SetDeletedFlag(bool isDeleted = true)
    {
        reinterpret_cast<TRowHeader*>(Opaque)->IsDeleted = isDeleted;
    }

    FORCED_INLINE const TRowValue& operator[](int index) const
    {
        return *reinterpret_cast<TRowValue*>(Opaque + sizeof(TRowHeader) + index * sizeof(TRowValue));
    }

    FORCED_INLINE TTimestamp GetTimestamp() const
    {
        return reinterpret_cast<TRowHeader*>(Opaque)->Timestamp;
    }

    FORCED_INLINE int GetValueCount() const
    {
        return reinterpret_cast<TRowHeader*>(Opaque)->ValueCount;
    }

    FORCED_INLINE bool IsDeleted() const
    {
        return reinterpret_cast<TRowHeader*>(Opaque)->IsDeleted;
    }

private:
    char* Opaque;

};

static_assert(sizeof(TRow) == 8, "TRow has to be exactly 8 bytes");

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
