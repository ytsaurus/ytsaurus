#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/core/misc/range.h>
#include <yt/core/misc/linear_probe.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkLookupHashTable
    : public virtual TRefCounted
{
public:
    virtual void Insert(TKey key, std::pair<ui16, ui32> index) = 0;
    virtual SmallVector<std::pair<ui16, ui32>, 1> Find(TKey key) const = 0;
    virtual size_t GetByteSize() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkLookupHashTable)

IChunkLookupHashTablePtr CreateChunkLookupHashTable(
    const std::vector<TSharedRef>& blocks,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TKeyComparer keyComparer);

////////////////////////////////////////////////////////////////////////////////

//! Same as CreateVersionedChunkReader but only suitable for in-memory tables
//! since it relies on block cache to retrieve chunk blocks.

//! Extracted chunk state to avoid unnecessary reference counting.
struct TCacheBasedChunkState
    : public TIntrinsicRefCounted
{
    TCacheBasedChunkState() = default;
    TCacheBasedChunkState(
        NChunkClient::IBlockCachePtr preloadedBlockCache,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkLookupHashTablePtr lookupHashTable,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TKeyComparer keyComparer)
        : PreloadedBlockCache(std::move(preloadedBlockCache))
        , ChunkMeta(std::move(chunkMeta))
        , LookupHashTable(std::move(lookupHashTable))
        , PerformanceCounters(std::move(performanceCounters))
        , KeyComparer(std::move(keyComparer))
    { }

    TCacheBasedChunkState(const TCacheBasedChunkState& other)
        : PreloadedBlockCache(other.PreloadedBlockCache)
        , ChunkMeta(other.ChunkMeta)
        , LookupHashTable(other.LookupHashTable)
        , PerformanceCounters(other.PerformanceCounters)
        , KeyComparer(other.KeyComparer)
    { }

    NChunkClient::IBlockCachePtr PreloadedBlockCache;
    TCachedVersionedChunkMetaPtr ChunkMeta;
    IChunkLookupHashTablePtr LookupHashTable;
    TChunkReaderPerformanceCountersPtr PerformanceCounters;
    TKeyComparer KeyComparer;
};

DEFINE_REFCOUNTED_TYPE(TCacheBasedChunkState)

/*!
 *  For each block #blockCache must be able for provide either a compressed
 *  or uncompressed version.
 *
 *  The implementation is (kind of) highly optimized :)
 */

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    const TCacheBasedChunkStatePtr& state,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    const TCacheBasedChunkStatePtr& state,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
