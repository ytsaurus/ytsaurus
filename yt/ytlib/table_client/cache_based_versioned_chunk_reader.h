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
/*!
 *  For each block #blockCache must be able for provide either a compressed
 *  or uncompressed version.
 *
 *  The implementation is (kind of) highly optimized :)
 */
IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IChunkLookupHashTablePtr lookupHashTable,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TKeyComparer keyComparer,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TOwningKey lowerBounI,
    TOwningKey upperBound,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
