#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/core/misc/range.h>
#include <yt/core/misc/linear_probe.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderPerformanceCounters
    : public virtual TIntrinsicRefCounted
{
    std::atomic<i64> StaticChunkRowLookupCount = {0};
    std::atomic<i64> StaticChunkRowLookupTrueNegativeCount = {0};
    std::atomic<i64> StaticChunkRowLookupFalsePositiveCount = {0};
    std::atomic<i64> StaticChunkRowReadCount = {0};
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkLookupHashTable
    : public TRefCounted
{
public:
    explicit TVersionedChunkLookupHashTable(size_t size);
    void Insert(TKey key, std::pair<ui16, ui32> index);
    SmallVector<std::pair<ui16, ui32>, 1> Find(TKey key) const;
    size_t GetByteSize() const;

private:
    TLinearProbeHashTable HashTable_;
};

DEFINE_REFCOUNTED_TYPE(TVersionedChunkLookupHashTable);

TVersionedChunkLookupHashTablePtr CreateChunkLookupHashTable(
    const std::vector<TSharedRef>& blocks,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TKeyComparer keyComparer);

////////////////////////////////////////////////////////////////////////////////

//! Creates a versioned chunk reader for a given range of rows.
IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    NChunkClient::TReadLimit lowerLimit,
    NChunkClient::TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

//! Creates a versioned chunk reader for a given set of keys.
/*!
 *  Number of rows readable via this reader is equal to the number of passed keys.
 *  If some key is missing, a null row is returned for it.
*/
IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TKeyComparer keyComparer,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

//! Extracted chunk state to avoid unnecessary ref counting.
struct TCacheBasedChunkState
    : public TIntrinsicRefCounted
{
    TCacheBasedChunkState() = default;
    TCacheBasedChunkState(const TCacheBasedChunkState& that)
        : BlockCache(that.BlockCache)
        , ChunkMeta(that.ChunkMeta)
        , LookupHashTable(that.LookupHashTable)
        , PerformanceCounters(that.PerformanceCounters)
        , KeyComparer(that.KeyComparer)
    { }

    NChunkClient::IBlockCachePtr BlockCache;
    TCachedVersionedChunkMetaPtr ChunkMeta;
    TVersionedChunkLookupHashTablePtr LookupHashTable;
    TChunkReaderPerformanceCountersPtr PerformanceCounters;
    TKeyComparer KeyComparer;
};

DEFINE_REFCOUNTED_TYPE(TCacheBasedChunkState)

//! Same as CreateVersionedChunkReader but only suitable for in-memory tables
//! since it relies on block cache to retrieve chunk blocks.
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
    TTimestamp timestamp = SyncLastCommittedTimestamp);

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    const TCacheBasedChunkStatePtr& state,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
