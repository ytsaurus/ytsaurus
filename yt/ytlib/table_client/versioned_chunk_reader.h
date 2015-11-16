#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/core/misc/range.h>

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
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TKeyComparer keyComparer,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
