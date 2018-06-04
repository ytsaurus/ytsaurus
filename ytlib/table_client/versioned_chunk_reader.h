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
    std::atomic<i64> StaticChunkRowReadCount = {0};
    std::atomic<i64> StaticChunkRowReadDataWeightCount = {0};
    std::atomic<i64> StaticChunkRowLookupCount = {0};
    std::atomic<i64> StaticChunkRowLookupTrueNegativeCount = {0};
    std::atomic<i64> StaticChunkRowLookupFalsePositiveCount = {0};
    std::atomic<i64> StaticChunkRowLookupDataWeightCount = {0};
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

std::vector<TColumnIdMapping> BuildVersionedSimpleSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta);

std::vector<TColumnIdMapping> BuildSchemalessHorizontalSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta);

////////////////////////////////////////////////////////////////////////////////

//! Creates a versioned chunk reader for a given range of rows.
IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    TSharedRange<TRowRange> ranges,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    TOwningKey lowerLimit,
    TOwningKey upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

//! Creates a versioned chunk reader for a given set of keys.
/*!
 *  Number of rows readable via this reader is equal to the number of passed keys.
 *  If some key is missing, a null row is returned for it.
*/
IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
