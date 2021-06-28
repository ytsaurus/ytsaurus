#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/linear_probe.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t RowBufferCapacity = 1000;

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderPerformanceCounters
    : public virtual TRefCounted
{
    std::atomic<i64> StaticChunkRowReadCount = 0;
    std::atomic<i64> StaticChunkRowReadDataWeightCount = 0;
    std::atomic<i64> StaticChunkRowLookupCount = 0;
    std::atomic<i64> StaticChunkRowLookupTrueNegativeCount = 0;
    std::atomic<i64> StaticChunkRowLookupFalsePositiveCount = 0;
    std::atomic<i64> StaticChunkRowLookupDataWeightCount = 0;
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

std::vector<TColumnIdMapping> BuildVersionedSimpleSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TTableSchemaPtr& tableSchema,
    const TTableSchemaPtr& chunkSchema);

std::vector<TColumnIdMapping> BuildSchemalessHorizontalSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TTableSchemaPtr& tableSchema,
    const TTableSchemaPtr& chunkSchema);

////////////////////////////////////////////////////////////////////////////////

//! Creates a versioned chunk reader for a given range of rows.
IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    TSharedRange<TRowRange> ranges,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TSharedRange<TRowRange>& singletonClippingRange = {},
    const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr);

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    TLegacyOwningKey lowerLimit,
    TLegacyOwningKey upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr);

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
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TSharedRange<TLegacyKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

class TRowReaderAdapter
    : public TRefCounted
{
public:
    TRowReaderAdapter(
        TChunkReaderConfigPtr config,
        NChunkClient::IChunkReaderPtr chunkReader,
        const TChunkStatePtr& chunkState,
        const TCachedVersionedChunkMetaPtr& chunkMeta,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        const TSharedRange<TLegacyKey>& keys,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions);

    void ReadRowset(const std::function<void (TVersionedRow)>& onRow);

private:
    const int KeyCount_;
    const IVersionedReaderPtr UnderlyingReader_;

    IVersionedRowBatchPtr RowBatch_;
    int RowIndex_ = -1;

    TVersionedRow FetchRow();
};

DEFINE_REFCOUNTED_TYPE(TRowReaderAdapter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
