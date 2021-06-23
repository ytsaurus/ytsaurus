#pragma once

#include "reader_statistics.h"

#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TItem>
NTableClient::IVersionedReaderPtr CreateVersionedChunkReader(
    TSharedRange<TItem> readItems,
    NTableClient::TTimestamp timestamp,
    NTableClient::TCachedVersionedChunkMetaPtr chunkMeta,
    const NTableClient::TColumnFilter& columnFilter,
    NChunkClient::IBlockCachePtr blockCache,
    const NTableClient::TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr underlyingReader,
    NTableClient::TChunkReaderPerformanceCountersPtr performanceCounters,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    bool produceAll,
    TReaderTimeStatisticsPtr timeStatistics = nullptr);

////////////////////////////////////////////////////////////////////////////////

using THolderPtr = TIntrusivePtr<TRefCounted>;

// Chunk view support.
TSharedRange<NTableClient::TRowRange> ClipRanges(
    TSharedRange<NTableClient::TRowRange> ranges,
    NTableClient::TUnversionedRow lower,
    NTableClient::TUnversionedRow upper,
    THolderPtr holder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
