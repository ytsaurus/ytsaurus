#pragma once

#include "reader_statistics.h"
#include "public.h"

#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NNewTableClient {

struct TPreparedChunkMeta;

////////////////////////////////////////////////////////////////////////////////

TBlockManagerFactory CreateAsyncBlockWindowManagerFactory(
    NTableClient::TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr underlyingReader,
    NChunkClient::IBlockCachePtr blockCache,
    NChunkClient::TClientChunkReadOptions chunkReadOptions,
    NTableClient::TCachedVersionedChunkMetaPtr chunkMeta,
    IInvokerPtr sessionInvoker = nullptr);

TBlockManagerFactory CreateSyncBlockWindowManagerFactory(
    NChunkClient::IBlockCachePtr blockCache,
    NTableClient::TCachedVersionedChunkMetaPtr chunkMeta,
    NChunkClient::TChunkId chunkId);

////////////////////////////////////////////////////////////////////////////////

template <class TReadItems>
NTableClient::IVersionedReaderPtr CreateVersionedChunkReader(
    TReadItems readItems,
    NTableClient::TTimestamp timestamp,
    NTableClient::TCachedVersionedChunkMetaPtr chunkMeta,
    const NTableClient::TTableSchemaPtr& tableSchema,
    const NTableClient::TColumnFilter& columnFilter,
    const NTableClient::TChunkColumnMappingPtr& chunkColumnMapping,
    TBlockManagerFactory blockManagerFactory,
    bool produceAll,
    TReaderStatisticsPtr readerStatistics = nullptr,
    NTableClient::TKeyFilterStatisticsPtr keyFilterStatistics = nullptr);

////////////////////////////////////////////////////////////////////////////////

using THolderPtr = TIntrusivePtr<TRefCounted>;

// Chunk view support.
TSharedRange<NTableClient::TRowRange> ClipRanges(
    TSharedRange<NTableClient::TRowRange> ranges,
    NTableClient::TUnversionedRow lower,
    NTableClient::TUnversionedRow upper,
    THolderPtr holder);

////////////////////////////////////////////////////////////////////////////////

TKeysWithHints BuildKeyHintsUsingLookupTable(
    const NTableClient::TChunkLookupHashTable& lookupHashTable,
    TSharedRange<NTableClient::TLegacyKey> keys);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
