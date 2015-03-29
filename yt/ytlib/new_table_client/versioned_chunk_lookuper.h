#pragma once

#include "public.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedLookuperPtr CreateVersionedChunkLookuper(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TColumnFilter& columnFilter,
    TLookuperPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp = SyncLastCommittedTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
