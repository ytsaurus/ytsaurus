#include "chunk_state.h"

#include "versioned_chunk_reader.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TChunkState::TChunkState(
    NChunkClient::IBlockCachePtr preloadedBlockCache,
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TCachedVersionedChunkMetaPtr chunkMeta,
    NTransactionClient::TTimestamp chunkTimestamp,
    IChunkLookupHashTablePtr lookupHashTable,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TKeyComparer keyComparer,
    TVirtualValueDirectoryPtr virtualValueDirectory)
    : BlockCache(std::move(preloadedBlockCache))
    , ChunkSpec(std::move(chunkSpec))
    , ChunkMeta(std::move(chunkMeta))
    , ChunkTimestamp(chunkTimestamp)
    , LookupHashTable(std::move(lookupHashTable))
    , PerformanceCounters(performanceCounters
        ? std::move(performanceCounters)
        : New<TChunkReaderPerformanceCounters>())
    , KeyComparer(std::move(keyComparer))
    , VirtualValueDirectory(std::move(virtualValueDirectory))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

