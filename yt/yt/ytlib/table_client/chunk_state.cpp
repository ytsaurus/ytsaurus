#include "chunk_state.h"

#include "versioned_chunk_reader.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TChunkState::TChunkState(
    NChunkClient::IBlockCachePtr blockCache,
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TCachedVersionedChunkMetaPtr chunkMeta,
    NTransactionClient::TTimestamp overrideTimestamp,
    TChunkLookupHashTablePtr lookupHashTable,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TKeyComparer keyComparer,
    TVirtualValueDirectoryPtr virtualValueDirectory,
    TTableSchemaPtr tableSchema,
    TChunkColumnMappingPtr chunkColumnMapping)
    : BlockCache(std::move(blockCache))
    , ChunkSpec(std::move(chunkSpec))
    , ChunkMeta(std::move(chunkMeta))
    , OverrideTimestamp(overrideTimestamp)
    , LookupHashTable(std::move(lookupHashTable))
    , PerformanceCounters(performanceCounters
        ? std::move(performanceCounters)
        : New<TChunkReaderPerformanceCounters>())
    , KeyComparer(std::move(keyComparer))
    , VirtualValueDirectory(std::move(virtualValueDirectory))
    , TableSchema(std::move(tableSchema))
    , ChunkColumnMapping(std::move(chunkColumnMapping))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

