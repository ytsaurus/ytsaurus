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
    TKeyComparer keyComparer,
    TVirtualValueDirectoryPtr virtualValueDirectory,
    TTableSchemaPtr tableSchema,
    TChunkColumnMappingPtr chunkColumnMapping)
    : BlockCache(std::move(blockCache))
    , ChunkSpec(std::move(chunkSpec))
    , ChunkMeta(std::move(chunkMeta))
    , OverrideTimestamp(overrideTimestamp)
    , LookupHashTable(std::move(lookupHashTable))
    , KeyComparer(std::move(keyComparer))
    , VirtualValueDirectory(std::move(virtualValueDirectory))
    , TableSchema(std::move(tableSchema))
    , ChunkColumnMapping(std::move(chunkColumnMapping))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

