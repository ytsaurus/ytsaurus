#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Extracted chunk state to avoid unnecessary reference counting.
struct TChunkState final
{
    NChunkClient::IBlockCachePtr BlockCache;
    NChunkClient::NProto::TChunkSpec ChunkSpec;
    // TODO(lukyan): Remove CachedVersionedChunkMeta because it is specific to versioned readers.
    // Not used in many other readers.
    TCachedVersionedChunkMetaPtr ChunkMeta;
    NTransactionClient::TTimestamp OverrideTimestamp = NTransactionClient::NullTimestamp;
    TChunkLookupHashTablePtr LookupHashTable;
    TKeyComparer KeyComparer;
    TVirtualValueDirectoryPtr VirtualValueDirectory;
    TTableSchemaPtr TableSchema;
    std::optional<NChunkClient::TDataSource> DataSource;
    TChunkColumnMappingPtr ChunkColumnMapping;
};

DEFINE_REFCOUNTED_TYPE(TChunkState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
