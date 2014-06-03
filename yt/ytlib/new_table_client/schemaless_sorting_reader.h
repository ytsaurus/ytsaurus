#pragma once

#include "public.h"

#include "schemaless_chunk_reader.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

/*
IMultiChunkSchemalessReaderPtr CreateSchemalessSortingReader(
    NChunkClient::TMultiChunkReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NTableClient::TKeyColumns& keyColumns,
    TClosure onNetworkReleased,
    std::vector<NChunkClient::NProto::TChunkSpec>&& chunks,
    i64 estimatedRowCount,
    bool isApproximate);
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
