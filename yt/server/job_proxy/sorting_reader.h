#pragma once

#include "public.h"

#include <ytlib/table_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISyncReaderPtr CreateSortingReader(
    NTableClient::TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr compressedBlockCache,
    NChunkClient::IBlockCachePtr uncompressedBlockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NTableClient::TKeyColumns& keyColumns,
    TClosure onNetworkReleased,
    std::vector<NChunkClient::NProto::TChunkSpec>&& chunks,
    int estimatedRowCount,
    bool isApproximate);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
