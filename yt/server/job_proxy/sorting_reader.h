#pragma once

#include "public.h"

#include <ytlib/actions/callback.h>

#include <ytlib/table_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/rpc/public.h>

#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISyncReaderPtr CreateSortingReader(
    NTableClient::TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NChunkClient::TNodeDirectoryPtr nodeDirectory,
    const NTableClient::TKeyColumns& keyColumns,
    TClosure onNetworkReleased,
    std::vector<NTableClient::NProto::TInputChunk>&& chunks,
    int estimatedRowCount,
    bool isApproximate);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
