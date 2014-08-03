#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NJournalClient {

////////////////////////////////////////////////////////////////////////////////

TAsyncError AbortSessionsQuorum(
    const NChunkClient::TChunkId& chunkId,
    const std::vector<NNodeTrackerClient::TNodeDescriptor>& replicas,
    TDuration timeout,
    int quorum);

TFuture<TErrorOr<NChunkClient::NProto::TMiscExt>> ComputeQuorumInfo(
    const NChunkClient::TChunkId& chunkId,
    const std::vector<NNodeTrackerClient::TNodeDescriptor>& replicas,
    TDuration timeout,
    int quorum);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalClient
} // namespace NYT
