#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NJournalClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> AbortSessionsQuorum(
    const NChunkClient::TChunkId& chunkId,
    const std::vector<NNodeTrackerClient::TNodeDescriptor>& replicas,
    TDuration timeout,
    int quorum);

TFuture<NChunkClient::NProto::TMiscExt> ComputeQuorumInfo(
    const NChunkClient::TChunkId& chunkId,
    const std::vector<NNodeTrackerClient::TNodeDescriptor>& replicas,
    TDuration timeout,
    int quorum);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalClient
} // namespace NYT
