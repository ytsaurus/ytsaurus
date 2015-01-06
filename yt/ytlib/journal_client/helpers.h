#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

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
