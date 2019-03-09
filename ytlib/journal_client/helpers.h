#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/core/actions/future.h>

#include <yt/core/rpc/public.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReplicaDescriptor
{
    NNodeTrackerClient::TNodeDescriptor NodeDescriptor;
    int MediumIndex;
};

void FormatValue(TStringBuilderBase* builder, const TChunkReplicaDescriptor& replica, TStringBuf spec);
TString ToString(const TChunkReplicaDescriptor& replica);

////////////////////////////////////////////////////////////////////////////////

// COMPAT(shakurov)
// Change #replicas to vector<TNodeDescriptor> and remove
// TChunkReplicaDescriptor once all nodes are up to date.
TFuture<void> AbortSessionsQuorum(
    NChunkClient::TChunkId chunkId,
    const std::vector<TChunkReplicaDescriptor>& replicas,
    TDuration timeout,
    int quorum,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

// TODO(shakurov): medium indexes in #replicas are not actually used. Remove them
// (but not from the protocol as medium indexes are used at other GetChunkMeta call sites).
TFuture<NChunkClient::NProto::TMiscExt> ComputeQuorumInfo(
    NChunkClient::TChunkId chunkId,
    const std::vector<TChunkReplicaDescriptor>& replicas,
    TDuration timeout,
    int quorum,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
