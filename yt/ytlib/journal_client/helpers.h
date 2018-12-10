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

void FormatValue(TStringBuilder* builder, const TChunkReplicaDescriptor& replica, TStringBuf spec);
TString ToString(const TChunkReplicaDescriptor& replica);

////////////////////////////////////////////////////////////////////////////////

TFuture<void> AbortSessionsQuorum(
    const NChunkClient::TChunkId& chunkId,
    const std::vector<TChunkReplicaDescriptor>& replicas,
    TDuration timeout,
    int quorum,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

TFuture<NChunkClient::NProto::TMiscExt> ComputeQuorumInfo(
    const NChunkClient::TChunkId& chunkId,
    const std::vector<TChunkReplicaDescriptor>& replicas,
    TDuration timeout,
    int quorum,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
