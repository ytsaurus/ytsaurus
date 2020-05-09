#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/actions/future.h>

#include <yt/core/rpc/public.h>

#include <yt/library/erasure/public.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateJournalAttributes(
    NErasure::ECodec erasureCodec,
    int replicationFactor,
    int readQuorum,
    int writeQuorum);

////////////////////////////////////////////////////////////////////////////////

struct TChunkReplicaDescriptor
{
    NNodeTrackerClient::TNodeDescriptor NodeDescriptor;
    int ReplicaIndex = NChunkClient::GenericChunkReplicaIndex;
    int MediumIndex = NChunkClient::GenericMediumIndex;
};

void FormatValue(TStringBuilderBase* builder, const TChunkReplicaDescriptor& replica, TStringBuf spec);
TString ToString(const TChunkReplicaDescriptor& replica);

////////////////////////////////////////////////////////////////////////////////

TFuture<void> AbortSessionsQuorum(
    NChunkClient::TChunkId chunkId,
    std::vector<TChunkReplicaDescriptor> replicas,
    TDuration timeout,
    int quorum,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

TFuture<NChunkClient::NProto::TMiscExt> ComputeQuorumInfo(
    NChunkClient::TChunkId chunkId,
    NErasure::ECodec codecId,
    std::vector<TChunkReplicaDescriptor> replicas,
    TDuration timeout,
    int quorum,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

std::vector<std::vector<TSharedRef>> EncodeErasureJournalRows(
    NErasure::ECodec codecId,
    const std::vector<TSharedRef>& rows);

std::vector<TSharedRef> DecodeErasureJournalRows(
    NErasure::ECodec codecId,
    const std::vector<std::vector<TSharedRef>>& encodedRowLists);

std::vector<std::vector<TSharedRef>> RepairErasureJournalRows(
    NErasure::ECodec codecId,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<std::vector<TSharedRef>>& repairRowLists);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
