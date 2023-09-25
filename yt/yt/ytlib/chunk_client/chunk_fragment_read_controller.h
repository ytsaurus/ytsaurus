#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/rpc/helpers.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TFragmentRequest
{
    i64 Length;
    i64 BlockOffset;
    std::optional<i64> BlockSize;
    int BlockIndex;
    int FragmentIndex;
};

////////////////////////////////////////////////////////////////////////////////

struct TPeerInfo final
{
    NNodeTrackerClient::TNodeId NodeId;
    TString Address;
    NRpc::IChannelPtr Channel;
};

using TPeerInfoPtr = TIntrusivePtr<TPeerInfo>;

////////////////////////////////////////////////////////////////////////////////

//! See ComputeProbingPenalty.
using TProbingPenalty = std::pair<int, double>;

struct TChunkReplicaInfo
{
    TProbingPenalty Penalty;
    TPeerInfoPtr PeerInfo;
    int ReplicaIndex = GenericChunkReplicaIndex;
};

using TChunkReplicaInfoList = TCompactVector<TChunkReplicaInfo, TypicalReplicaCount>;

struct TReplicasWithRevision
{
    NHydra::TRevision Revision;
    TChunkReplicaInfoList Replicas;

    bool IsEmpty() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkFragmentReadControllerPlan
{
    TCompactVector<int, TypicalReplicaCount> PeerIndices;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkFragmentReadController
{
    virtual ~IChunkFragmentReadController() = default;

    virtual TChunkId GetChunkId() const = 0;
    virtual NErasure::ECodec GetCodecId() const = 0;

    virtual void RegisterRequest(const TFragmentRequest& request) = 0;

    virtual void SetReplicas(const TReplicasWithRevision& replicasWithRevision) = 0;
    virtual const TChunkReplicaInfo& GetReplica(int peerIndex) = 0;

    virtual const TChunkFragmentReadControllerPlan* TryMakePlan() = 0;

    virtual bool IsDone() const = 0;

    virtual void PrepareRpcSubrequest(
        const TChunkFragmentReadControllerPlan* plan,
        int peerIndex,
        NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest* subrequest) = 0;
    virtual void HandleRpcSubresponse(
        const TChunkFragmentReadControllerPlan* plan,
        int peerIndex,
        const NChunkClient::NProto::TRspGetChunkFragmentSet_TSubresponse& subresponse,
        TMutableRange<TSharedRef> fragments) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkFragmentReadController> CreateChunkFragmentReadController(
    TChunkId chunkId,
    NErasure::ECodec erasureCodec,
    std::vector<TSharedRef>* responseFragments);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
