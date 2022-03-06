#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/rpc/helpers.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TFragmentRequest
{
    int FragmentIndex;
    i64 Length;
    int BlockIndex;
    i64 BlockOffset;
};

////////////////////////////////////////////////////////////////////////////////

struct TPeerInfo final
{
    NNodeTrackerClient::TNodeId NodeId;
    NRpc::TAddressWithNetwork Address;
    NRpc::IChannelPtr Channel;
};

using TPeerInfoPtr = TIntrusivePtr<TPeerInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TChunkReplicaInfo
{
    int ReplicaIndex = GenericChunkReplicaIndex;
    double Penalty = 0;
    TPeerInfoPtr PeerInfo;
};

using TChunkReplicaInfoList = TCompactVector<TChunkReplicaInfo, TypicalReplicaCount>;

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

    virtual void RegisterRequest(const TFragmentRequest& request) = 0;

    virtual void SetReplicas(const TChunkReplicaInfoList& replicas) = 0;
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
