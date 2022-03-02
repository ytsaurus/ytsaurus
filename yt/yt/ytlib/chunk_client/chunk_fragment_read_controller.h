#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

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
    double Penalty = 0;
    TPeerInfoPtr PeerInfo;
};

using TChunkReplicaInfoList = TCompactVector<TChunkReplicaInfo, TypicalReplicaCount>;

////////////////////////////////////////////////////////////////////////////////

class TChunkFragmentReadController
{
public:
    TChunkFragmentReadController(
        TChunkId chunkId,
        std::vector<TSharedRef>* responseFragments);

    TChunkId GetChunkId() const;

    void RegisterRequest(const TFragmentRequest& request);

    void SetReplicas(const TChunkReplicaInfoList& replicas);
    const TChunkReplicaInfo& GetReplica(int peerIndex);

    using TPlan = TCompactVector<int, TypicalReplicaCount>;
    std::optional<TPlan> TryMakePlan();

    bool IsDone() const;

    int PrepareRpcSubrequest(
        int /*peerIndex*/,
        NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest* subrequest);
    void HandleRpcSubresponse(
        int /*peerIndex*/,
        const NChunkClient::NProto::TRspGetChunkFragmentSet_TSubresponse& subresponse,
        TMutableRange<TSharedRef> fragments);

private:
    const TChunkId ChunkId_;
    std::vector<TSharedRef>* const ResponseFragments_;

    std::vector<TFragmentRequest> FragmentRequests_;

    TChunkReplicaInfoList Peers_;
    int CurrentPeerIndex_ = 0;

    bool Done_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
