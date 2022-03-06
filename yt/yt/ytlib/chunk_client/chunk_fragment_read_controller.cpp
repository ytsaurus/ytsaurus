#include "chunk_fragment_read_controller.h"

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <util/generic/algorithm.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkFragmentReadControllerBase
    : public IChunkFragmentReadController
{
public:
    TChunkFragmentReadControllerBase(
        TChunkId chunkId,
        std::vector<TSharedRef>* responseFragments)
        : ChunkId_(chunkId)
        , ResponseFragments_(responseFragments)
    { }

    TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    void RegisterRequest(const TFragmentRequest& request) override
    {
        FragmentRequests_.push_back(request);
    }

    const TChunkReplicaInfo& GetReplica(int peerIndex) override
    {
        return Peers_[peerIndex];
    }

    bool IsDone() const override
    {
        return Done_;
    }

protected:
    const TChunkId ChunkId_;
    std::vector<TSharedRef>* const ResponseFragments_;

    std::vector<TFragmentRequest> FragmentRequests_;

    TChunkReplicaInfoList Peers_;

    bool Done_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TRegularChunkFragmentReadController
    : public TChunkFragmentReadControllerBase
{
public:
    using TChunkFragmentReadControllerBase::TChunkFragmentReadControllerBase;

    void SetReplicas(const TChunkReplicaInfoList& replicas) override
    {
        Peers_ = replicas;

        SortBy(Peers_, [] (const TChunkReplicaInfo& replicaInfo) {
            return std::make_tuple(replicaInfo.Penalty, replicaInfo.PeerInfo->NodeId);
        });

        for (int index = 0; index < std::min(static_cast<int>(Peers_.size()), MaxPlans); ++index) {
            Plans_.push_back(TChunkFragmentReadControllerPlan{
                .PeerIndices = {index}
            });
        }
    }

    const TChunkFragmentReadControllerPlan* TryMakePlan() override
    {
        if (CurrentPlanIndex_ >= std::ssize(Plans_)) {
            return nullptr;
        }

        return &Plans_[CurrentPlanIndex_++];
    }

    void PrepareRpcSubrequest(
        const TChunkFragmentReadControllerPlan* /*plan*/,
        int /*peerIndex*/,
        NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest* subrequest) override
    {
        ToProto(subrequest->mutable_chunk_id(), ChunkId_);
        for (const auto& fragmentRequest : FragmentRequests_) {
            auto* fragment = subrequest->add_fragments();
            fragment->set_length(fragmentRequest.Length);
            fragment->set_block_index(fragmentRequest.BlockIndex);
            fragment->set_block_offset(fragmentRequest.BlockOffset);
        }
    }

    void HandleRpcSubresponse(
        const TChunkFragmentReadControllerPlan* /*plan*/,
        int /*peerIndex*/,
        const NChunkClient::NProto::TRspGetChunkFragmentSet_TSubresponse& subresponse,
        TMutableRange<TSharedRef> fragments) override
    {
        if (Done_) {
            return;
        }

        if (!subresponse.has_complete_chunk()) {
            return;
        }

        YT_VERIFY(fragments.size() == FragmentRequests_.size());
        for (int index = 0; index < std::ssize(fragments); ++index) {
            const auto& request = FragmentRequests_[index];
            YT_VERIFY(fragments[index]);
            (*ResponseFragments_)[request.FragmentIndex] = std::move(fragments[index]);
        }

        Done_ = true;
    }

private:
    int CurrentPlanIndex_ = 0;
    static constexpr int MaxPlans = 2;
    TCompactVector<TChunkFragmentReadControllerPlan, MaxPlans> Plans_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkFragmentReadController> CreateChunkFragmentReadController(
    TChunkId chunkId,
    NErasure::ECodec erasureCodec,
    std::vector<TSharedRef>* responseFragments)
{
    if (IsErasureChunkId(chunkId)) {
        if (erasureCodec == NErasure::ECodec::None) {
            THROW_ERROR_EXCEPTION("Erasure codec is not specified for erasure chunk %v",
                erasureCodec,
                chunkId);
        }
        THROW_ERROR_EXCEPTION("Erasure chunks are not supported");
    } else {
        if (erasureCodec != NErasure::ECodec::None) {
            THROW_ERROR_EXCEPTION("Unexpected erasure codec %Qlv specified for regular chunk %v",
                erasureCodec,
                chunkId);
        }
        return std::unique_ptr<IChunkFragmentReadController>(
            new TRegularChunkFragmentReadController(chunkId, responseFragments));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
