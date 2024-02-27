#include "chunk_fragment_read_controller.h"

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/misc/serialize.h>

#include <util/generic/algorithm.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

bool TReplicasWithRevision::IsEmpty() const
{
    return Replicas.empty();
}

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

    bool IsDone() const override
    {
        return Done_;
    }

protected:
    const TChunkId ChunkId_;
    std::vector<TSharedRef>* const ResponseFragments_;

    bool Done_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TRegularChunkFragmentReadController
    : public TChunkFragmentReadControllerBase
{
public:
    using TChunkFragmentReadControllerBase::TChunkFragmentReadControllerBase;

    NErasure::ECodec GetCodecId() const override
    {
        return NErasure::ECodec::None;
    }

    void RegisterRequest(const TFragmentRequest& request) override
    {
        FragmentRequests_.push_back(request);
    }

    void SetReplicas(const TReplicasWithRevision& replicasWithRevision) override
    {
        ReplicasWithRevision_ = replicasWithRevision;

        SortBy(ReplicasWithRevision_.Replicas, [] (const TChunkReplicaInfo& replicaInfo) {
            return std::tuple(replicaInfo.Penalty, replicaInfo.PeerInfo->NodeId);
        });

        for (int index = 0; index < std::min(std::ssize(ReplicasWithRevision_.Replicas), MaxPlans); ++index) {
            Plans_.push_back(TChunkFragmentReadControllerPlan{
                .PeerIndices = {index}
            });
        }
    }

    const TChunkReplicaInfo& GetReplica(int peerIndex) override
    {
        return ReplicasWithRevision_.Replicas[peerIndex];
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
        subrequest->set_ally_replicas_revision(ReplicasWithRevision_.Revision);
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
            YT_ASSERT(fragments[index]);
            (*ResponseFragments_)[request.FragmentIndex] = std::move(fragments[index]);
        }

        Done_ = true;
    }

private:
    int CurrentPlanIndex_ = 0;
    static constexpr i64 MaxPlans = 2;
    TCompactVector<TChunkFragmentReadControllerPlan, MaxPlans> Plans_;

    TReplicasWithRevision ReplicasWithRevision_;

    std::vector<TFragmentRequest> FragmentRequests_;
};

////////////////////////////////////////////////////////////////////////////////

class TErasureChunkFragmentReadController
    : public TChunkFragmentReadControllerBase
{
public:
    TErasureChunkFragmentReadController(
        TChunkId chunkId,
        NErasure::ECodec codecId,
        std::vector<TSharedRef>* responseFragments)
        : TChunkFragmentReadControllerBase(chunkId, responseFragments)
        , Codec_(NErasure::GetCodec(codecId))
        , TotalPartCount_(Codec_->GetTotalPartCount())
        , DataPartCount_(Codec_->GetDataPartCount())
        , Replicas_(TotalPartCount_)
    { }

    NErasure::ECodec GetCodecId() const override
    {
        return Codec_->GetId();
    }

    void RegisterRequest(const TFragmentRequest& request) override
    {
        YT_VERIFY(request.BlockSize);
        Requests_.push_back(TRequest(request, DataPartCount_));
    }

    void SetReplicas(const TReplicasWithRevision& replicasWithRevision) override
    {
        ReplicasRevision_ = replicasWithRevision.Revision;

        std::fill(Replicas_.begin(), Replicas_.end(), std::nullopt);

        for (const auto& replica : replicasWithRevision.Replicas) {
            auto partIndex = replica.ReplicaIndex;
            auto& partReplica = Replicas_[partIndex];
            if (!partReplica || partReplica->Penalty > replica.Penalty) {
                partReplica = replica;
            }
        }
    }

    const TChunkReplicaInfo& GetReplica(int peerIndex) override
    {
        return *Replicas_[peerIndex];
    }

    const TChunkFragmentReadControllerPlan* TryMakePlan() override
    {
        constexpr int RegularPlanIndex = 0;
        constexpr int RepairPlanIndex = 1;
        constexpr int PlanCount = 2;

        while (CurrentPlanIndex_ < PlanCount) {
            TChunkFragmentReadControllerPlan* plan = nullptr;
            if (CurrentPlanIndex_ == RegularPlanIndex) {
                plan = BuildRegularPlan();
            } else if (CurrentPlanIndex_ == RepairPlanIndex) {
                plan = BuildRepairPlan();
            }

            ++CurrentPlanIndex_;
            if (plan) {
                return plan;
            }
        }

        return nullptr;
    }

    void PrepareRpcSubrequest(
        const TChunkFragmentReadControllerPlan* plan,
        int partIndex,
        NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest* subrequest) override
    {
        YT_ASSERT(partIndex >= 0 && partIndex < TotalPartCount_);
        YT_ASSERT(GetReplica(partIndex).ReplicaIndex == partIndex);

        if (plan == &RegularPlan_) {
            PrepareRegularRpcSubrequest(partIndex, subrequest);
        } else if (plan == &RepairPlan_) {
            PrepareRepairRpcSubrequest(partIndex, subrequest);
        } else {
            YT_ABORT();
        }
    }

    void HandleRpcSubresponse(
        const TChunkFragmentReadControllerPlan* plan,
        int partIndex,
        const NChunkClient::NProto::TRspGetChunkFragmentSet_TSubresponse& subresponse,
        TMutableRange<TSharedRef> fragments) override
    {
        YT_ASSERT(partIndex >= 0 && partIndex < TotalPartCount_);
        YT_ASSERT(GetReplica(partIndex).ReplicaIndex == partIndex);

        if (Done_) {
            return;
        }

        if (!subresponse.has_complete_chunk()) {
            return;
        }

        if (plan == &RegularPlan_) {
            HandleRegularRpcSubresponse(partIndex, fragments);
        } else if (plan == &RepairPlan_) {
            HandleRepairRpcSubresponse(partIndex, fragments);
        } else {
            YT_ABORT();
        }
    }

private:
    const NErasure::ICodec* const Codec_;
    const int TotalPartCount_;
    const int DataPartCount_;

    NHydra::TRevision ReplicasRevision_ = NHydra::NullRevision;
    TCompactVector<std::optional<TChunkReplicaInfo>, TypicalReplicaCount> Replicas_;

    TChunkFragmentReadControllerPlan RegularPlan_;
    TChunkFragmentReadControllerPlan RepairPlan_;
    int CurrentPlanIndex_ = 0;

    struct TBlockFragment
    {
        i64 BlockOffset;
        i64 Length;

        static TBlockFragment FromRange(i64 start, i64 end)
        {
            return TBlockFragment{
                .BlockOffset = start,
                .Length = end - start
            };
        }
    };

    struct TChunkFragment
    {
        int BlockIndex;
        i64 BlockOffset;
        i64 Length;
    };

    struct TRequest
    {
        int FragmentIndex;
        i64 Length;
        int BlockIndex;
        i64 BlockOffset;
        i64 BlockPartSize;

        int FirstPartIndex;
        i64 FirstPartStartOffset;
        int LastPartIndex;
        i64 LastPartEndOffset;

        constexpr static int TypicalRequestParts = 2;
        TCompactVector<TSharedRef, TypicalRequestParts> Parts;
        int FetchedPartCount = 0;

        bool Completed = false;

        TRequest(const TFragmentRequest& request, int codecDataPartCount)
            : FragmentIndex(request.FragmentIndex)
            , Length(request.Length)
            , BlockIndex(request.BlockIndex)
            , BlockOffset(request.BlockOffset)
            , BlockPartSize(DivCeil<i64>(*request.BlockSize, codecDataPartCount))
            , FirstPartIndex(BlockOffset / BlockPartSize)
            , FirstPartStartOffset(BlockOffset % BlockPartSize)
            , LastPartIndex((BlockOffset + Length - 1) / BlockPartSize)
            , LastPartEndOffset((BlockOffset + Length - 1) % BlockPartSize + 1)
            , Parts(LastPartIndex - FirstPartIndex + 1)
        { }

        TBlockFragment GetProjection(int partIndex) const
        {
            auto firstPartIndex = FirstPartIndex;
            auto lastPartIndex = LastPartIndex;

            YT_ASSERT(partIndex >= FirstPartIndex);
            YT_ASSERT(partIndex <= LastPartIndex);

            if (firstPartIndex == lastPartIndex) {
                return TBlockFragment{
                    .BlockOffset = FirstPartStartOffset,
                    .Length = Length
                };
            } else if (partIndex == firstPartIndex) {
                return TBlockFragment::FromRange(FirstPartStartOffset, BlockPartSize);
            } else if (partIndex == lastPartIndex) {
                return TBlockFragment::FromRange(0, LastPartEndOffset);
            } else {
                return TBlockFragment::FromRange(0, BlockPartSize);
            }
        }
    };
    std::vector<TRequest> Requests_;
    int CompletedRequestCount_ = 0;

    constexpr static int TypicalRequestsPerPart = 2;
    std::vector<TCompactVector<int, TypicalRequestsPerPart>> PartIndexToRegularRequestIndices_;

    std::vector<TChunkFragment> RepairFragments_;

    struct TRequestPartDescriptor
    {
        int RequestIndex;
        int PartIndex;
    };
    constexpr static int TypicalRequestPartsPerRepairRequest = 2;
    std::vector<TCompactVector<TRequestPartDescriptor, TypicalRequestPartsPerRepairRequest>> RepairRequestIndexToRequestParts_;

    std::vector<std::vector<TSharedRef>> RepairFragmentParts_;

    TCompactVector<bool, TypicalReplicaCount> HasPartForRepair_;

    TChunkFragmentReadControllerPlan* BuildRegularPlan()
    {
        PartIndexToRegularRequestIndices_.resize(DataPartCount_);

        for (int requestIndex = 0; requestIndex < std::ssize(Requests_); ++requestIndex) {
            const auto& request = Requests_[requestIndex];
            for (
                int partIndex = request.FirstPartIndex;
                partIndex <= request.LastPartIndex;
                ++partIndex)
            {
                PartIndexToRegularRequestIndices_[partIndex].push_back(requestIndex);
            }
        }

        for (int partIndex = 0; partIndex < DataPartCount_; ++partIndex) {
            if (!PartIndexToRegularRequestIndices_[partIndex].empty()) {
                if (Replicas_[partIndex]) {
                    RegularPlan_.PeerIndices.push_back(partIndex);
                } else {
                    return nullptr;
                }
            }
        }

        return &RegularPlan_;
    }

    void PrepareRegularRpcSubrequest(
        int partIndex,
        NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest* subrequest)
    {
        YT_ASSERT(partIndex >= 0 && partIndex < DataPartCount_);

        ToProto(subrequest->mutable_chunk_id(), ErasurePartIdFromChunkId(ChunkId_, partIndex));
        subrequest->set_ally_replicas_revision(ReplicasRevision_);

        for (auto requestIndex : PartIndexToRegularRequestIndices_[partIndex]) {
            const auto& request = Requests_[requestIndex];
            auto blockFragment = request.GetProjection(partIndex);

            auto* fragment = subrequest->add_fragments();
            fragment->set_block_index(request.BlockIndex);
            fragment->set_length(blockFragment.Length);
            fragment->set_block_offset(blockFragment.BlockOffset);
        }
    }

    void HandleRegularRpcSubresponse(int partIndex, TMutableRange<TSharedRef> fragments)
    {
        YT_ASSERT(partIndex >= 0 && partIndex < DataPartCount_);
        YT_VERIFY(std::ssize(fragments) == std::ssize(PartIndexToRegularRequestIndices_[partIndex]));

        for (int index = 0; index < std::ssize(fragments); ++index) {
            auto requestIndex = PartIndexToRegularRequestIndices_[partIndex][index];
            SetResponsePart(requestIndex, partIndex, fragments[index]);
        }
    }

    TChunkFragmentReadControllerPlan* BuildRepairPlan()
    {
        NErasure::TPartIndexList erasedIndices;
        for (int partIndex = 0; partIndex < TotalPartCount_; ++partIndex) {
            if (Replicas_[partIndex]) {
                RepairPlan_.PeerIndices.push_back(partIndex);
            } else {
                erasedIndices.push_back(partIndex);
            }
        }

        if (!Codec_->CanRepair(erasedIndices)) {
            return nullptr;
        }

        struct TEndpoint
        {
            int BlockIndex;
            i64 BlockOffset;
            bool Open;

            TRequestPartDescriptor PartDescriptor;
        };
        std::vector<TEndpoint> endpoints;

        for (int requestIndex = 0; requestIndex < std::ssize(Requests_); ++requestIndex) {
            const auto& request = Requests_[requestIndex];
            for (
                int partIndex = request.FirstPartIndex;
                partIndex <= request.LastPartIndex;
                ++partIndex)
            {
                TRequestPartDescriptor partDescriptor{
                    .RequestIndex = requestIndex,
                    .PartIndex = partIndex
                };

                auto projection = request.GetProjection(partIndex);
                endpoints.push_back(TEndpoint{
                    .BlockIndex = request.BlockIndex,
                    .BlockOffset = projection.BlockOffset,
                    .Open = true,
                    .PartDescriptor = partDescriptor
                });
                endpoints.push_back(TEndpoint{
                    .BlockIndex = request.BlockIndex,
                    .BlockOffset = projection.BlockOffset + projection.Length - 1,
                    .Open = false,
                    .PartDescriptor = partDescriptor
                });
            }
        }

        SortBy(endpoints, [&] (const TEndpoint& endpoint) {
            return std::tuple(endpoint.BlockIndex, endpoint.BlockOffset, !endpoint.Open);
        });

        int balance = 0;
        for (const auto& endpoint : endpoints) {
            if (endpoint.Open) {
                if (++balance == 1) {
                    RepairFragments_.push_back(TChunkFragment{
                        .BlockIndex = endpoint.BlockIndex,
                        .BlockOffset = endpoint.BlockOffset,
                        .Length = -1
                    });
                    RepairRequestIndexToRequestParts_.emplace_back();
                }
                RepairRequestIndexToRequestParts_.back().push_back(endpoint.PartDescriptor);
            } else {
                if (--balance == 0) {
                    auto& fragment = RepairFragments_.back();
                    YT_VERIFY(fragment.BlockIndex == endpoint.BlockIndex);
                    fragment.Length = endpoint.BlockOffset - fragment.BlockOffset + 1;
                }
            }
        }

        RepairFragmentParts_.resize(RepairFragments_.size());
        for (auto& parts : RepairFragmentParts_) {
            parts.resize(TotalPartCount_);
        }

        HasPartForRepair_.resize(TotalPartCount_);

        return &RepairPlan_;
    }

    void PrepareRepairRpcSubrequest(
        int partIndex,
        NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest* subrequest)
    {
        ToProto(subrequest->mutable_chunk_id(), ErasurePartIdFromChunkId(ChunkId_, partIndex));
        subrequest->set_ally_replicas_revision(ReplicasRevision_);

        for (const auto& repairFragment : RepairFragments_) {
            auto* fragment = subrequest->add_fragments();
            fragment->set_block_index(repairFragment.BlockIndex);
            fragment->set_block_offset(repairFragment.BlockOffset);
            fragment->set_length(repairFragment.Length);
        }
    }

    void HandleRepairRpcSubresponse(
        int partIndex,
        TMutableRange<TSharedRef> fragments)
    {
        HasPartForRepair_[partIndex] = true;

        YT_VERIFY(fragments.size() == RepairFragments_.size());
        for (int index = 0; index < std::ssize(fragments); ++index) {
            RepairFragmentParts_[index][partIndex] = fragments[index];
        }

        NErasure::TPartIndexList erasedIndices;
        erasedIndices.reserve(TotalPartCount_);
        for (int index = 0; index < TotalPartCount_; ++index) {
            if (!HasPartForRepair_[index]) {
                erasedIndices.push_back(index);
            }
        }

        auto repairIndices = Codec_->GetRepairIndices(erasedIndices);
        if (!repairIndices) {
            return;
        }

        std::vector<TSharedRef> blocks;
        blocks.reserve(repairIndices->size());
        for (int index = 0; index < std::ssize(fragments); ++index) {
            blocks.clear();
            for (auto partIndex : *repairIndices) {
                blocks.push_back(RepairFragmentParts_[index][partIndex]);
            }

            auto repairedBlocks = Codec_->Decode(blocks, erasedIndices);
            YT_VERIFY(repairedBlocks.size() == erasedIndices.size());
            for (int partIndexIndex = 0; partIndexIndex < std::ssize(erasedIndices); ++partIndexIndex) {
                auto partIndex = erasedIndices[partIndexIndex];
                RepairFragmentParts_[index][partIndex] = repairedBlocks[partIndexIndex];
            }
        }

        for (int index = 0; index < std::ssize(fragments); ++index) {
            auto fragmentStartOffset = std::move(RepairFragments_[index].BlockOffset);
            for (auto requestPart : RepairRequestIndexToRequestParts_[index]) {
                const auto& request = Requests_[requestPart.RequestIndex];
                if (request.Completed) {
                    continue;
                }

                auto partIndex = requestPart.PartIndex;
                auto relativePartIndex = partIndex - request.FirstPartIndex;
                if (request.Parts[relativePartIndex]) {
                    continue;
                }

                auto projection = request.GetProjection(partIndex);
                auto startOffset = projection.BlockOffset - fragmentStartOffset;
                auto endOffset = startOffset + projection.Length;
                auto part = RepairFragmentParts_[index][partIndex].Slice(startOffset, endOffset);
                SetResponsePart(requestPart.RequestIndex, partIndex, std::move(part));
            }
        }

        YT_VERIFY(Done_);
    }

    void SetResponsePart(
        int requestIndex,
        int partIndex,
        TSharedRef part)
    {
        YT_ASSERT(partIndex >= 0 && partIndex < DataPartCount_);

        auto& request = Requests_[requestIndex];
        YT_VERIFY(request.GetProjection(partIndex).Length == std::ssize(part));
        if (request.Completed) {
            return;
        }

        auto relativePartIndex = partIndex - request.FirstPartIndex;
        if (request.Parts[relativePartIndex]) {
            return;
        }

        request.Parts[relativePartIndex] = std::move(part);
        if (++request.FetchedPartCount == std::ssize(request.Parts)) {
            auto& response = (*ResponseFragments_)[request.FragmentIndex];
            if (std::ssize(request.Parts) == 1) {
                response = std::move(request.Parts.front());
            } else {
                struct TChunkFragmentReaderTag { };
                auto fragment = MergeRefsToRef<TChunkFragmentReaderTag>(MakeRange(request.Parts));
                response = std::move(fragment);
            }

            request.Parts = {};
            request.Completed = true;

            if (++CompletedRequestCount_ == std::ssize(Requests_)) {
                Done_ = true;
            }
        }
    }
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
                chunkId);
        }
        return std::make_unique<TErasureChunkFragmentReadController>(chunkId, erasureCodec, responseFragments);
    } else {
        if (erasureCodec != NErasure::ECodec::None) {
            THROW_ERROR_EXCEPTION("Unexpected erasure codec %Qlv specified for regular chunk %v",
                erasureCodec,
                chunkId);
        }
        return std::make_unique<TRegularChunkFragmentReadController>(chunkId, responseFragments);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
