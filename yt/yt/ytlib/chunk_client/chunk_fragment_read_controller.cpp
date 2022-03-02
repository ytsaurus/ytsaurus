#include "chunk_fragment_read_controller.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TChunkFragmentReadController::TChunkFragmentReadController(
    TChunkId chunkId,
    std::vector<TSharedRef>* responseFragments)
    : ChunkId_(chunkId)
    , ResponseFragments_(responseFragments)
{ }

TChunkId TChunkFragmentReadController::GetChunkId() const
{
    return ChunkId_;
}

void TChunkFragmentReadController::RegisterRequest(const TFragmentRequest& request)
{
    FragmentRequests_.push_back(request);
}

void TChunkFragmentReadController::SetReplicas(const TChunkReplicaInfoList& replicas)
{
    Peers_ = replicas;
}

const TChunkReplicaInfo& TChunkFragmentReadController::GetReplica(int peerIndex)
{
    return Peers_[peerIndex];
}

std::optional<TChunkFragmentReadController::TPlan> TChunkFragmentReadController::TryMakePlan()
{
    if (CurrentPeerIndex_ >= std::ssize(Peers_)) {
        return {};
    }
    return TPlan{CurrentPeerIndex_++};
}

bool TChunkFragmentReadController::IsDone() const
{
    return Done_;
}

int TChunkFragmentReadController::PrepareRpcSubrequest(
    int /*peerIndex*/,
    NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest* subrequest)
{
    ToProto(subrequest->mutable_chunk_id(), ChunkId_);
    for (const auto& fragmentRequest : FragmentRequests_) {
        auto* fragment = subrequest->add_fragments();
        fragment->set_length(fragmentRequest.Length);
        fragment->set_block_index(fragmentRequest.BlockIndex);
        fragment->set_block_offset(fragmentRequest.BlockOffset);
    }
    return std::ssize(FragmentRequests_);
}

void TChunkFragmentReadController::HandleRpcSubresponse(
    int /*peerIndex*/,
    const NChunkClient::NProto::TRspGetChunkFragmentSet_TSubresponse& subresponse,
    TMutableRange<TSharedRef> fragments)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
