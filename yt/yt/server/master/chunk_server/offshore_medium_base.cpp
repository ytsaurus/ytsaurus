#include "offshore_medium_base.h"

#include <yt/yt/server/master/chunk_server/helpers.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

bool TOffshoreMedium::IsDomestic() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

void TOffshoreMedium::ClearDestroyedReplicas()
{
    DestroyedReplicas_ = {};
    ResetDestroyedReplicasIterator();
}

bool TOffshoreMedium::AddDestroyedReplica(const TChunkIdWithIndex& replica)
{
    // TODO(achulkov2): Should we keep the joint removal-destroyed-replica implementation?
    RemoveFromChunkRemovalQueue(replica);
    auto shardId = GetChunkShardIndex(replica.Id);

    auto [it, inserted] = DestroyedReplicas_[shardId].insert(replica);
    if (!inserted) {
        return false;
    }
    DestroyedReplicasIterators_[shardId] = it;
    return true;
}

bool TOffshoreMedium::RemoveDestroyedReplica(const TChunkIdWithIndex& replica)
{
    auto shardId = GetChunkShardIndex(replica.Id);
    auto& destroyedReplicas = DestroyedReplicas_[shardId];
    auto& destroyedReplicasIterator = DestroyedReplicasIterators_[shardId];
    if (!destroyedReplicas.empty() && *destroyedReplicasIterator == replica) {
        if (destroyedReplicas.size() == 1) {
            destroyedReplicasIterator = destroyedReplicas.end();
        } else {
            auto it = GetDestroyedReplicasIterator(shardId);
            it.Advance();
            SetDestroyedReplicasIterator(it, shardId);
        }
    }
    return destroyedReplicas.erase(replica) > 0;
}

void TOffshoreMedium::AddToChunkRemovalQueue(const TChunkIdWithIndex& replica)
{
    // TODO(achulkov2): TChunkLocation implementation has a node-related assert here.
    // How do we keep it in a shared interface? Or maybe it is fine to drop it...

    auto shardId = GetChunkShardIndex(replica.Id);
    if (DestroyedReplicas_[shardId].contains(replica)) {
        return;
    }

    ChunkRemovalQueue_.insert(replica);
}

void TOffshoreMedium::RemoveFromChunkRemovalQueue(const TChunkIdWithIndex& replica)
{
    ChunkRemovalQueue_.erase(replica);
}

////////////////////////////////////////////////////////////////////////////////

void TOffshoreMedium::Save(NCellMaster::TSaveContext& context) const
{
    TMedium::Save(context);

    using NYT::Save;

    Save(context, DestroyedReplicas_);
}

void TOffshoreMedium::Load(NCellMaster::TLoadContext& context)
{
    TMedium::Load(context);

    using NYT::Load;

    // TODO(achulkov2): Compat for field introduction.
    Load(context, DestroyedReplicas_);
}


////////////////////////////////////////////////////////////////////////////////

TOffshoreMedium::TDestroyedReplicasIterator TOffshoreMedium::GetDestroyedReplicasIterator(int shardId) const
{
    return TDestroyedReplicasIterator(DestroyedReplicas_[shardId], DestroyedReplicasIterators_[shardId]);
}

const TOffshoreMedium::TDestroyedReplicaSet& TOffshoreMedium::GetDestroyedReplicaSet(int shardId) const
{
    return DestroyedReplicas_[shardId];
}

void TOffshoreMedium::SetDestroyedReplicasIterator(TDestroyedReplicasIterator iterator, int shardId)
{
    DestroyedReplicasIterators_[shardId] = iterator.Current_;
}

void TOffshoreMedium::ResetDestroyedReplicasIterator()
{
    static_assert(std::extent_v<TDestroyedReplicaShardedSet> == std::extent_v<decltype(DestroyedReplicasIterators_)>,
        "Sizes of DestroyedReplicas and DestroyedReplicasIterators are different");
    for (auto i = 0; i < std::ssize(DestroyedReplicasIterators_); ++i) {
        DestroyedReplicasIterators_[i] = DestroyedReplicas_[i].begin();
    }
}

////////////////////////////////////////////////////////////////////////////////

TOffshoreMedium::TDestroyedReplicasIterator::TDestroyedReplicasIterator(
    const TDestroyedReplicaSet& replicas,
    TDestroyedReplicaSet::const_iterator start)
    : Replicas_(&replicas)
    , Start_(start)
    , Current_(start)
{ }

TChunkIdWithIndex TOffshoreMedium::TDestroyedReplicasIterator::operator*() const
{
    return *Current_;
}

bool TOffshoreMedium::TDestroyedReplicasIterator::Advance()
{
    ++Current_;
    if (Current_ == Replicas_->end()) {
        Current_ = Replicas_->begin();
    }
    return Current_ != Start_;
}

TOffshoreMedium::TDestroyedReplicasIterator TOffshoreMedium::TDestroyedReplicasIterator::GetNext() const
{
    auto advanced = *this;
    advanced.Advance();
    return advanced;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
