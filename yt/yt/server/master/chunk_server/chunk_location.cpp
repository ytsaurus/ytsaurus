#include "chunk_location.h"

#include "chunk.h"
#include "domestic_medium.h"
#include "helpers.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

size_t TChunkLocation::TReplicaHasher::operator()(const TChunkPtrWithReplicaInfo& replica) const
{
    return TChunkPtrWithReplicaIndex(replica).GetHash();
}

////////////////////////////////////////////////////////////////////////////////

bool TChunkLocation::TReplicaEqual::operator()(const TChunkPtrWithReplicaInfo& lhs, const TChunkPtrWithReplicaInfo& rhs) const
{
    return lhs.GetPtr() == rhs.GetPtr() && lhs.GetReplicaIndex() == rhs.GetReplicaIndex();
}

////////////////////////////////////////////////////////////////////////////////

int TChunkLocation::GetEffectiveMediumIndex() const
{
    return Statistics_.medium_index();
}

void TChunkLocation::ReserveReplicas(int sizeHint)
{
    Replicas_.reserve(sizeHint);
    RandomReplicaIter_ = Replicas_.end();
}

bool TChunkLocation::AddReplica(TChunkPtrWithReplicaInfo replica)
{
    auto* chunk = replica.GetPtr();
    if (chunk->IsJournal()) {
        DoRemoveReplica(TChunkPtrWithReplicaIndex(chunk, replica.GetReplicaIndex()));
    }
    return DoAddReplica(replica);
}

bool TChunkLocation::DoAddReplica(TChunkPtrWithReplicaInfo replica)
{
    auto [it, inserted] = Replicas_.insert(replica);
    if (inserted) {
        RandomReplicaIter_ = it;
    }
    return inserted;
}

bool TChunkLocation::RemoveReplica(TChunkPtrWithReplicaIndex replica)
{
    DoRemoveReplica(replica);
    return UnapprovedReplicas_.erase(replica) == 0;
}

bool TChunkLocation::DoRemoveReplica(TChunkPtrWithReplicaIndex replica)
{
    auto it = Replicas_.find(TChunkPtrWithReplicaInfo(replica.GetPtr(), replica.GetReplicaIndex()));
    if (it == Replicas_.end()) {
        return false;
    }
    if (RandomReplicaIter_ == it) {
        ++RandomReplicaIter_;
    }
    Replicas_.erase(it);
    return true;
}

bool TChunkLocation::HasReplica(TChunkPtrWithReplicaIndex replica) const
{
    return Replicas_.contains(TChunkPtrWithReplicaInfo(replica.GetPtr(), replica.GetReplicaIndex()));
}

TChunkPtrWithReplicaInfo TChunkLocation::PickRandomReplica()
{
    if (Replicas_.empty()) {
        return TChunkPtrWithReplicaInfo();
    }
    if (RandomReplicaIter_ == Replicas_.end()) {
        RandomReplicaIter_ = Replicas_.begin();
    }
    auto replica = *(RandomReplicaIter_++);
    return replica;
}

void TChunkLocation::ShrinkHashTables()
{
    ShrinkHashTable(Replicas_);
    RandomReplicaIter_ = Replicas_.end();
    ShrinkHashTable(UnapprovedReplicas_);
    ShrinkHashTable(ChunkRemovalQueue_);
    ShrinkHashTable(ChunkSealQueue_);
}

void TChunkLocation::Reset()
{
    ChunkRemovalQueue_.clear();
    ResetDestroyedReplicasIterator();

    ChunkSealQueue_.clear();
}

void TChunkLocation::ClearReplicas()
{
    Replicas_.clear();
    UnapprovedReplicas_.clear();
    RandomReplicaIter_ = Replicas_.end();
    ClearDestroyedReplicas();
}

void TChunkLocation::AddUnapprovedReplica(TChunkPtrWithReplicaIndex replica, TInstant timestamp)
{
    EmplaceOrCrash(UnapprovedReplicas_, replica, timestamp);
}

bool TChunkLocation::HasUnapprovedReplica(TChunkPtrWithReplicaIndex replica) const
{
    return UnapprovedReplicas_.contains(replica);
}

void TChunkLocation::ApproveReplica(TChunkPtrWithReplicaInfo replica)
{
    auto* chunk = replica.GetPtr();
    TChunkPtrWithReplicaIndex genericState(chunk, replica.GetReplicaIndex());
    EraseOrCrash(UnapprovedReplicas_, genericState);
    if (chunk->IsJournal()) {
        // NB: We remove replica and add it again with possibly new replica state.
        DoRemoveReplica(genericState);
        YT_VERIFY(DoAddReplica(replica));
    }
}

void TChunkLocation::ClearDestroyedReplicas()
{
    DestroyedReplicas_ = {};
    ResetDestroyedReplicasIterator();
}

bool TChunkLocation::AddDestroyedReplica(const TChunkIdWithIndex& replica)
{
    RemoveFromChunkRemovalQueue(replica);
    auto shardId = GetChunkShardIndex(replica.Id);

    auto [it, inserted] = DestroyedReplicas_[shardId].insert(replica);
    if (!inserted) {
        return false;
    }
    DestroyedReplicasIterators_[shardId] = it;
    return true;
}

bool TChunkLocation::RemoveDestroyedReplica(const TChunkIdWithIndex& replica)
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

void TChunkLocation::AddToChunkRemovalQueue(const TChunkIdWithIndex& replica)
{
    YT_ASSERT(Node_->ReportedDataNodeHeartbeat());

    auto shardId = GetChunkShardIndex(replica.Id);
    if (DestroyedReplicas_[shardId].contains(replica)) {
        return;
    }

    ChunkRemovalQueue_.insert(replica);
}

void TChunkLocation::RemoveFromChunkRemovalQueue(const TChunkIdWithIndex& replica)
{
    ChunkRemovalQueue_.erase(replica);
}

void TChunkLocation::AddToChunkSealQueue(const TChunkIdWithIndex& replica)
{
    YT_ASSERT(Node_);
    YT_ASSERT(Node_->ReportedDataNodeHeartbeat());
    ChunkSealQueue_.insert(replica);
}

void TChunkLocation::RemoveFromChunkSealQueue(const TChunkIdWithIndex& replica)
{
    ChunkSealQueue_.erase(replica);
}

TChunkLocation::TLoadScratchData* TChunkLocation::GetLoadScratchData() const
{
    return LoadScratchData_.get();
}

void TChunkLocation::ResetLoadScratchData()
{
    LoadScratchData_.reset();
}

void TChunkLocation::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Node_);
    Save(context, DestroyedReplicas_);
    TSizeSerializer::Save(context, Replicas_.size());
    Save(context, UnapprovedReplicas_);
    Save(context, ReplicaEndorsements_);
    Save(context, Uuid_);
    Save(context, State_);
    Save(context, MediumOverride_);
    Save(context, Statistics_);
    Save(context, LastSeenTime_);
}

void TChunkLocation::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;

    Load(context, Node_);
    Load(context, DestroyedReplicas_);
    LoadScratchData_ = std::make_unique<TLoadScratchData>(TSizeSerializer::Load(context));
    Load(context, UnapprovedReplicas_);
    if (context.GetVersion() >= EMasterReign::PerLocationNodeHeartbeat) {
        Load(context, ReplicaEndorsements_);
    }
    Load(context, Uuid_);
    Load(context, State_);
    Load(context, MediumOverride_);
    Load(context, Statistics_);
    // COMPAT(koloshmet)
    if (context.GetVersion() >= EMasterReign::DanglingLocationsCleaning) {
        Load(context, LastSeenTime_);
    } else {
        LastSeenTime_ = TInstant::Max();
    }

    ResetDestroyedReplicasIterator();
}

TNode* TChunkLocation::SkipImaginaryChunkLocation(NCellMaster::TLoadContext& context)
{
    YT_VERIFY(context.GetVersion() < EMasterReign::DropImaginaryChunkLocations);

    using NYT::Load;

    auto owningNode = Load<NNodeTrackerServer::TNodeRawPtr>(context);

    // NB: Some compats are too old for trunk.
#if 0
    // COMPAT(danilalexeev)
    if (context.GetVersion() >= EMasterReign::MakeDestroyedReplicasSetSharded) {
        using NYT::Load;
        Load(context, DestroyedReplicas_);
    } else {
        TDestroyedReplicaSet preShardedDestroyedReplicaSet;
        Load(context, preShardedDestroyedReplicaSet);
        for (auto replica : preShardedDestroyedReplicaSet) {
            auto shardId = GetChunkShardIndex(replica.Id);
            EmplaceOrCrash(DestroyedReplicas_[shardId], replica);
        }
    }
#else
    Load<TDestroyedReplicaShardedSet>(context);
#endif

    // Scratch data size.
    TSizeSerializer::Load(context);

    Load<TUnapprovedReplicaMap>(context);

    return owningNode;
}

i64 TChunkLocation::GetDestroyedReplicasCount() const
{
    i64 count = 0;
    for (const auto& set : DestroyedReplicas_) {
        count += std::ssize(set);
    }
    return count;
}

const TChunkLocation::TDestroyedReplicaSet& TChunkLocation::GetDestroyedReplicaSet(int shardId) const
{
    return DestroyedReplicas_[shardId];
}

void TChunkLocation::SetDestroyedReplicasIterator(TDestroyedReplicasIterator iterator, int shardId)
{
    DestroyedReplicasIterators_[shardId] = iterator.Current_;
}

void TChunkLocation::ResetDestroyedReplicasIterator()
{
    static_assert(std::extent_v<TDestroyedReplicaShardedSet> == std::extent_v<decltype(DestroyedReplicasIterators_)>,
        "Sizes of DestroyedReplicas and DestroyedReplicasIterators are different");
    for (auto i = 0; i < std::ssize(DestroyedReplicasIterators_); ++i) {
        DestroyedReplicasIterators_[i] = DestroyedReplicas_[i].begin();
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkLocation::TLoadScratchData::TLoadScratchData(size_t replicasSize)
    : Replicas(replicasSize)
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkLocation::TDestroyedReplicasIterator TChunkLocation::GetDestroyedReplicasIterator(int shardId) const
{
    return TDestroyedReplicasIterator(DestroyedReplicas_[shardId], DestroyedReplicasIterators_[shardId]);
}

TChunkLocation::TDestroyedReplicasIterator::TDestroyedReplicasIterator(
    const TDestroyedReplicaSet& replicas,
    TDestroyedReplicaSet::const_iterator start)
    : Replicas_(&replicas)
    , Start_(start)
    , Current_(start)
{ }

TChunkIdWithIndex TChunkLocation::TDestroyedReplicasIterator::operator*() const
{
    return *Current_;
}

bool TChunkLocation::TDestroyedReplicasIterator::Advance()
{
    ++Current_;
    if (Current_ == Replicas_->end()) {
        Current_ = Replicas_->begin();
    }
    return Current_ != Start_;
}

TChunkLocation::TDestroyedReplicasIterator TChunkLocation::TDestroyedReplicasIterator::GetNext() const
{
    auto advanced = *this;
    advanced.Advance();
    return advanced;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
