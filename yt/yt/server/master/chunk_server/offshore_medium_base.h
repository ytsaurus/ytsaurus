#pragma once

#include "public.h"

#include "medium_base.h"

#include <yt/yt/ytlib/chunk_client/config.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! TODO(achulkov2): Comment.
class TOffshoreMedium
    : public TMedium
{
public:
    using TMedium::TMedium;

    bool IsDomestic() const override;

public:
    // We need to store the following information about each replica:
    //   - chunk id
    //   - replica index (just in case we support erasure chunks at some point)
    //   - extra replica metadata (e.g. source path)
    // Medium index is not needed, because it's always the same for all replicas on a given medium.
    // TODO(achulkov2): For now TChunkIdWithIndex is enough (stores chunk id + replica index),
    // but once we have extra metadata, we'll need to migrate.
    using TDestroyedReplicaSet = THashSet<NChunkClient::TChunkIdWithIndex>;
    using TDestroyedReplicaShardedSet = std::array<TDestroyedReplicaSet, ChunkShardCount>;
    // TODO(achulkov2): Does this need to be public, if there is a per-shard accessor?
    DEFINE_BYREF_RO_PROPERTY(TDestroyedReplicaShardedSet, DestroyedReplicas);

    using TChunkQueue = THashSet<NChunkClient::TChunkIdWithIndex>;
    //! Key:
    //!   Encodes chunk and one of its parts (for erasure chunks only, others use GenericChunkReplicaIndex).
    //! Value:
    //!   Indicates medium where removal of this chunk is scheduled.
    //! TODO(achulkov2): Should this queue live in medium?
    //! Do we need public write access, if there designated methods?
    DEFINE_BYREF_RW_PROPERTY(TChunkQueue, ChunkRemovalQueue);

public:
    //! TODO(achulkov2): Can we unify handling of destroyed replicas with TChunkLocation?
    //! The problem is that the stored type will differ at some point, once we store extra replica metadata.
    //! We also want to keep serialization exactly the same for TChunkLocation.
    //! We can probably make a mixin templated by the stored replica type.

    bool AddDestroyedReplica(const NChunkClient::TChunkIdWithIndex& replica);
    bool RemoveDestroyedReplica(const NChunkClient::TChunkIdWithIndex& replica);
    void ClearDestroyedReplicas();

    // i64 GetDestroyedReplicasCount() const;
    const TDestroyedReplicaSet& GetDestroyedReplicaSet(int shardId) const;

    class TDestroyedReplicasIterator;
    void SetDestroyedReplicasIterator(TDestroyedReplicasIterator iterator, int shardId);
    void ResetDestroyedReplicasIterator();
    TDestroyedReplicasIterator GetDestroyedReplicasIterator(int shardId) const;

    void AddToChunkRemovalQueue(const NChunkClient::TChunkIdWithIndex& replica);
    void RemoveFromChunkRemovalQueue(const NChunkClient::TChunkIdWithIndex& replica);

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

private:
    std::array<TDestroyedReplicaSet::const_iterator, ChunkShardCount> DestroyedReplicasIterators_;
};

class TOffshoreMedium::TDestroyedReplicasIterator
{
public:
    friend class TOffshoreMedium;

    TDestroyedReplicasIterator(
        const TDestroyedReplicaSet& replicas,
        TDestroyedReplicaSet::const_iterator start);

    TDestroyedReplicasIterator(const TDestroyedReplicasIterator&) = default;
    TDestroyedReplicasIterator& operator=(const TDestroyedReplicasIterator&) = default;

    //! Returns current destroyed replica.
    NChunkClient::TChunkIdWithIndex operator*() const;

    //! Returns |false| if all replicas were traversed.
    bool Advance();

    //! Returns advanced iterator like std::next().
    TDestroyedReplicasIterator GetNext() const;

private:
    const TDestroyedReplicaSet* Replicas_;
    TDestroyedReplicaSet::const_iterator Start_;
    TDestroyedReplicaSet::const_iterator Current_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
