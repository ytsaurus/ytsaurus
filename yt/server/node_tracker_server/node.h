#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/lease_manager.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/chunk_server/chunk_replica.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ENodeState,
    // Not registered.
    (Offline)
    // Registered but did not report the first heartbeat yet.
    (Registered)
    // Registered and reported the first heartbeat.
    (Online)
);

class TNode
{
    // Import third-party types into the scope.
    typedef NChunkServer::TChunkPtrWithIndex TChunkPtrWithIndex;
    typedef NChunkServer::TChunkId TChunkId;
    typedef NChunkServer::TJob TJob;

    DEFINE_BYVAL_RO_PROPERTY(TNodeId, Id);
    DEFINE_BYVAL_RW_PROPERTY(ENodeState, State);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeStatistics, Statistics);

    // Lease tracking.
    DEFINE_BYVAL_RW_PROPERTY(bool, Confirmed);
    DEFINE_BYVAL_RW_PROPERTY(TLeaseManager::TLease, Lease);

    // Chunk Manager stuff.
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkPtrWithIndex>, StoredReplicas);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkPtrWithIndex>, CachedReplicas);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkPtrWithIndex>, UnapprovedReplicas);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJob*>, Jobs);
    DEFINE_BYVAL_RW_PROPERTY(int, HintedSessionCount);

    //! Indexed by priority.
    typedef std::vector< yhash_set<TChunkId> > TChunksToReplicate;
    DEFINE_BYREF_RW_PROPERTY(TChunksToReplicate, ChunksToReplicate);

    //! NB: Ids are used instead of raw pointers since these chunks are typically already dead.
    typedef yhash_set<TChunkId> TChunksToRemove;
    DEFINE_BYREF_RW_PROPERTY(TChunksToRemove, ChunksToRemove);

public:
    TNode(
        TNodeId id,
        const TNodeDescriptor& descriptor);

    explicit TNode(TNodeId id);

    const TNodeDescriptor& GetDescriptor() const;
    const Stroka& GetAddress() const;

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

    // Chunk Manager stuff.
    void AddReplica(TChunkPtrWithIndex replica, bool cached);
    void RemoveReplica(TChunkPtrWithIndex replica, bool cached);
    bool HasReplica(TChunkPtrWithIndex, bool cached) const;

    void MarkReplicaUnapproved(TChunkPtrWithIndex replica);
    bool HasUnapprovedReplica(TChunkPtrWithIndex replica) const;
    void ApproveReplica(TChunkPtrWithIndex replica);

    void AddJob(TJob* job);
    void RemoveJob(TJob* id);

    int GetTotalSessionCount() const;

private:
    TNodeDescriptor Descriptor_;

    void Init();

};

TNodeId GetObjectId(const TNode* node);
bool CompareObjectsForSerialization(const TNode* lhs, const TNode* rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
