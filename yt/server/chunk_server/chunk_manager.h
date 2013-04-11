#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <ytlib/misc/small_vector.h>

#include <ytlib/actions/signal.h>

#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/mutation.h>
#include <ytlib/meta_state/map.h>

#include <ytlib/chunk_client/chunk_replica.h>

#include <server/object_server/public.h>

#include <server/chunk_server/chunk_manager.pb.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkManager
    : public TRefCounted
{
public:
    TChunkManager(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    NMetaState::TMutationPtr CreateUpdateJobsMutation(
        const NProto::TMetaReqUpdateJobs& reuqest);

    NMetaState::TMutationPtr CreateUpdateChunkReplicationFactorMutation(
        const NProto::TMetaReqUpdateChunkReplicationFactor& request);

    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, TChunkId);
    DECLARE_METAMAP_ACCESSORS(Job, TJob, TJobId);

    TChunkTree* FindChunkTree(const TChunkTreeId& id);
    TChunkTree* GetChunkTree(const TChunkTreeId& id);

    const TReplicationSink* FindReplicationSink(const Stroka& address);

    TSmallVector<TNode*, TypicalReplicationFactor> AllocateUploadTargets(
        int replicaCount,
        const TNullable<Stroka>& preferredHostName);

    TChunk* CreateChunk(NObjectServer::EObjectType type);
    TChunkList* CreateChunkList();

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree** childrenBegin,
        TChunkTree** childrenEnd,
        bool resetSorted = true);
    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children,
        bool resetSorted = true);
    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child,
        bool resetSorted = true);

    void RebalanceChunkTree(TChunkList* chunkList);

    void ConfirmChunk(
        TChunk* chunk,
        const std::vector<NChunkClient::TChunkReplica>& replicas,
        NChunkClient::NProto::TChunkInfo* chunkInfo,
        NChunkClient::NProto::TChunkMeta* chunkMeta);

    void ClearChunkList(TChunkList* chunkList);

    void ScheduleJobs(
        TNode* node,
        const std::vector<NNodeTrackerClient::NProto::TJobInfo>& runningJobs,
        std::vector<NNodeTrackerClient::NProto::TJobStartInfo>* jobsToStart,
        std::vector<NNodeTrackerClient::NProto::TJobStopInfo>* jobsToStop);

    bool IsReplicatorEnabled();

    void ScheduleRFUpdate(TChunkTree* chunkTree);

    TNodePtrWithIndexList GetChunkReplicas(const TChunk* chunk);

    const yhash_set<TChunk*>& LostVitalChunks() const;
    const yhash_set<TChunk*>& LostChunks() const;
    const yhash_set<TChunk*>& OverreplicatedChunks() const;
    const yhash_set<TChunk*>& UnderreplicatedChunks() const;

    //! Returns the total number of all chunk replicas.
    int GetChunkReplicaCount();

    std::vector<NYPath::TYPath> GetOwningNodes(TChunkTree* chunkTree);

private:
    class TImpl;
    class TChunkTypeHandlerBase;
    class TChunkTypeHandler;
    class TErasureChunkTypeHandler;
    class TChunkListTypeHandler;

    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
