#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <core/misc/small_vector.h>

#include <core/actions/signal.h>

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

    ~TChunkManager();

    void Initialize();

    NMetaState::TMutationPtr CreateUpdateChunkPropertiesMutation(
        const NProto::TMetaReqUpdateChunkProperties& request);

    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    TChunk* GetChunkOrThrow(const TChunkId& id);

    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);

    TChunkTree* FindChunkTree(const TChunkTreeId& id);
    TChunkTree* GetChunkTree(const TChunkTreeId& id);
    TChunkTree* GetChunkTreeOrThrow(const TChunkTreeId& id);

    TNodeList AllocateWriteTargets(
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

    TNodePtrWithIndexList LocateChunk(TChunkPtrWithIndex chunkWithIndex);

    void ClearChunkList(TChunkList* chunkList);

    TJobPtr FindJob(const TJobId& id);
    TJobListPtr FindJobList(TChunk* chunk);

    void ScheduleJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove);

    bool IsReplicatorEnabled();

    void SchedulePropertiesUpdate(TChunkTree* chunkTree);

    const yhash_set<TChunk*>& LostVitalChunks() const;
    const yhash_set<TChunk*>& LostChunks() const;
    const yhash_set<TChunk*>& OverreplicatedChunks() const;
    const yhash_set<TChunk*>& UnderreplicatedChunks() const;
    const yhash_set<TChunk*>& DataMissingChunks() const;
    const yhash_set<TChunk*>& ParityMissingChunks() const;

    //! Returns the total number of all chunk replicas.
    int GetTotalReplicaCount();

    std::vector<NYPath::TYPath> GetOwningNodes(TChunkTree* chunkTree);

    EChunkStatus ComputeChunkStatus(TChunk* chunk);

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
