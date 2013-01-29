#pragma once

#include "public.h"
#include "chunk_service_proxy.h"

#include <ytlib/misc/small_vector.h>

#include <ytlib/actions/signal.h>

#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/mutation.h>
#include <ytlib/meta_state/map.h>

#include <ytlib/rpc/service.h>

#include <server/chunk_server/chunk_manager.pb.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {
    typedef TReqFullHeartbeat TMetaReqFullHeartbeat;
}

class TChunkManager
    : public TRefCounted
{
public:
    TChunkManager(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    ~TChunkManager();

    NMetaState::TMutationPtr CreateRegisterNodeMutation(
        const NProto::TMetaReqRegisterNode& request);

    NMetaState::TMutationPtr CreateUnregisterNodeMutation(
        const NProto::TMetaReqUnregisterNode& request);

    // Pass RPC service context to full heartbeat handler to avoid copying request message.
    typedef NRpc::TTypedServiceContext<NProto::TReqFullHeartbeat, NProto::TRspFullHeartbeat> TCtxFullHeartbeat;
    typedef TIntrusivePtr<TCtxFullHeartbeat> TCtxFullHeartbeatPtr;
    NMetaState::TMutationPtr CreateFullHeartbeatMutation(
        TCtxFullHeartbeatPtr context);

    NMetaState::TMutationPtr CreateIncrementalHeartbeatMutation(
        const NProto::TMetaReqIncrementalHeartbeat& request);

    NMetaState::TMutationPtr CreateUpdateJobsMutation(
        const NProto::TMetaReqUpdateJobs& reuqest);

    NMetaState::TMutationPtr CreateUpdateChunkReplicationFactorMutation(
        const NProto::TMetaReqUpdateChunkReplicationFactor& request);

    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(Node, TDataNode, TNodeId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, TChunkId);
    DECLARE_METAMAP_ACCESSORS(Job, TJob, TJobId);

    TChunkTreeRef GetChunkTree(const TChunkTreeId& id);

    //! Fired when a node gets registered.
    /*!
     *  \note
     *  Only fired for leaders, not fired during recovery.
     */
    DECLARE_SIGNAL(void(const TDataNode*), NodeRegistered);
    //! Fired when a node gets unregistered.
    /*!
     *  \note
     *  Only fired for leaders, not fired during recovery.
     */
    DECLARE_SIGNAL(void(const TDataNode*), NodeUnregistered);

    //! Returns a node registered at the given address (|NULL| if none).
    TDataNode* FindNodeByAddress(const Stroka& address);

    //! Returns an arbitrary node registered at the host (|NULL| if none).
    TDataNode* FindNodeByHostName(const Stroka& hostName);

    const TReplicationSink* FindReplicationSink(const Stroka& address);

    TSmallVector<TDataNode*, TypicalReplicationFactor> AllocateUploadTargets(
        int count,
        TNullable<Stroka> preferredHostName);

    TChunk* CreateChunk();
    TChunkList* CreateChunkList();

    void AttachToChunkList(
        TChunkList* chunkList,
        const TChunkTreeRef* childrenBegin,
        const TChunkTreeRef* childrenEnd,
        bool resetSorted = true);
    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTreeRef>& children,
        bool resetSorted = true);
    void AttachToChunkList(
        TChunkList* chunkList,
        const TChunkTreeRef childRef,
        bool resetSorted = true);

    void RebalanceChunkTree(TChunkList* chunkList);

    void ConfirmChunk(
        TChunk* chunk,
        const std::vector<Stroka>& addresses,
        NChunkClient::NProto::TChunkInfo* chunkInfo,
        NChunkClient::NProto::TChunkMeta* chunkMeta);

    void ClearChunkList(TChunkList* chunkList);

    void ScheduleJobs(
        TDataNode* node,
        const std::vector<NProto::TJobInfo>& runningJobs,
        std::vector<NProto::TJobStartInfo>* jobsToStart,
        std::vector<NProto::TJobStopInfo>* jobsToStop);

    bool IsReplicatorEnabled();

    void ScheduleRFUpdate(TChunkTreeRef ref);

    TSmallVector<Stroka, TypicalReplicationFactor> GetChunkAddresses(const TChunk* chunk);

    const yhash_set<TChunkId>& LostVitalChunkIds() const;
    const yhash_set<TChunkId>& LostChunkIds() const;
    const yhash_set<TChunkId>& OverreplicatedChunkIds() const;
    const yhash_set<TChunkId>& UnderreplicatedChunkIds() const;

    TTotalNodeStatistics GetTotalNodeStatistics();

    bool IsNodeConfirmed(const TDataNode* node);

    //! Returns the total number of all chunk replicas.
    int GetChunkReplicaCount();

    std::vector<NYPath::TYPath> GetOwningNodes(TChunkTreeRef ref);

private:
    class TImpl;
    class TChunkTypeHandler;
    class TChunkListTypeHandler;
    
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
