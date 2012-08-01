#pragma once

#include "public.h"
#include "chunk_service_proxy.h"

#include <ytlib/chunk_server/chunk_manager.pb.h>
#include <ytlib/actions/signal.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/mutation.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/cell_master/public.h>
#include <ytlib/rpc/service.h>

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
    //! Creates an instance.
    TChunkManager(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    ~TChunkManager();

    NMetaState::TMutationPtr CreateRegisterNodeMutation(
        const NProto::TMetaReqRegisterNode& message);

    NMetaState::TMutationPtr CreateUnregisterNodeMutation(
        const NProto::TMetaReqUnregisterNode& message);

    // Pass RPC service context to full heartbeat handler to avoid copying request message.
    typedef NRpc::TTypedServiceContext<NProto::TReqFullHeartbeat, NProto::TRspFullHeartbeat> TCtxFullHeartbeat;
    typedef TIntrusivePtr<TCtxFullHeartbeat> TCtxFullHeartbeatPtr;
    NMetaState::TMutationPtr CreateFullHeartbeatMutation(
        TCtxFullHeartbeatPtr context);

    NMetaState::TMutationPtr CreateIncrementalHeartbeatMutation(
        const NProto::TMetaReqIncrementalHeartbeat& message);

    NMetaState::TMutationPtr CreateUpdateJobsMutation(
        const NProto::TMetaReqUpdateJobs& message);

    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(Node, THolder, TNodeId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, TChunkId);
    DECLARE_METAMAP_ACCESSORS(Job, TJob, TJobId);

    //! Fired when a node gets registered.
    /*!
     *  \note
     *  Only fired for leaders, not fired during recovery.
     */
    DECLARE_SIGNAL(void(const THolder*), NodeRegistered);
    //! Fired when a node gets unregistered.
    /*!
     *  \note
     *  Only fired for leaders, not fired during recovery.
     */
    DECLARE_SIGNAL(void(const THolder*), NodeUnregistered);

    //! Returns a node registered at the given address (|NULL| if none).
    THolder* FindNodeByAddress(const Stroka& address);

    //! Returns an arbitrary node registered at the host (|NULL| if none).
    THolder* FindNodeByHostName(const Stroka& hostName);

    const TReplicationSink* FindReplicationSink(const Stroka& address);

    std::vector<THolder*> AllocateUploadTargets(
        int nodeCount,
        TNullable<Stroka> preferredHostName);

    TChunk* CreateChunk();
    TChunkList* CreateChunkList();

    void AttachToChunkList(
        TChunkList* chunkList,
        const TChunkTreeRef* childrenBegin,
        const TChunkTreeRef* childrenEnd);
    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTreeRef>& children);
    void AttachToChunkList(
        TChunkList* chunkList,
        const TChunkTreeRef childRef);

    void ClearChunkList(TChunkList* chunkList);

    // TODO(babenko): consider making private
    void SetChunkTreeParent(TChunkList* parent, TChunkTreeRef childRef);
    void ResetChunkTreeParent(TChunkList* parent, TChunkTreeRef childRef);

    void ScheduleJobs(
        THolder* node,
        const std::vector<NProto::TJobInfo>& runningJobs,
        std::vector<NProto::TJobStartInfo>* jobsToStart,
        std::vector<NProto::TJobStopInfo>* jobsToStop);

    bool IsReplicatorEnabled();

    //! Fills a given protobuf structure with the list of data node addresses.
    /*!
     *  Not too nice but seemingly fast.
     */
    void FillNodeAddresses(
        ::google::protobuf::RepeatedPtrField< TProtoStringType>* addresses,
        const TChunk* chunk);

    const yhash_set<TChunkId>& LostChunkIds() const;
    const yhash_set<TChunkId>& OverreplicatedChunkIds() const;
    const yhash_set<TChunkId>& UnderreplicatedChunkIds() const;

    TTotalNodeStatistics GetTotalNodeStatistics();

    bool IsNodeConfirmed(const THolder* node);

    //! Returns the total number of all chunk replicas.
    i32 GetChunkReplicaCount();

private:
    class TImpl;
    class TChunkTypeHandler;
    class TChunkProxy;
    class TChunkListTypeHandler;
    class TChunkListProxy;
    
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
