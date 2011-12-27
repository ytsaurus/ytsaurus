#pragma once

#include "common.h"
#include "holder.h"
#include "chunk.h"
#include "chunk_list.h"
#include "job.h"
#include "job_list.h"
#include "chunk_service_proxy.h"
#include "holder_authority.h"
#include "chunk_manager.pb.h"

#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_change.h"
#include "../transaction_server/transaction_manager.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkManager> TPtr;
    typedef TChunkManagerConfig TConfig;
    typedef NProto::TReqHolderHeartbeat::TJobInfo TJobInfo;
    typedef NProto::TRspHolderHeartbeat::TJobStartInfo TJobStartInfo;

    //! Creates an instance.
    TChunkManager(
        TConfig* config,
        NMetaState::IMetaStateManager* metaStateManager,
        NMetaState::TCompositeMetaState* metaState,
        NTransactionServer::TTransactionManager* transactionManager,
        IHolderRegistry* holderRegistry);

    // TODO: provide Stop method

    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, NChunkClient::TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(Holder, THolder, THolderId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, NChunkClient::TChunkId);
    DECLARE_METAMAP_ACCESSORS(Job, TJob, NChunkHolder::TJobId);

    //! Fired when a holder gets registered.
    /*!
     *  \note
     *  Only fired for leaders, not fired during recovery.
     */
    DECLARE_BYREF_RW_PROPERTY(TParamSignal<const THolder&>, HolderRegistered);
    //! Fired when a holder gets unregistered.
    /*!
     *  \note
     *  Only fired for leaders, not fired during recovery.
     */
    DECLARE_BYREF_RW_PROPERTY(TParamSignal<const THolder&>, HolderUnregistered);

    const THolder* FindHolder(const Stroka& address);
    const TReplicationSink* FindReplicationSink(const Stroka& address);

    yvector<THolderId> AllocateUploadTargets(int replicaCount);

    NMetaState::TMetaChange<NChunkClient::TChunkId>::TPtr InitiateAllocateChunk(
        const NTransactionServer::TTransactionId& transactionId);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateConfirmChunks(
        const NProto::TMsgConfirmChunks& message);

    TChunkList& CreateChunkList();
    void AddChunkToChunkList(TChunk& chunk, TChunkList& chunkList);

    void RefChunk(const NChunkClient::TChunkId& chunkId);
    void RefChunk(TChunk& chunk);
    void UnrefChunk(const NChunkClient::TChunkId& chunkId);
    void UnrefChunk(TChunk& chunk);

    void RefChunkList(const TChunkListId& chunkListId);
    void RefChunkList(TChunkList& chunkList);
    void UnrefChunkList(const TChunkListId& chunkListId);
    void UnrefChunkList(TChunkList& chunkList);

    NMetaState::TMetaChange<THolderId>::TPtr InitiateRegisterHolder(
        Stroka address,
        const NChunkServer::NProto::THolderStatistics& statistics);
    NMetaState::TMetaChange<TVoid>::TPtr  InitiateUnregisterHolder(THolderId holderId);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateHeartbeatRequest(
        const NProto::TMsgHeartbeatRequest& message);
    NMetaState::TMetaChange<TVoid>::TPtr InitiateHeartbeatResponse(
        const NProto::TMsgHeartbeatResponse& message);

    void RunJobControl(
        const THolder& holder,
        const yvector<TJobInfo>& runningJobs,
        yvector<TJobStartInfo>* jobsToStart,
        yvector<NChunkHolder::TJobId>* jobsToStop);

    //! Fills a given protobuf structure with the list of holder addresses.
    /*!
     *  Not too nice but seemingly fast.
     */
    void FillHolderAddresses(
        ::google::protobuf::RepeatedPtrField< TProtoStringType>* addresses,
        const TChunk& chunk);

    const yhash_set<NChunkClient::TChunkId>& LostChunkIds() const;
    const yhash_set<NChunkClient::TChunkId>& OverReplicatedChunkIds() const;
    const yhash_set<NChunkClient::TChunkId>& UnderReplicatedChunkIds() const;

private:
    class TImpl;
    
    TConfig::TPtr Config;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
