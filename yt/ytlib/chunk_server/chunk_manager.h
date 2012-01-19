#pragma once

#include "common.h"
#include "config.h"
#include "holder.h"
#include "chunk.h"
#include "chunk_list.h"
#include "job.h"
#include "job_list.h"
#include "chunk_service_proxy.h"
#include "holder_authority.h"
#include "chunk_manager.pb.h"

#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/meta_change.h>
#include <ytlib/transaction_server/transaction_manager.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// Simple aliases that make sense as long as request and message contracts are the same.
namespace NProto {
    typedef NProto::TReqCreateChunkLists TMsgCreateChunkLists;
    typedef NProto::TReqAttachChunkTrees TMsgAttachChunkTrees;
    typedef NProto::TReqDetachChunkTrees TMsgDetachChunkTrees;
} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

class TChunkManager
    : public TExtrinsicRefCounted
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

    NMetaState::TMetaChange< yvector<TChunkId> >::TPtr InitiateCreateChunks(
        const NProto::TMsgCreateChunks& message);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateConfirmChunks(
        const NProto::TMsgConfirmChunks& message);

    NMetaState::TMetaChange< yvector<TChunkListId> >::TPtr InitiateCreateChunkLists(
        const NProto::TMsgCreateChunkLists& message);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateAttachChunkTrees(
        const NProto::TMsgAttachChunkTrees& message);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateDetachChunkTrees(
        const NProto::TMsgDetachChunkTrees& message);

    NMetaState::TMetaChange<THolderId>::TPtr InitiateRegisterHolder(
        const NProto::TMsgRegisterHolder& message);

    NMetaState::TMetaChange<TVoid>::TPtr  InitiateUnregisterHolder(
        const NProto::TMsgUnregisterHolder& message);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateHeartbeatRequest(
        const NProto::TMsgHeartbeatRequest& message);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateHeartbeatResponse(
        const NProto::TMsgHeartbeatResponse& message);

    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(Holder, THolder, THolderId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, TChunkId);
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

    TChunkList& CreateChunkList();

    void RefChunkTree(const TChunkTreeId& treeId);
    void UnrefChunkTree(const TChunkTreeId& treeId);

    void RefChunkTree(TChunk& chunk);
    void UnrefChunkTree(TChunk& chunk);

    void RefChunkTree(TChunkList& chunkList);
    void UnrefChunkTree(TChunkList& chunkList);

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

    const yhash_set<TChunkId>& LostChunkIds() const;
    const yhash_set<TChunkId>& OverreplicatedChunkIds() const;
    const yhash_set<TChunkId>& UnderreplicatedChunkIds() const;

    TConfig::TPtr Config;

private:
    class TImpl;

    TIntrusivePtr<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
