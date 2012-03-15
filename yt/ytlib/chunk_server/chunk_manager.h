#pragma once

#include "config.h"
#include "holder.h"
#include "chunk.h"
#include "chunk_list.h"
#include "job.h"
#include "job_list.h"
#include "chunk_service_proxy.h"
#include "holder_authority.h"
#include "chunk_manager.pb.h"
#include "holder_statistics.h"

#include <ytlib/actions/signal.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/meta_change.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkManager
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TChunkManager> TPtr;
    typedef TChunkManagerConfig TConfig;

    //! Creates an instance.
    TChunkManager(
        TConfig* config,
        NCellMaster::TBootstrap* bootstrap);

    NMetaState::TMetaChange< yvector<TChunkId> >::TPtr InitiateCreateChunks(
        const NProto::TMsgCreateChunks& message);

    NMetaState::TMetaChange<THolderId>::TPtr InitiateRegisterHolder(
        const NProto::TMsgRegisterHolder& message);

    NMetaState::TMetaChange<TVoid>::TPtr  InitiateUnregisterHolder(
        const NProto::TMsgUnregisterHolder& message);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateFullHeartbeat(
        const NProto::TMsgFullHeartbeat & message);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateIncrementalHeartbeat(
        const NProto::TMsgIncrementalHeartbeat& message);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateUpdateJobs(
        const NProto::TMsgUpdateJobs& message);

    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(Holder, THolder, THolderId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, TChunkId);
    DECLARE_METAMAP_ACCESSORS(Job, TJob, TJobId);

    //! Fired when a holder gets registered.
    /*!
     *  \note
     *  Only fired for leaders, not fired during recovery.
     */
    DECLARE_SIGNAL(void(const THolder&), HolderRegistered);
    //! Fired when a holder gets unregistered.
    /*!
     *  \note
     *  Only fired for leaders, not fired during recovery.
     */
    DECLARE_SIGNAL(void(const THolder&), HolderUnregistered);

    const THolder* FindHolder(const Stroka& address) const;
    THolder* FindHolder(const Stroka& address);
    const TReplicationSink* FindReplicationSink(const Stroka& address);

    yvector<THolderId> AllocateUploadTargets(int replicaCount);

    TChunk& CreateChunk();
    TChunkList& CreateChunkList();

    void AttachToChunkList(TChunkList& chunkList, const yvector<TChunkTreeId>& childrenIds);
    void DetachFromChunkList(TChunkList& chunkList, const yvector<TChunkTreeId>& childrenIds);

    void RunJobControl(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<NProto::TJobStartInfo>* jobsToStart,
        yvector<NProto::TJobStopInfo>* jobsToStop);

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

    TTotalHolderStatistics GetTotalHolderStatistics();

    bool IsHolderConfirmed(const THolder& holder);

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
