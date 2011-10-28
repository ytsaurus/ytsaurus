#pragma once

#include "common.h"
#include "holder.h"
#include "chunk.h"
#include "chunk_list.h"
#include "job.h"
#include "job_list.h"
#include "chunk_service_rpc.h"
#include "chunk_manager.pb.h"

#include "../meta_state/meta_change.h"

namespace NYT {
namespace NChunkServer {

using NMetaState::TMetaChange;

////////////////////////////////////////////////////////////////////////////////

using NMetaState::TMetaChange;
using NMetaState::TMetaStateManager;
using NMetaState::TCompositeMetaState;
using NTransaction::TTransactionManager;
using NTransaction::TTransactionId;
using NTransaction::TTransaction;

class TChunkManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkManager> TPtr;
    typedef TChunkManagerConfig TConfig;

    //! Creates an instance.
    TChunkManager(
        const TConfig& config,
        TMetaStateManager* metaStateManager,
        TCompositeMetaState* metaState,
        TTransactionManager* transactionManager);

    METAMAP_ACCESSORS_DECL(Chunk, TChunk, TChunkId);
    METAMAP_ACCESSORS_DECL(ChunkList, TChunkList, TChunkListId);
    METAMAP_ACCESSORS_DECL(Holder, THolder, THolderId);
    METAMAP_ACCESSORS_DECL(JobList, TJobList, TChunkId);
    METAMAP_ACCESSORS_DECL(Job, TJob, TJobId);

    const THolder* FindHolder(const Stroka& address);
    const TReplicationSink* FindReplicationSink(const Stroka& address);

    yvector<THolderId> AllocateUploadTargets(int replicaCount);

    TMetaChange<TChunkId>::TPtr InitiateCreateChunk(const TTransactionId& transactionId);

    TChunkList& CreateChunkList();

    void RefChunk(const TChunkId& chunkId);
    void RefChunk(TChunk& chunk);
    void UnrefChunk(const TChunkId& chunkId);
    void UnrefChunk(TChunk& chunk);

    void RefChunkList(const TChunkListId& chunkListId);
    void RefChunkList(TChunkList& chunkList);
    void UnrefChunkList(const TChunkListId& chunkListId);
    void UnrefChunkList(TChunkList& chunkList);

    TMetaChange<THolderId>::TPtr InitiateRegisterHolder(
        Stroka address,
        const NChunkHolder::THolderStatistics& statistics);
    TMetaChange<TVoid>::TPtr  InitiateUnregisterHolder(THolderId holderId);

    TMetaChange<TVoid>::TPtr InitiateHeartbeatRequest(const NProto::TMsgHeartbeatRequest& message);
    TMetaChange<TVoid>::TPtr InitiateHeartbeatResponse(const NProto::TMsgHeartbeatResponse& message);

    void RunJobControl(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<NProto::TJobStartInfo>* jobsToStart,
        yvector<TJobId>* jobsToStop);

private:
    class TImpl;
    
    TConfig Config;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
