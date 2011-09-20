#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"
#include "chunk_manager_rpc.pb.h"
#include "chunk_manager.pb.h"
#include "holder.h"
#include "chunk.h"
#include "job.h"

#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_state_service.h"
#include "../meta_state/map.h"
#include "../transaction/transaction_manager.h"
#include "../rpc/server.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunkPlacement;
class TChunkReplication;
class THolderExpiration;

class TChunkManager
    : public TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TChunkManager> TPtr;
    typedef TChunkManagerConfig TConfig;

    //! Creates an instance.
    TChunkManager(
        const TConfig& config,
        TMetaStateManager::TPtr metaStateManager,
        TCompositeMetaState::TPtr metaState,
        NRpc::TServer::TPtr server,
        TTransactionManager::TPtr transactionManager);

    void UnregisterHolder(THolderId holderId);

    METAMAP_ACCESSORS_DECL(Chunk, TChunk, TChunkId);
    METAMAP_ACCESSORS_DECL(Holder, THolder, THolderId);
    METAMAP_ACCESSORS_DECL(JobList, TJobList, TChunkId);
    METAMAP_ACCESSORS_DECL(Job, TJob, TJobId);

private:
    typedef TChunkManager TThis;
    typedef TChunkManagerProxy::EErrorCode EErrorCode;
    typedef yvector<THolderId> THolders;

    class TState;
    
    //! Configuration.
    TConfig Config;

    //! Manages transactions.
    TTransactionManager::TPtr TransactionManager;

    //! Manages placement of new chunks and rearranges existing ones.
    TIntrusivePtr<TChunkPlacement> ChunkPlacement;

    //! Manages chunk replication.
    TIntrusivePtr<TChunkReplication> ChunkReplication;

    //! Manages expiration of holder leases.
    TIntrusivePtr<THolderExpiration> HolderExpiration;

    //! Meta-state.
    TIntrusivePtr<TState> State;

    //! Registers RPC methods.
    void RegisterMethods();

    void ValidateHolderId(THolderId holderId);
    void ValidateTransactionId(const TTransactionId& transactionId);
    void ValidateChunkId(const TChunkId& chunkId, const TTransactionId& transactionId);

    RPC_SERVICE_METHOD_DECL(NProto, RegisterHolder);
    void OnHolderRegistered(
        THolderId id,
        TCtxRegisterHolder::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, HolderHeartbeat);
    void OnHolderHeartbeatProcessed(
        TVoid,
        TCtxHolderHeartbeat::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, AddChunk);
    void OnChunkAdded(
        TChunkId id,
        TCtxAddChunk::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, FindChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
