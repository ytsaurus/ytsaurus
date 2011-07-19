#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"
#include "chunk_manager.pb.h"
#include "holder_tracker.h"
#include "chunk.h"

#include "../transaction/transaction_manager.h"

#include "../rpc/service.h"
#include "../rpc/server.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunkManager
    : public NRpc::TServiceBase
    , public NTransaction::ITransactionHandler
{
public:
    typedef TIntrusivePtr<TChunkManager> TPtr;
    typedef TChunkManagerConfig TConfig;

    //! Creates an instance.
    TChunkManager(
        const TConfig& config,
        NRpc::TServer::TPtr server,
        NTransaction::TTransactionManager::TPtr transactionManager);
 
private:
    typedef TChunkManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    class TState;
    
    //! Configuration.
    TConfig Config;

    NTransaction::TTransactionManager::TPtr TransactionManager;

    //! All state modifications are carried out via this invoker.
    IInvoker::TPtr ServiceInvoker;

    //! Meta-state.
    TIntrusivePtr<TState> State;

    //! Tracks holder liveness.
    THolderTracker::TPtr HolderTracker;

    THolder::TPtr GetHolder(int id);
    NTransaction::TTransaction::TPtr GetTransaction(const NTransaction::TTransactionId& id, bool forUpdate = false);

    void UpdateChunk(TChunk::TPtr chunk);
    void CleanupChunkLocations(TChunk::TPtr chunk);
    void AddChunkLocation(TChunk::TPtr chunk, THolder::TPtr holder);

    RPC_SERVICE_METHOD_DECL(NProto, RegisterHolder);
    RPC_SERVICE_METHOD_DECL(NProto, HolderHeartbeat);
    RPC_SERVICE_METHOD_DECL(NProto, AddChunk);
    RPC_SERVICE_METHOD_DECL(NProto, FindChunk);

    // ITransactionHandler
    virtual void OnTransactionStarted(NTransaction::TTransaction::TPtr transaction);
    virtual void OnTransactionCommitted(NTransaction::TTransaction::TPtr transaction);
    virtual void OnTransactionAborted(NTransaction::TTransaction::TPtr transaction);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
