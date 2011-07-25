#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"
#include "chunk_manager.pb.h"
#include "holder.h"
#include "chunk.h"

#include "../master/master_state_manager.h"
#include "../master/composite_meta_state.h"
#include "../master/meta_state_service.h"

#include "../transaction/transaction_manager.h"

#include "../rpc/server.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunkManager
    : public TMetaStateServiceBase
{
public:
    typedef TChunkManager TThis;
    typedef TIntrusivePtr<TThis> TPtr;
    typedef TChunkManagerConfig TConfig;

    //! Creates an instance.
    TChunkManager(
        const TConfig& config,
        TMasterStateManager::TPtr metaStateManager,
        TCompositeMetaState::TPtr metaState,
        IInvoker::TPtr serviceInvoker,
        NRpc::TServer::TPtr server,
        TTransactionManager::TPtr transactionManager);
 
private:
    typedef TChunkManager TThis;
    typedef TChunkManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;
    typedef yvector<THolder::TPtr> THolders;

    class TState;
    
    //! Configuration.
    TConfig Config;

    //! Manages transactions.
    TTransactionManager::TPtr TransactionManager;

    //! Meta-state.
    TIntrusivePtr<TState> State;

    //! Registers RPC methods.
    void RegisterMethods();

    TTransaction::TPtr GetTransaction(const TTransactionId& id, bool forUpdate = false);

    RPC_SERVICE_METHOD_DECL(NProto, RegisterHolder);
    void OnHolderRegistered(
        THolder::TPtr holder,
        TCtxRegisterHolder::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, HolderHeartbeat);
    void OnHolderUpdated(
        THolder::TPtr holder,
        TCtxRegisterHolder::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, AddChunk);
    void OnChunkAdded(
        TChunk::TPtr chunk,
        TCtxAddChunk::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, FindChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
