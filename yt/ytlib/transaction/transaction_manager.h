#pragma once

#include "common.h"
#include "transaction.h"
#include "transaction_manager_rpc.h"

#include "../master/master_state_manager.h"
#include "../master/composite_meta_state.h"
#include "../master/meta_state_service.h"

#include "../rpc/server.h"

#include "../misc/lease_manager.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////
    
//! Manages user transactions.
class TTransactionManager
    : public TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TTransactionManager> TPtr;

    struct TConfig
    {
        TDuration TransactionTimeout;

        TConfig()
            : TransactionTimeout(TDuration::Seconds(15))
        { }
    };

    //! Creates an instance.
    TTransactionManager(
        const TConfig& config,
        TMasterStateManager::TPtr metaStateManager,
        TCompositeMetaState::TPtr metaState,
        IInvoker::TPtr serviceInvoker,
        NRpc::TServer::TPtr server);

    //! Registers a handler.
    /*!
     * Each handler gets notified when a transaction is started, committed or aborted.
     */
    void RegisterHander(ITransactionHandler::TPtr handler);

    //! Finds transaction by id. Returns NULL if not found.
    /*!
     * If a transaction is found, its lease is renewed automatically.
     */
    TTransaction::TPtr FindTransaction(const TTransactionId& id, bool forUpdate = false);

private:
    typedef TTransactionManager TThis;
    typedef TTransactionManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    class TState;
    
    //! Meta-state.
    TIntrusivePtr<TState> State;

    RPC_SERVICE_METHOD_DECL(NProto, StartTransaction);
    void OnTransactionStarted(
        TTransaction::TPtr transaction,
        TCtxStartTransaction::TPtr context);

    RPC_SERVICE_METHOD_DECL(NProto, CommitTransaction);
    void OnTransactionCommitted(
        TVoid,
        TCtxCommitTransaction::TPtr context);

    RPC_SERVICE_METHOD_DECL(NProto, AbortTransaction);
    void OnTransactionAborted(
        TVoid,
        TCtxAbortTransaction::TPtr context);

    RPC_SERVICE_METHOD_DECL(NProto, RenewTransactionLease);

    //! Registers RPC methods.
    void RegisterMethods();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
