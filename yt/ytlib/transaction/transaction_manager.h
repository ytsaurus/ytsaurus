#pragma once

#include "common.h"
#include "transaction.h"
#include "transaction_manager_rpc.h"

#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_state_service.h"
#include "../meta_state/map.h"

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
        TMetaStateManager::TPtr metaStateManager,
        TCompositeMetaState::TPtr metaState,
        IInvoker::TPtr serviceInvoker,
        NRpc::TServer::TPtr server);

    //! Registers a handler.
    /*!
     * Each handler gets notified when a transaction is started, committed or aborted.
     */
    void RegisterHander(ITransactionHandler::TPtr handler);

    METAMAP_ACCESSORS_DECL(Transaction, TTransaction, TTransactionId);

private:
    typedef TTransactionManager TThis;
    typedef TTransactionManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    class TState;
    
    //! Meta-state.
    TIntrusivePtr<TState> State;

    void ValidateTransactionId(const TTransactionId& id);

    RPC_SERVICE_METHOD_DECL(NProto, StartTransaction);
    void OnTransactionStarted(
        TTransactionId id,
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
