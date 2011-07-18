#pragma once

#include "common.h"
#include "transaction.h"
#include "transaction_manager_rpc.h"
#include "transaction_manager.pb.h"

#include "../rpc/service.h"
#include "../rpc/server.h"

#include "../misc/lease_manager.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

//! Manages user transactions.
class TTransactionManager
    : public NRpc::TServiceBase
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
        NRpc::TServer* server);

    //! Registers a handler.
    /*!
     * Each handler gets notified when a transaction is started, committed or aborted.
     */
    void RegisterHander(ITransactionHandler::TPtr handler);

    //! Finds transaction by id. Returns NULL if not found.
    /*!
     * If a transaction is found, its lease is renewed automatically.
     */
    TTransaction::TPtr FindTransaction(TTransactionId id, bool forUpdate = false);

private:
    typedef TTransactionManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    class TState;
    
    typedef yvector<ITransactionHandler::TPtr> THandlers;

    //! Configuration.
    TConfig Config;

    //! All state modifications are carried out via this invoker.
    IInvoker::TPtr ServiceInvoker;

    //! Controls leases of running transactions.
    TLeaseManager::TPtr LeaseManager;

    //! Meta-state.
    TIntrusivePtr<TState> State;

    //! Registered handlers.
    THandlers Handlers;

    TTransaction::TPtr DoStartTransaction();
    void DoCommitTransaction(TTransaction::TPtr transaction);
    void DoAbortTransaction(TTransaction::TPtr transaction);
    void DoRenewTransactionLease(TTransaction::TPtr transaction);

    void OnTransactionExpired(TTransaction::TPtr transaction);

    RPC_SERVICE_METHOD_DECL(NProto, StartTransaction);
    RPC_SERVICE_METHOD_DECL(NProto, CommitTransaction);
    RPC_SERVICE_METHOD_DECL(NProto, AbortTransaction);
    RPC_SERVICE_METHOD_DECL(NProto, RenewTransactionLease);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
