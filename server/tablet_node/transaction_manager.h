#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/hive/transaction_manager.h>

#include <yt/server/hydra/composite_automaton.h>
#include <yt/server/hydra/entity_map.h>

#include <yt/core/actions/signal.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public NHiveServer::ITransactionManager
{
public:
    //! Raised when a new transaction is started.
    DECLARE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is prepared.
    DECLARE_SIGNAL(void(TTransaction*, bool), TransactionPrepared);

    //! Raised when a transaction is committed.
    DECLARE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is serialized by a barrier.
    DECLARE_SIGNAL(void(TTransaction*), TransactionSerialized);

    //! Raised when a transaction is aborted.
    DECLARE_SIGNAL(void(TTransaction*), TransactionAborted);

    //! Raised on epoch finish for each transaction (both persistent and transient)
    //! to help all dependent subsystems to reset their transient transaction-related
    //! state.
    DECLARE_SIGNAL(void(TTransaction*), TransactionTransientReset);

public:
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        TTabletSlotPtr slot,
        NCellNode::TBootstrap* bootstrap);
    ~TTransactionManager();

    using TCtxRegisterTransactionActions = NRpc::TTypedServiceContext<
        NTabletClient::NProto::TReqRegisterTransactionActions,
        NTabletClient::NProto::TRspRegisterTransactionActions>;
    using TCtxRegisterTransactionActionsPtr = TIntrusivePtr<TCtxRegisterTransactionActions>;
    std::unique_ptr<NHydra::TMutation> CreateRegisterTransactionActionsMutation(TCtxRegisterTransactionActionsPtr context);

    //! Finds transaction by id.
    //! If it does not exist then creates a new transaction
    //! (either persistent or transient, depending on #transient).
    //! \param fresh An out-param indicating if the transaction was just-created.
    TTransaction* GetOrCreateTransaction(
        const TTransactionId& transactionId,
        TTimestamp startTimestamp,
        TDuration timeout,
        bool transient,
        const TString& user = TString(),
        bool* fresh = nullptr);

    //! Finds a transaction by id.
    //! If a persistent instance is found, just returns it.
    //! If a transient instance is found, makes is persistent and returns it.
    //! Fails if no transaction is found.
    TTransaction* MakeTransactionPersistent(const TTransactionId& transactionId);

    //! Removes a given #transaction, which must be transient.
    void DropTransaction(TTransaction* transaction);

    //! Returns the full list of transactions, including transient and persistent.
    std::vector<TTransaction*> GetTransactions();

    void RegisterTransactionActionHandlers(
        const NHiveServer::TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const NHiveServer::TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const NHiveServer::TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor);

    TTimestamp GetMinPrepareTimestamp();
    TTimestamp GetMinCommitTimestamp();

    NYTree::IYPathServicePtr GetOrchidService();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

    /// ITransactionManager overrides.
    virtual void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        bool persistent,
        TTimestamp prepareTimestamp) override;
    virtual void PrepareTransactionAbort(
        const TTransactionId& transactionId,
        bool force) override;
    virtual void CommitTransaction(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp) override;
    virtual void AbortTransaction(
        const TTransactionId& transactionId,
        bool force) override;
    virtual void PingTransaction(
        const TTransactionId& transactionId,
        bool pingAncestors) override;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
