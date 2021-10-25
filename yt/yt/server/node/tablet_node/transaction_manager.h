#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hive/transaction_manager.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Transaction manager is tightly coupled to the tablet slot which acts as a host
//! for it. The following interface specifies methods of the tablet slot
//! required by the transaction manager and provides means for unit-testing of transaction manager.
struct ITransactionManagerHost
    : public virtual TRefCounted
{
    virtual NHydra::IDistributedHydraManagerPtr GetHydraManager() = 0;
    virtual const NHydra::TCompositeAutomatonPtr& GetAutomaton() = 0;
    virtual IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual const NHiveServer::ITransactionSupervisorPtr& GetTransactionSupervisor() = 0;
    virtual const TRuntimeTabletCellDataPtr& GetRuntimeData() = 0;
    virtual NTransactionClient::TTimestamp GetLatestTimestamp() = 0;
    virtual NObjectClient::TCellTag GetNativeCellTag() = 0;
    virtual NHydra::TCellId GetCellId() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionManagerHost)

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
        ITransactionManagerHostPtr host,
        NHiveServer::ITransactionLeaseTrackerPtr transactionLeaseTracker);
    ~TTransactionManager();

    //! Finds transaction by id.
    //! If it does not exist then creates a new transaction
    //! (either persistent or transient, depending on #transient).
    //! \param fresh An out-param indicating if the transaction was just-created.
    TTransaction* GetOrCreateTransaction(
        TTransactionId transactionId,
        TTimestamp startTimestamp,
        TDuration timeout,
        bool transient,
        bool* fresh = nullptr);

    //! Finds a transaction by id.
    //! If a persistent instance is found, just returns it.
    //! If a transient instance is found, makes is persistent and returns it.
    //! Fails if no transaction is found.
    TTransaction* MakeTransactionPersistent(TTransactionId transactionId);

    //! Removes a given #transaction, which must be transient.
    void DropTransaction(TTransaction* transaction);

    //! Returns the full list of transactions, including transient and persistent.
    std::vector<TTransaction*> GetTransactions();

    //! Schedules a mutation that creates a given transaction (if missing) and
    //! registers a set of actions.
    TFuture<void> RegisterTransactionActions(
        TTransactionId transactionId,
        TTimestamp transactionStartTimestamp,
        TDuration transactionTimeout,
        TTransactionSignature signature,
        ::google::protobuf::RepeatedPtrField<NTransactionClient::NProto::TTransactionActionData>&& actions);

    void RegisterTransactionActionHandlers(
        const NHiveServer::TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const NHiveServer::TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const NHiveServer::TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor);

    TTimestamp GetMinPrepareTimestamp();
    TTimestamp GetMinCommitTimestamp();

    void Decommission();
    bool IsDecommissioned() const;

    NYTree::IYPathServicePtr GetOrchidService();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

    /// ITransactionManager overrides.
    TFuture<void> GetReadyToPrepareTransactionCommit(
        const std::vector<TTransactionId>& prerequisiteTransactionIds,
        const std::vector<TCellId>& cellIdsToSyncWith) override;
    void PrepareTransactionCommit(
        TTransactionId transactionId,
        bool persistent,
        TTimestamp prepareTimestamp,
        const std::vector<TTransactionId>& prerequisiteTransactionIds) override;
    void PrepareTransactionAbort(
        TTransactionId transactionId,
        bool force) override;
    void CommitTransaction(
        TTransactionId transactionId,
        TTimestamp commitTimestamp) override;
    void AbortTransaction(
        TTransactionId transactionId,
        bool force) override;
    void PingTransaction(
        TTransactionId transactionId,
        bool pingAncestors) override;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
