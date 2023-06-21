#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_manager.h>

#include <yt/yt/ytlib/api/native/public.h>

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
    virtual NHydra::ISimpleHydraManagerPtr GetSimpleHydraManager() = 0;
    virtual const NHydra::TCompositeAutomatonPtr& GetAutomaton() = 0;
    virtual IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) = 0;
    virtual const NTransactionSupervisor::ITransactionSupervisorPtr& GetTransactionSupervisor() = 0;
    virtual const TRuntimeTabletCellDataPtr& GetRuntimeData() = 0;
    virtual NTransactionClient::TTimestamp GetLatestTimestamp() = 0;
    virtual NObjectClient::TCellTag GetNativeCellTag() = 0;
    virtual const NApi::NNative::IConnectionPtr& GetNativeConnection() = 0;
    virtual NHydra::TCellId GetCellId() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionManagerHost)

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public NTransactionSupervisor::ITransactionManager
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

    //! Raised just before TransactionSerialized.
    DECLARE_SIGNAL(void(TTransaction*), BeforeTransactionSerialized);

    //! Raised when a transaction is aborted.
    DECLARE_SIGNAL(void(TTransaction*), TransactionAborted);

    //! Raised when transaction barrier is promoted.
    DECLARE_SIGNAL(void(TTimestamp), TransactionBarrierHandled);

    //! Raised on epoch finish for each transaction (both persistent and transient)
    //! to help all dependent subsystems to reset their transient transaction-related
    //! state.
    DECLARE_SIGNAL(void(TTransaction*), TransactionTransientReset);

    /// ITransactionManager overrides.
    TFuture<void> GetReadyToPrepareTransactionCommit(
        const std::vector<TTransactionId>& prerequisiteTransactionIds,
        const std::vector<TCellId>& cellIdsToSyncWith) override;
    void PrepareTransactionCommit(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionPrepareOptions& options) override;
    void PrepareTransactionAbort(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionAbortOptions& options) override;
    void CommitTransaction(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionCommitOptions& options) override;
    void AbortTransaction(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionAbortOptions& options) override;
    void PingTransaction(
        TTransactionId transactionId,
        bool pingAncestors) override;

    bool CommitTransaction(TCtxCommitTransactionPtr context) override;
    bool AbortTransaction(TCtxAbortTransactionPtr context) override;

public:
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        ITransactionManagerHostPtr host,
        NApi::TClusterTag clockClusterTag,
        NTransactionSupervisor::ITransactionLeaseTrackerPtr transactionLeaseTracker);
    ~TTransactionManager();

    //! Finds transaction by id.
    //! If it does not exist then creates a new transaction
    //! (either persistent or transient, depending on #transient).
    //! Throws if tablet cell is decommissioned or suspended.
    TTransaction* GetOrCreateTransactionOrThrow(
        TTransactionId transactionId,
        TTimestamp startTimestamp,
        TDuration timeout,
        bool transient);

    //! Finds a transaction by id.
    //! If a persistent instance is found, just returns it.
    //! If a transient instance is found or no transaction is found
    //! returns |nullptr|.
    TTransaction* FindPersistentTransaction(TTransactionId transcationId);

    //! Finds a transaction by id.
    //! If a persistent instance is found, just returns it.
    //! Fails if a transient instance is found or no transaction is found.
    TTransaction* GetPersistentTransaction(TTransactionId transactionId);

    //! Finds a transaction by id.
    //! If a persistent instance is found, just returns it.
    //! If a transient instance is found, makes is persistent and returns it.
    //! Fails if no transaction is found.
    //! Throws if tablet cell is decommissioned or suspended.
    TTransaction* MakeTransactionPersistentOrThrow(TTransactionId transactionId);

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
        const NTransactionSupervisor::TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const NTransactionSupervisor::TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const NTransactionSupervisor::TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor);

    void RegisterTransactionActionHandlers(
        const NTransactionSupervisor::TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const NTransactionSupervisor::TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const NTransactionSupervisor::TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor,
        const NTransactionSupervisor::TTransactionSerializeActionHandlerDescriptor<TTransaction>& serializeActionDescriptor);

    //! Increases transaction commit signature.
    // NB: After incrementing transaction may become committed and destroyed.
    void IncrementCommitSignature(TTransaction* transaction, TTransactionSignature delta);

    TTimestamp GetMinPrepareTimestamp();
    TTimestamp GetMinCommitTimestamp();

    void SetDecommission(bool decommission);
    bool GetDecommission() const;
    void SetRemoving();

    //! Returns true if transaction manager is decommissioned and threre are
    //! no alive transactions in it, so tablet cell can be safely removed.
    bool IsDecommissioned() const;

    void AddTransientAffectedTablet(TTransaction* transaction, TTablet* tablet);
    void AddPersistentAffectedTablet(TTransaction* transaction, TTablet* tablet);

    // COMPAT(gritukan)
    ETabletReign GetSnapshotReign() const;

    NYTree::IYPathServicePtr GetOrchidService();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTransactionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
