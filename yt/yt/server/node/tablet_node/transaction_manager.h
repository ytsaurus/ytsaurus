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

struct ITransactionManager
    : public NTransactionSupervisor::ITransactionManager
{
    //! Raised when a new transaction is started.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is prepared.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*, bool), TransactionPrepared);

    //! Raised when a transaction is committed.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is serialized by a barrier.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*), TransactionSerialized);

    //! Raised just before TransactionSerialized.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*), BeforeTransactionSerialized);

    //! Raised when a transaction is aborted.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*), TransactionAborted);

    //! Raised when transaction barrier is promoted.
    DECLARE_INTERFACE_SIGNAL(void(TTimestamp), TransactionBarrierHandled);

    //! Raised on epoch finish for each transaction (both persistent and transient)
    //! to help all dependent subsystems to reset their transient transaction-related
    //! state.
    DECLARE_INTERFACE_SIGNAL(void(TTransaction*), TransactionTransientReset);

    //! Finds transaction by id.
    //! If it does not exist then creates a new transaction
    //! (either persistent or transient, depending on #transient).
    //! Throws if tablet cell is decommissioned or suspended.
    //! \param fresh An out-param indicating if the transaction was just-created.
    virtual TTransaction* GetOrCreateTransactionOrThrow(
        TTransactionId transactionId,
        TTimestamp startTimestamp,
        TDuration timeout,
        bool transient,
        bool* fresh = nullptr) = 0;

    //! Finds a transaction by id.
    //! If a persistent instance is found, just returns it.
    //! If a transient instance is found or no transaction is found
    //! returns |nullptr|.
    virtual TTransaction* FindPersistentTransaction(TTransactionId transcationId) = 0;

    //! Finds a transaction by id.
    //! If a persistent instance is found, just returns it.
    //! Fails if a transient instance is found or no transaction is found.
    virtual TTransaction* GetPersistentTransaction(TTransactionId transactionId) = 0;

    //! Finds a transaction by id.
    //! If a persistent instance is found, just returns it.
    //! If a transient instance is found, makes is persistent and returns it.
    //! Fails if no transaction is found.
    //! Throws if tablet cell is decommissioned or suspended.
    virtual TTransaction* MakeTransactionPersistentOrThrow(TTransactionId transactionId) = 0;

    //! Removes a given #transaction, which must be transient.
    virtual void DropTransaction(TTransaction* transaction) = 0;

    //! Returns the full list of transactions, including transient and persistent.
    virtual std::vector<TTransaction*> GetTransactions() = 0;

    //! Schedules a mutation that creates a given transaction (if missing) and
    //! registers a set of actions.
    virtual TFuture<void> RegisterTransactionActions(
        TTransactionId transactionId,
        TTimestamp transactionStartTimestamp,
        TDuration transactionTimeout,
        TTransactionSignature signature,
        ::google::protobuf::RepeatedPtrField<NTransactionClient::NProto::TTransactionActionData>&& actions) = 0;

    virtual void RegisterTransactionActionHandlers(
        const NTransactionSupervisor::TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const NTransactionSupervisor::TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const NTransactionSupervisor::TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor) = 0;

    virtual void RegisterTransactionActionHandlers(
        const NTransactionSupervisor::TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const NTransactionSupervisor::TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const NTransactionSupervisor::TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor,
        const NTransactionSupervisor::TTransactionSerializeActionHandlerDescriptor<TTransaction>& serializeActionDescriptor) = 0;

    //! Increases transaction commit signature.
    // NB: After incrementing transaction may become committed and destroyed.
    virtual void IncrementCommitSignature(TTransaction* transaction, TTransactionSignature delta) = 0;

    virtual TTimestamp GetMinPrepareTimestamp() const = 0;
    virtual TTimestamp GetMinCommitTimestamp() const = 0;

    virtual void SetDecommission(bool decommission) = 0;
    virtual bool GetDecommission() const = 0;
    virtual void SetRemoving() = 0;

    //! Returns true if transaction manager is decommissioned and threre are
    //! no alive transactions in it, so tablet cell can be safely removed.
    virtual bool IsDecommissioned() const = 0;

    // COMPAT(gritukan)
    virtual ETabletReign GetSnapshotReign() const = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionManager)

////////////////////////////////////////////////////////////////////////////////

ITransactionManagerPtr CreateTransactionManager(
    TTransactionManagerConfigPtr config,
    ITransactionManagerHostPtr host,
    NApi::TClusterTag clockClusterTag,
    NTransactionSupervisor::ITransactionLeaseTrackerPtr transactionLeaseTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
