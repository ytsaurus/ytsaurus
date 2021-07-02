#include "transaction_manager.h"
#include "bootstrap.h"
#include "transaction.h"
#include "private.h"
#include "automaton.h"
#include "chaos_slot.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/hive/transaction_detail.h>
#include <yt/yt/server/lib/hive/transaction_supervisor.h>
#include <yt/yt/server/lib/hive/transaction_lease_tracker.h>
#include <yt/yt/server/lib/hive/transaction_manager_detail.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>
#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NChaosNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NHydra;
using namespace NHiveServer;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager
    : public ITransactionManager
    , public TChaosAutomatonPart
    , public TTransactionManagerBase<TTransaction>
{
public:
    TTransactionManager(
        TTransactionManagerConfigPtr config,
        IChaosSlotPtr slot,
        IBootstrap* bootstrap)
        : TChaosAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
        , LeaseTracker_(New<TTransactionLeaseTracker>(
            Bootstrap_->GetTransactionTrackerInvoker(),
            Logger))
        , AbortTransactionIdPool_(Config_->MaxAbortedTransactionPoolSize)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "TransactionManager.Keys",
            BIND(&TTransactionManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TransactionManager.Values",
            BIND(&TTransactionManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TransactionManager.Keys",
            BIND(&TTransactionManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TransactionManager.Values",
            BIND(&TTransactionManager::SaveValues, Unretained(this)));

        OrchidService_ = IYPathService::FromProducer(BIND(&TTransactionManager::BuildOrchidYson, MakeWeak(this)), TDuration::Seconds(1))
            ->Via(Slot_->GetGuardedAutomatonInvoker());
    }

    virtual IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

    virtual void RegisterTransactionActionHandlers(
        const TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor) override
    {
        TTransactionManagerBase<TTransaction>::RegisterTransactionActionHandlers(
            prepareActionDescriptor,
            commitActionDescriptor,
            abortActionDescriptor);
    }

    // ITransactionManager implementation.
    virtual TFuture<void> GetReadyToPrepareTransactionCommit(
        const std::vector<TTransactionId>& /*prerequisiteTransactionIds*/,
        const std::vector<TCellId>& /*cellIdsToSyncWith*/) override
    {
        return VoidFuture;
    }

    virtual void PrepareTransactionCommit(
        TTransactionId transactionId,
        bool persistent,
        TTimestamp prepareTimestamp,
        const std::vector<TTransactionId>& /*prerequisiteTransactionIds*/) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        auto state = transaction->GetState();
        auto signature = transaction->GetSignature();

        if (signature != FinalTransactionSignature) {
            THROW_ERROR_EXCEPTION("Transaction %v is incomplete: expected signature %x, actual signature %x",
                transactionId,
                FinalTransactionSignature,
                signature);
        }

        if (state != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        YT_VERIFY(transaction->GetPrepareTimestamp() == NullTimestamp);
        transaction->SetPrepareTimestamp(prepareTimestamp);
        transaction->SetState(ETransactionState::PersistentCommitPrepared);

        RunPrepareTransactionActions(transaction, persistent);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Transaction commit prepared (TransactionId: %v, "
            "PrepareTimestamp: %llx)",
            transactionId,
            prepareTimestamp);
    }

    virtual void PrepareTransactionAbort(TTransactionId transactionId, bool force) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AbortTransactionIdPool_.Register(transactionId);

        auto* transaction = GetTransactionOrThrow(transactionId);

        if (!transaction->IsActive() && !force) {
            transaction->ThrowInvalidState();
        }

        if (transaction->IsActive()) {
            transaction->SetState(ETransactionState::TransientAbortPrepared);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Transaction abort prepared (TransactionId: %v)",
                transactionId);
        }
    }

    virtual void CommitTransaction(TTransactionId transactionId, TTimestamp commitTimestamp) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        auto state = transaction->GetState();
        if (state == ETransactionState::Committed) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Transaction is already committed (TransactionId: %v)",
                transactionId);
            return;
        }

        if (state != ETransactionState::Active &&
            state != ETransactionState::PersistentCommitPrepared)
        {
            transaction->ThrowInvalidState();
        }

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetCommitTimestamp(commitTimestamp);
        transaction->SetState(ETransactionState::Committed);

        RunCommitTransactionActions(transaction);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Transaction committed (TransactionId: %v, CommitTimestamp: %llx)",
            transactionId,
            commitTimestamp);
    }

    virtual void AbortTransaction(TTransactionId transactionId, bool force) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        auto state = transaction->GetState();
        if (state == ETransactionState::PersistentCommitPrepared && !force) {
            transaction->ThrowInvalidState();
        }

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetState(ETransactionState::Aborted);

        RunAbortTransactionActions(transaction);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Transaction aborted (TransactionId: %v, Force: %v)",
            transactionId,
            force);

        TransactionMap_.Remove(transactionId);
    }

    virtual void PingTransaction(TTransactionId transactionId, bool pingAncestors) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LeaseTracker_->PingTransaction(transactionId, pingAncestors);
    }

private:
    const TTransactionManagerConfigPtr Config_;
    const TTransactionLeaseTrackerPtr LeaseTracker_;

    TEntityMap<TTransaction> TransactionMap_;
    TTransactionIdPool AbortTransactionIdPool_;

    IYPathServicePtr OrchidService_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        BuildYsonFluently(consumer)
            .DoMapFor(TransactionMap_, [] (TFluentMap fluent, const std::pair<TTransactionId, TTransaction*>& pair) {
                auto* transaction = pair.second;
                fluent
                    .Item(ToString(transaction->GetId())).BeginMap()
                    .Item("timeout").Value(transaction->GetTimeout())
                    .Item("state").Value(transaction->GetState())
                    .Item("start_timestamp").Value(transaction->GetStartTimestamp())
                    .Item("prepare_timestamp").Value(transaction->GetPrepareTimestamp())
                    // Omit CommitTimestamp, it's typically null.
                    .EndMap();
            });
    }

    void CreateLease(TTransaction* transaction)
    {
        if (transaction->GetHasLease()) {
            return;
        }

        auto invoker = Slot_->GetEpochAutomatonInvoker();

        LeaseTracker_->RegisterTransaction(
            transaction->GetId(),
            NullTransactionId,
            transaction->GetTimeout(),
            /*deadline*/ std::nullopt,
            BIND(&TTransactionManager::OnTransactionExpired, MakeStrong(this))
                .Via(invoker));
        transaction->SetHasLease(true);
    }

    void CloseLease(TTransaction* transaction)
    {
        if (!transaction->GetHasLease()) {
            return;
        }

        LeaseTracker_->UnregisterTransaction(transaction->GetId());
        transaction->SetHasLease(false);
    }

    void OnTransactionExpired(TTransactionId id)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(id);
        if (!transaction) {
            return;
        }

        if (transaction->GetState() != ETransactionState::Active) {
            return;
        }

        const auto& transactionSupervisor = Slot_->GetTransactionSupervisor();
        transactionSupervisor->AbortTransaction(id).Subscribe(BIND([=] (const TError& error) {
            if (!error.IsOK()) {
                YT_LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                    id);
            }
        }));
    }


    virtual void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnAfterSnapshotLoaded();
    }

    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnLeaderActive();
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnStopLeading();
    }


    void SaveKeys(TSaveContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;
        TransactionMap_.SaveValues(context);
    }


    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        TransactionMap_.LoadValues(context);
    }


    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::Clear();
    }


    TTransaction* GetTransaction(TTransactionId transactionId)
    {
        return TransactionMap_.Get(transactionId);
    }

    TTransaction* GetTransactionOrThrow(TTransactionId transactionId)
    {
        if (auto* transaction = FindTransaction(transactionId)) {
            return transaction;
        }
        THROW_ERROR_EXCEPTION(
            NTransactionClient::EErrorCode::NoSuchTransaction,
            "No such transaction %v",
            transactionId);
    }

    TTransaction* FindTransaction(TTransactionId transactionId)
    {
        if (auto* transaction = TransactionMap_.Find(transactionId)) {
            return transaction;
        }
        return nullptr;
    }

    TTransaction* GetOrCreateTransaction(
        TTransactionId transactionId,
        TTimestamp startTimestamp,
        TDuration timeout,
        const TString& user = TString(),
        bool* fresh = nullptr)
    {
        if (auto* transaction = TransactionMap_.Find(transactionId)) {
            return transaction;
        }

        if (AbortTransactionIdPool_.IsRegistered(transactionId)) {
            THROW_ERROR_EXCEPTION("Abort was requested for transaction %v",
                transactionId);
        }

        if (fresh) {
            *fresh = true;
        }

        auto transactionHolder = std::make_unique<TTransaction>(transactionId);
        transactionHolder->SetTimeout(timeout);
        transactionHolder->SetStartTimestamp(startTimestamp);
        transactionHolder->SetState(ETransactionState::Active);
        transactionHolder->SetUser(user);

        auto* transaction = TransactionMap_.Insert(transactionId, std::move(transactionHolder));

        if (IsLeader()) {
            CreateLease(transaction);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Transaction started (TransactionId: %v, StartTimestamp: %llx, StartTime: %v, "
            "Timeout: %v)",
            transactionId,
            startTimestamp,
            TimestampToInstant(startTimestamp).first,
            timeout);

        return transaction;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITransactionManagerPtr CreateTransactionManager(
    TTransactionManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap)
{
    return New<TTransactionManager>(
        config,
        slot,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
