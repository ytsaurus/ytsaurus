#include "transaction_manager.h"

#include "bootstrap.h"
#include "transaction.h"
#include "private.h"
#include "automaton.h"
#include "chaos_slot.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_detail.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_lease_tracker.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_manager_detail.h>

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

using namespace NApi;
using namespace NChaosClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NHiveServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NTransactionSupervisor;
using namespace NYTree;
using namespace NYson;

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
        TClusterTag clockClusterTag,
        IBootstrap* bootstrap)
        : TChaosAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
        , LeaseTracker_(CreateTransactionLeaseTracker(
            Bootstrap_->GetTransactionTrackerInvoker(),
            Logger))
        , ClockClusterTag_(clockClusterTag)
        , AbortTransactionIdPool_(Config_->MaxAbortedTransactionPoolSize)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        Logger = ChaosNodeLogger.WithTag("CellId: %v", slot->GetCellId());

        YT_LOG_INFO("Set transaction manager clock cluster tag (ClockClusterTag: %v)",
            ClockClusterTag_);

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

        RegisterMethod(BIND_NO_PROPAGATE(&TTransactionManager::HydraRegisterTransactionActions, Unretained(this)));

        OrchidService_ = IYPathService::FromProducer(BIND(&TTransactionManager::BuildOrchidYson, MakeWeak(this)), TDuration::Seconds(1))
            ->Via(Slot_->GetGuardedAutomatonInvoker());
    }

    IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

    void RegisterTransactionActionHandlers(
        TTransactionActionDescriptor<TTransaction> descriptor) override
    {
        TTransactionManagerBase<TTransaction>::RegisterTransactionActionHandlers(std::move(descriptor));
    }

    // ITransactionManager implementation.
    TFuture<void> GetReadyToPrepareTransactionCommit(
        const std::vector<TTransactionId>& /*prerequisiteTransactionIds*/,
        const std::vector<TCellId>& /*cellIdsToSyncWith*/) override
    {
        return VoidFuture;
    }

    void PrepareTransactionCommit(
        TTransactionId transactionId,
        const TTransactionPrepareOptions& options) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ValidateTimestampClusterTag(
            transactionId,
            options.PrepareTimestampClusterTag,
            options.PrepareTimestamp);

        YT_VERIFY(options.Persistent);

        auto* transaction = GetTransactionOrThrow(transactionId);
        auto state = transaction->GetPersistentState();
        auto signature = transaction->GetSignature();

        if (state != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        if (signature != FinalTransactionSignature) {
            THROW_ERROR_EXCEPTION("Transaction %v is incomplete: expected signature %x, actual signature %x",
                transactionId,
                FinalTransactionSignature,
                signature);
        }

        if (state == ETransactionState::Active) {
            YT_VERIFY(transaction->GetPrepareTimestamp() == NullTimestamp);
            transaction->SetPrepareTimestamp(options.PrepareTimestamp);
            transaction->SetPersistentState(ETransactionState::PersistentCommitPrepared);

            RunPrepareTransactionActions(transaction, options);

            YT_LOG_DEBUG("Transaction commit prepared (TransactionId: %v, "
                "PrepareTimestamp: %v@%v)",
                transactionId,
                options.PrepareTimestamp,
                options.PrepareTimestampClusterTag);
        }
    }

    void PrepareTransactionAbort(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionAbortOptions& options) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AbortTransactionIdPool_.Register(transactionId);

        auto* transaction = GetTransactionOrThrow(transactionId);

        if (transaction->GetTransientState() != ETransactionState::Active && !options.Force) {
            transaction->ThrowInvalidState();
        }

        if (transaction->GetTransientState() == ETransactionState::Active) {
            transaction->SetTransientState(ETransactionState::TransientAbortPrepared);

            YT_LOG_DEBUG("Transaction abort prepared (TransactionId: %v)",
                transactionId);
        }
    }

    void CommitTransaction(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionCommitOptions& options) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        ValidateTimestampClusterTag(
            transactionId,
            options.CommitTimestampClusterTag,
            transaction->GetPrepareTimestamp());

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Committed) {
            YT_LOG_DEBUG("Transaction is already committed (TransactionId: %v)",
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

        transaction->SetCommitTimestamp(options.CommitTimestamp);
        transaction->SetPersistentState(ETransactionState::Committed);

        RunCommitTransactionActions(transaction, options);

        YT_LOG_DEBUG(
            "Transaction committed (TransactionId: %v, CommitTimestamp: %v@%v)",
            transactionId,
            options.CommitTimestamp,
            options.CommitTimestampClusterTag);

        TransactionMap_.Remove(transactionId);
    }

    void AbortTransaction(
        TTransactionId transactionId,
        const NTransactionSupervisor::TTransactionAbortOptions& options) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::PersistentCommitPrepared && !options.Force) {
            transaction->ThrowInvalidState();
        }

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetPersistentState(ETransactionState::Aborted);

        RunAbortTransactionActions(transaction, options);

        YT_LOG_DEBUG("Transaction aborted (TransactionId: %v, Force: %v)",
            transactionId,
            options.Force);

        TransactionMap_.Remove(transactionId);
    }

    void PingTransaction(TTransactionId transactionId, bool pingAncestors) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LeaseTracker_->PingTransaction(transactionId, pingAncestors);
    }

    bool CommitTransaction(TCtxCommitTransactionPtr /*context*/) override
    {
        return false;
    }

    bool AbortTransaction(TCtxAbortTransactionPtr /*context*/) override
    {
        return false;
    }

    std::unique_ptr<TMutation> CreateRegisterTransactionActionsMutation(
        TCtxRegisterTransactionActionsPtr context) override
    {
        return CreateMutation(HydraManager_, std::move(context));
    }

private:
    const TTransactionManagerConfigPtr Config_;
    const ITransactionLeaseTrackerPtr LeaseTracker_;
    const TClusterTag ClockClusterTag_;

    TEntityMap<TTransaction> TransactionMap_;
    TTransactionIdPool AbortTransactionIdPool_;

    bool NeedClearCommittedTransactions_ = false;

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
                    .Item("state").Value(transaction->GetTransientState())
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

        if (transaction->GetTransientState() != ETransactionState::Active) {
            return;
        }

        const auto& transactionSupervisor = Slot_->GetTransactionSupervisor();
        transactionSupervisor->AbortTransaction(id)
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    YT_LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                        id);
                }
            }));
    }


    void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnAfterSnapshotLoaded();

        if (NeedClearCommittedTransactions_) {
            std::vector<TTransactionId> transactionIds;
            for (const auto& [transactionId, transaction] : TransactionMap_) {
                if (transaction->GetPersistentState() == ETransactionState::Committed) {
                    transactionIds.push_back(transactionId);
                }
            }
            for (auto transactionId : transactionIds) {
                TransactionMap_.Remove(transactionId);
            }
        }
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnLeaderActive();

        // Recreate leases for all active transactions.
        for (auto [transactionId, transaction] : TransactionMap_) {
            auto state = transaction->GetPersistentState();

            YT_LOG_FATAL_IF(state != transaction->GetTransientState(),
                "Found transaction in unexpected state (TransactionId: %v, PersistentState: %v, TransientState: %v, StartTimestatmp: %v)",
                transactionId,
                state,
                transaction->GetTransientState(),
                transaction->GetStartTimestamp());

            if (state == ETransactionState::Active ||
                state == ETransactionState::PersistentCommitPrepared)
            {
                CreateLease(transaction);
            }
        }

        LeaseTracker_->Start();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnStopLeading();

        LeaseTracker_->Stop();

        // Reset all transiently prepared persistent transactions back into active state.
        // Clear all lease flags.
        for (auto [transactionId, transaction] : TransactionMap_) {
            transaction->ResetTransientState();
            transaction->SetHasLease(false);
        }
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

        NeedClearCommittedTransactions_ = context.GetVersion() < EChaosReign::RemoveCommitted;

        Automaton_->RememberReign(static_cast<TReign>(context.GetVersion()));
    }


    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::Clear();

        TransactionMap_.Clear();
        NeedClearCommittedTransactions_ = false;
    }


    void HydraRegisterTransactionActions(NChaosClient::NProto::TReqRegisterTransactionActions* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto transactionStartTimestamp = request->transaction_start_timestamp();
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());
        auto signature = request->signature();

        auto* transaction = GetOrCreateTransaction(
            transactionId,
            transactionStartTimestamp,
            transactionTimeout);

        auto state = transaction->GetPersistentState();
        if (state != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        for (const auto& protoData : request->actions()) {
            auto data = FromProto<TTransactionActionData>(protoData);
            transaction->Actions().push_back(data);

            YT_LOG_DEBUG("Transaction action registered (TransactionId: %v, ActionType: %v)",
                transactionId,
                data.Type);
        }

        transaction->SetSignature(transaction->GetSignature() + signature);
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Transaction, TTransaction);

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
        transactionHolder->SetPersistentState(ETransactionState::Active);
        transactionHolder->SetUser(user);

        auto* transaction = TransactionMap_.Insert(transactionId, std::move(transactionHolder));

        if (IsLeader()) {
            CreateLease(transaction);
        }

        YT_LOG_DEBUG("Transaction started (TransactionId: %v, StartTimestamp: %v, StartTime: %v, "
            "Timeout: %v)",
            transactionId,
            startTimestamp,
            TimestampToInstant(startTimestamp).first,
            timeout);

        return transaction;
    }

    void ValidateTimestampClusterTag(TTransactionId transactionId, TClusterTag timestampClusterTag, TTimestamp prepareTimestamp)
    {
        if (prepareTimestamp == NullTimestamp) {
            return;
        }

        if (ClockClusterTag_ == InvalidCellTag || timestampClusterTag == InvalidCellTag) {
            return;
        }

        if (ClockClusterTag_ != timestampClusterTag) {
            YT_LOG_ALERT("Transaction timestamp is generated from unexpected clock (TransactionId: %v, TransactionClusterTag: %v, ClockClusterTag: %v)",
                transactionId,
                timestampClusterTag,
                ClockClusterTag_);
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TransactionMap_);

////////////////////////////////////////////////////////////////////////////////

ITransactionManagerPtr CreateTransactionManager(
    TTransactionManagerConfigPtr config,
    IChaosSlotPtr slot,
    TClusterTag clockClusterTag,
    IBootstrap* bootstrap)
{
    return New<TTransactionManager>(
        config,
        slot,
        clockClusterTag,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
