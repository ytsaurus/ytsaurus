#include "transaction_manager.h"
#include "private.h"
#include "automaton.h"
#include "config.h"
#include "tablet_slot.h"
#include "transaction.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/hive/transaction_supervisor.h>
#include <yt/server/hive/transaction_lease_tracker.h>
#include <yt/server/hive/transaction_manager_detail.h>

#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/mutation.h>

#include <yt/server/tablet_node/transaction_manager.pb.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/client/transaction_client/timestamp_provider.h>
#include <yt/ytlib/transaction_client/action.h>

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/tablet_client/tablet_service.pb.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/heap.h>
#include <yt/core/misc/ring_queue.h>

#include <set>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NHydra;
using namespace NHiveServer;
using namespace NCellNode;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

//! Maintains a set of transaction ids of bounded capacity.
//! Expires old ids in FIFO order.
class TTransactionIdPool
{
public:
    explicit TTransactionIdPool(int maxSize)
        : MaxSize_(maxSize)
    { }

    void Register(const TTransactionId& id)
    {
        if (IdSet_.insert(id).second) {
            IdQueue_.push(id);
        }

        if (IdQueue_.size() > MaxSize_) {
            auto idToExpire = IdQueue_.front();
            IdQueue_.pop();
            YCHECK(IdSet_.erase(idToExpire) == 1);
        }
    }

    bool IsRegistered(const TTransactionId& id) const
    {
        return IdSet_.find(id) != IdSet_.end();
    }

private:
    const int MaxSize_;

    THashSet<TTransactionId> IdSet_;
    TRingQueue<TTransactionId> IdQueue_;

};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TTabletAutomatonPart
    , public TTransactionManagerBase<TTransaction>
{
public:
    DEFINE_SIGNAL(void(TTransaction*), TransactionStarted);
    DEFINE_SIGNAL(void(TTransaction*, bool), TransactionPrepared);
    DEFINE_SIGNAL(void(TTransaction*), TransactionCommitted);
    DEFINE_SIGNAL(void(TTransaction*), TransactionSerialized);
    DEFINE_SIGNAL(void(TTransaction*), TransactionAborted);
    DEFINE_SIGNAL(void(TTransaction*), TransactionTransientReset);

public:
    TImpl(
        TTransactionManagerConfigPtr config,
        TTabletSlotPtr slot,
        NCellNode::TBootstrap* bootstrap)
        : TCompositeAutomatonPart(
            slot->GetHydraManager(),
            slot->GetAutomaton(),
            slot->GetAutomatonInvoker())
        , TTabletAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
        , LeaseTracker_(New<TTransactionLeaseTracker>(
            Bootstrap_->GetTransactionTrackerInvoker(),
            Logger))
        , NativeCellTag_(Bootstrap_->GetMasterClient()->GetConnection()->GetCellTag())
        , AbortTransactionIdPool_(Config_->MaxAbortedTransactionPoolSize)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "TransactionManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TransactionManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));
        RegisterLoader(
            "TransactionManager.Async",
            BIND(&TImpl::LoadAsync, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TransactionManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TransactionManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
        RegisterSaver(
            EAsyncSerializationPriority::Default,
            "TransactionManager.Async",
            BIND(&TImpl::SaveAsync, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraRegisterTransactionActions, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraHandleTransactionBarrier, Unretained(this)));

        OrchidService_ = IYPathService::FromProducer(BIND(&TImpl::BuildOrchidYson, MakeWeak(this)), TDuration::Seconds(1))
            ->Via(Slot_->GetGuardedAutomatonInvoker());
    }

    std::unique_ptr<TMutation> CreateRegisterTransactionActionsMutation(
        TCtxRegisterTransactionActionsPtr context)
    {
        return CreateMutation(HydraManager_, std::move(context));
    }

    TTransaction* FindPersistentTransaction(const TTransactionId& transactionId)
    {
        return PersistentTransactionMap_.Find(transactionId);
    }

    TTransaction* GetPersistentTransaction(const TTransactionId& transactionId)
    {
        return PersistentTransactionMap_.Get(transactionId);
    }

    TTransaction* GetPersistentTransactionOrThrow(const TTransactionId& transactionId)
    {
        if (auto* transaction = PersistentTransactionMap_.Find(transactionId)) {
            return transaction;
        }
        THROW_ERROR_EXCEPTION(
            NTransactionClient::EErrorCode::NoSuchTransaction,
            "No such transaction %v",
            transactionId);
    }

    TTransaction* FindTransaction(const TTransactionId& transactionId)
    {
        if (auto* transaction = TransientTransactionMap_.Find(transactionId)) {
            return transaction;
        }
        if (auto* transaction = PersistentTransactionMap_.Find(transactionId)) {
            return transaction;
        }
        return nullptr;
    }

    TTransaction* GetTransactionOrThrow(const TTransactionId& transactionId)
    {
        auto* transaction = FindTransaction(transactionId);
        if (!transaction) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::NoSuchTransaction,
                "No such transaction %v",
                transactionId);
        }
        return transaction;
    }

    TTransaction* GetOrCreateTransaction(
        const TTransactionId& transactionId,
        TTimestamp startTimestamp,
        TDuration timeout,
        bool transient,
        const TString& user = TString(),
        bool* fresh = nullptr)
    {
        if (fresh) {
            *fresh = false;
        }

        if (auto* transaction = TransientTransactionMap_.Find(transactionId)) {
            return transaction;
        }
        if (auto* transaction = PersistentTransactionMap_.Find(transactionId)) {
            return transaction;
        }

        if (transient && AbortTransactionIdPool_.IsRegistered(transactionId)) {
            THROW_ERROR_EXCEPTION("Abort was requested for transaction %v",
                transactionId);
        }

        if (fresh) {
            *fresh = true;
        }

        auto transactionHolder = std::make_unique<TTransaction>(transactionId);
        transactionHolder->SetForeign(CellTagFromId(transactionId) != NativeCellTag_);
        transactionHolder->SetTimeout(timeout);
        transactionHolder->SetStartTimestamp(startTimestamp);
        transactionHolder->SetState(ETransactionState::Active);
        transactionHolder->SetTransient(transient);
        transactionHolder->SetUser(user);

        auto& map = transient ? TransientTransactionMap_ : PersistentTransactionMap_;
        auto* transaction = map.Insert(transactionId, std::move(transactionHolder));

        if (IsLeader()) {
            CreateLease(transaction);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction started (TransactionId: %v, StartTimestamp: %llx, StartTime: %v, "
            "Timeout: %v, Transient: %v)",
            transactionId,
            startTimestamp,
            TimestampToInstant(startTimestamp).first,
            timeout,
            transient);

        return transaction;
    }

    TTransaction* MakeTransactionPersistent(const TTransactionId& transactionId)
    {
        if (auto* transaction = TransientTransactionMap_.Find(transactionId)) {
            transaction->SetTransient(false);
            if (IsLeader()) {
                CreateLease(transaction);
            }
            auto transactionHolder = TransientTransactionMap_.Release(transactionId);
            PersistentTransactionMap_.Insert(transactionId, std::move(transactionHolder));
            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction became persistent (TransactionId: %v)",
                transactionId);
            return transaction;
        }

        if (auto* transaction = PersistentTransactionMap_.Find(transactionId)) {
            YCHECK(!transaction->GetTransient());
            return transaction;
        }

        Y_UNREACHABLE();
    }

    void DropTransaction(TTransaction* transaction)
    {
        YCHECK(transaction->GetTransient());

        if (IsLeader()) {
            CloseLease(transaction);
        }

        auto transactionId = transaction->GetId();
        TransientTransactionMap_.Remove(transactionId);

        LOG_DEBUG("Transaction dropped (TransactionId: %v)",
            transactionId);
    }

    std::vector<TTransaction*> GetTransactions()
    {
        std::vector<TTransaction*> transactions;
        for (const auto& pair : TransientTransactionMap_) {
            transactions.push_back(pair.second);
        }
        for (const auto& pair : PersistentTransactionMap_) {
            transactions.push_back(pair.second);
        }
        return transactions;
    }

    IYPathServicePtr GetOrchidService()
    {
        return OrchidService_;
    }


    // ITransactionManager implementation.
    void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        bool persistent,
        TTimestamp prepareTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTransaction* transaction;
        ETransactionState state;
        TTransactionSignature signature;
        if (persistent) {
            transaction = GetPersistentTransactionOrThrow(transactionId);
            state = transaction->GetPersistentState();
            signature = transaction->GetPersistentSignature();
        } else {
            transaction = GetTransactionOrThrow(transactionId);
            state = transaction->GetState();
            signature = transaction->GetTransientSignature();
        }

        // Allow preparing transactions in Active and TransientCommitPrepared (for persistent mode) states.
        if (state != ETransactionState::Active &&
            !(persistent && state == ETransactionState::TransientCommitPrepared))
        {
            transaction->ThrowInvalidState();
        }

        if (signature != FinalTransactionSignature) {
            THROW_ERROR_EXCEPTION("Transaction %v is incomplete: expected signature %x, actual signature %x",
                transactionId,
                FinalTransactionSignature,
                signature);
        }

        if (state == ETransactionState::Active) {
            YCHECK(transaction->GetPrepareTimestamp() == NullTimestamp);
            transaction->SetPrepareTimestamp(prepareTimestamp);
            RegisterPrepareTimestamp(transaction);
            transaction->SetState(persistent
                ? ETransactionState::PersistentCommitPrepared
                : ETransactionState::TransientCommitPrepared);

            TransactionPrepared_.Fire(transaction, persistent);
            RunPrepareTransactionActions(transaction, persistent);

            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit prepared (TransactionId: %v, Persistent: %v, "
                "PrepareTimestamp: %llx)",
                transactionId,
                persistent,
                prepareTimestamp);
        }
    }

    void PrepareTransactionAbort(const TTransactionId& transactionId, bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AbortTransactionIdPool_.Register(transactionId);

        auto* transaction = GetTransactionOrThrow(transactionId);

        if (!transaction->IsActive() && !force) {
            transaction->ThrowInvalidState();
        }

        if (transaction->IsActive()) {
            transaction->SetState(ETransactionState::TransientAbortPrepared);

            LOG_DEBUG("Transaction abort prepared (TransactionId: %v)",
                transactionId);
        }
    }

    void CommitTransaction(const TTransactionId& transactionId, TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetPersistentTransactionOrThrow(transactionId);

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Committed) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction is already committed (TransactionId: %v)",
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

        TransactionCommitted_.Fire(transaction);
        RunCommitTransactionActions(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %v, CommitTimestamp: %llx)",
            transactionId,
            commitTimestamp);

        FinishTransaction(transaction);

        if (transaction->IsSerializationNeeded()) {
            auto& heap = SerializingTransactionHeaps_[transaction->GetCellTag()];
            heap.push_back(transaction);
            AdjustHeapBack(heap.begin(), heap.end(), SerializingTransactionHeapComparer);
            UpdateMinCommitTimestamp(heap);
        } else {
            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction removed without serialization (TransactionId: %v)",
                transactionId);
            PersistentTransactionMap_.Remove(transactionId);
        }
    }

    void AbortTransaction(const TTransactionId& transactionId, bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetPersistentTransactionOrThrow(transactionId);

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::PersistentCommitPrepared && !force) {
            transaction->ThrowInvalidState();
        }

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetState(ETransactionState::Aborted);

        TransactionAborted_.Fire(transaction);
        RunAbortTransactionActions(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %v, Force: %v)",
            transactionId,
            force);

        FinishTransaction(transaction);
        PersistentTransactionMap_.Remove(transactionId);
    }

    void PingTransaction(const TTransactionId& transactionId, bool pingAncestors)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LeaseTracker_->PingTransaction(transactionId, pingAncestors);
    }

    TTimestamp GetMinPrepareTimestamp() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return PreparedTransactions_.empty()
            ? GetLatestTimestamp()
            : PreparedTransactions_.begin()->first;
    }

    TTimestamp GetMinCommitTimestamp() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return MinCommitTimestamp_.Get(GetLatestTimestamp());
    }

private:
    const TTransactionManagerConfigPtr Config_;
    const TTransactionLeaseTrackerPtr LeaseTracker_;
    const TCellTag NativeCellTag_;

    const NProfiling::TProfiler& Profiler = TabletNodeProfiler;
    NProfiling::TAggregateGauge TransactionSerializationLagCounter_ = {"/transaction_serialization_lag"};

    TEntityMap<TTransaction> PersistentTransactionMap_;
    TEntityMap<TTransaction> TransientTransactionMap_;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;
    NConcurrency::TPeriodicExecutorPtr BarrierCheckExecutor_;

    THashMap<TCellTag, std::vector<TTransaction*>> SerializingTransactionHeaps_;
    THashMap<TCellTag, TTimestamp> LastSerializedCommitTimestamps_;
    TTimestamp TransientBarrierTimestamp_ = MinTimestamp;
    TNullable<TTimestamp> MinCommitTimestamp_;

    IYPathServicePtr OrchidService_;

    std::set<std::pair<TTimestamp, TTransaction*>> PreparedTransactions_;

    TTransactionIdPool AbortTransactionIdPool_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto dumpTransaction = [&] (TFluentMap fluent, const std::pair<TTransactionId, TTransaction*>& pair) {
            auto* transaction = pair.second;
            fluent
                .Item(ToString(transaction->GetId())).BeginMap()
                    .Item("transient").Value(transaction->GetTransient())
                    .Item("timeout").Value(transaction->GetTimeout())
                    .Item("state").Value(transaction->GetState())
                    .Item("start_timestamp").Value(transaction->GetStartTimestamp())
                    .Item("prepare_timestamp").Value(transaction->GetPrepareTimestamp())
                    // Omit CommitTimestamp, it's typically null.
                    .Item("locked_row_count").Value(transaction->LockedRows().size())
                    .Item("prelocked_row_count").Value(transaction->PrelockedRows().size())
                    .Item("immediate_locked_write_log_size").Value(transaction->ImmediateLockedWriteLog().Size())
                    .Item("immediate_lockless_write_log_size").Value(transaction->ImmediateLocklessWriteLog().Size())
                    .Item("delayed_write_log_size").Value(transaction->DelayedLocklessWriteLog().Size())
                .EndMap();
        };
        BuildYsonFluently(consumer)
            .BeginMap()
                .DoFor(TransientTransactionMap_, dumpTransaction)
                .DoFor(PersistentTransactionMap_, dumpTransaction)
            .EndMap();
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
            BIND(&TImpl::OnTransactionExpired, MakeStrong(this))
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


    void OnTransactionExpired(const TTransactionId& id)
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
                LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                    id);
            }
        }));
    }

    void OnTransactionTimedOut(const TTransactionId& id)
    {
        auto* transaction = FindTransaction(id);
        if (!transaction) {
            return;
        }

        if (transaction->GetState() != ETransactionState::Active) {
            return;
        }

        LOG_DEBUG("Transaction timed out (TransactionId: %v)",
            id);

        const auto& transactionSupervisor = Slot_->GetTransactionSupervisor();
        transactionSupervisor->AbortTransaction(id).Subscribe(BIND([=] (const TError& error) {
            if (!error.IsOK()) {
                LOG_DEBUG(error, "Error aborting timed out transaction (TransactionId: %v)",
                    id);
            }
        }));
    }

    void FinishTransaction(TTransaction* transaction)
    {
        UnregisterPrepareTimestamp(transaction);
    }


    virtual void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnAfterSnapshotLoaded();

        SerializingTransactionHeaps_.clear();
        for (const auto& pair : PersistentTransactionMap_) {
            auto* transaction = pair.second;
            if (transaction->GetState() == ETransactionState::Committed && transaction->IsSerializationNeeded()) {
                SerializingTransactionHeaps_[transaction->GetCellTag()].push_back(transaction);
            }
            if (transaction->IsPrepared()) {
                RegisterPrepareTimestamp(transaction);
            }
        }
        for (auto& pair : SerializingTransactionHeaps_) {
            auto& heap = pair.second;
            MakeHeap(heap.begin(), heap.end(), SerializingTransactionHeapComparer);
            UpdateMinCommitTimestamp(heap);
        }
    }

    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnLeaderActive();

        YCHECK(TransientTransactionMap_.GetSize() == 0);

        // Recreate leases for all active transactions.
        for (const auto& pair : PersistentTransactionMap_) {
            auto* transaction = pair.second;
            if (transaction->GetState() == ETransactionState::Active ||
                transaction->GetState() == ETransactionState::PersistentCommitPrepared)
            {
                CreateLease(transaction);
            }
        }

        TransientBarrierTimestamp_ = MinTimestamp;

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Slot_->GetEpochAutomatonInvoker(),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();

        BarrierCheckExecutor_ = New<TPeriodicExecutor>(
            Slot_->GetEpochAutomatonInvoker(),
            BIND(&TImpl::OnPeriodicBarrierCheck, MakeWeak(this)),
            Config_->BarrierCheckPeriod);
        BarrierCheckExecutor_->Start();

        LeaseTracker_->Start();
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopLeading();

        if (ProfilingExecutor_) {
            ProfilingExecutor_->Stop();
            ProfilingExecutor_.Reset();
        }

        if (BarrierCheckExecutor_) {
            BarrierCheckExecutor_->Stop();
            BarrierCheckExecutor_.Reset();
        }

        // Drop all transient transactions.
        for (const auto& pair : TransientTransactionMap_) {
            auto* transaction = pair.second;
            transaction->ResetFinished();
            TransactionTransientReset_.Fire(transaction);
            UnregisterPrepareTimestamp(transaction);
        }
        TransientTransactionMap_.Clear();

        // Reset all transiently prepared persistent transactions back into active state.
        // Mark all transactions as finished to release pending readers.
        for (const auto& pair : PersistentTransactionMap_) {
            auto* transaction = pair.second;
            if (transaction->GetState() == ETransactionState::TransientCommitPrepared) {
                UnregisterPrepareTimestamp(transaction);
                transaction->SetPrepareTimestamp(NullTimestamp);
            }
            transaction->SetState(transaction->GetPersistentState());
            transaction->SetTransientSignature(transaction->GetPersistentSignature());
            transaction->ResetFinished();
            TransactionTransientReset_.Fire(transaction);
            CloseLease(transaction);
        }

        LeaseTracker_->Stop();
    }


    void SaveKeys(TSaveContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PersistentTransactionMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;
        PersistentTransactionMap_.SaveValues(context);
        // TODO(savrus) Save whole map in 19.4.
        Save(context, LastSerializedCommitTimestamps_[NativeCellTag_]);
    }

    TCallback<void(TSaveContext&)> SaveAsync()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        std::vector<std::pair<TTransactionId, TCallback<void(TSaveContext&)>>> capturedTransactions;
        for (const auto& pair : PersistentTransactionMap_) {
            auto* transaction = pair.second;
            capturedTransactions.push_back(std::make_pair(transaction->GetId(), transaction->AsyncSave()));
        }

        return BIND([capturedTransactions = std::move(capturedTransactions)] (TSaveContext& context) {
                using NYT::Save;
                // NB: This is not stable.
                for (const auto& pair : capturedTransactions) {
                    Save(context, pair.first);
                    pair.second.Run(context);
                }
            });
    }


    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PersistentTransactionMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        PersistentTransactionMap_.LoadValues(context);
        Load(context, LastSerializedCommitTimestamps_[NativeCellTag_]);
    }

    void LoadAsync(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        SERIALIZATION_DUMP_WRITE(context, "transactions[%v]", PersistentTransactionMap_.size());
        SERIALIZATION_DUMP_INDENT(context) {
            for (int index = 0; index < PersistentTransactionMap_.size(); ++index) {
                auto transactionId = Load<TTransactionId>(context);
                SERIALIZATION_DUMP_WRITE(context, "%v =>", transactionId);
                SERIALIZATION_DUMP_INDENT(context) {
                    auto* transaction = GetPersistentTransaction(transactionId);
                    transaction->AsyncLoad(context);
                }
            }
        }
    }


    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::Clear();

        TransientTransactionMap_.Clear();
        PersistentTransactionMap_.Clear();
        SerializingTransactionHeaps_.clear();
        PreparedTransactions_.clear();
        LastSerializedCommitTimestamps_.clear();
        MinCommitTimestamp_.Reset();
    }


    void HydraRegisterTransactionActions(NTabletClient::NProto::TReqRegisterTransactionActions* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto transactionStartTimestamp = request->transaction_start_timestamp();
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());
        auto signature = request->signature();

        auto* transaction = GetOrCreateTransaction(
            transactionId,
            transactionStartTimestamp,
            transactionTimeout,
            false);

        auto state = transaction->GetPersistentState();
        if (state != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        for (const auto& protoData : request->actions()) {
            auto data = FromProto<TTransactionActionData>(protoData);
            transaction->Actions().push_back(data);

            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction action registered (TransactionId: %v, ActionType: %v)",
                transactionId,
                data.Type);
        }

        transaction->SetPersistentSignature(transaction->GetPersistentSignature() + signature);
    }

    void HydraHandleTransactionBarrier(NTabletNode::NProto::TReqHandleTransactionBarrier* request)
    {
        auto barrierTimestamp = request->timestamp();

        LOG_DEBUG_UNLESS(IsRecovery(), "Handling transaction barrier (Timestamp: %llx)",
            barrierTimestamp);

        for (auto& pair : SerializingTransactionHeaps_) {
            auto& heap = pair.second;

            while (!heap.empty()) {
                auto* transaction = heap.front();
                auto commitTimestamp = transaction->GetCommitTimestamp();
                if (commitTimestamp > barrierTimestamp) {
                    break;
                }

                UpdateLastSerializedCommitTimestamp(transaction);

                const auto& transactionId = transaction->GetId();
                LOG_DEBUG_UNLESS(IsRecovery(), "Transaction serialized (TransactionId: %v, CommitTimestamp: %llx)",
                    transaction->GetId(),
                    commitTimestamp);

                transaction->SetState(ETransactionState::Serialized);
                TransactionSerialized_.Fire(transaction);

                PersistentTransactionMap_.Remove(transactionId);

                ExtractHeap(heap.begin(), heap.end(), SerializingTransactionHeapComparer);
                heap.pop_back();
            }
        }

        MinCommitTimestamp_.Reset();
        for (const auto& heap : SerializingTransactionHeaps_) {
            UpdateMinCommitTimestamp(heap.second);
        }

        // YT-8542: It is important to update this timestamp only _after_ all relevant transactions are serialized.
        // See TTableReplicator.
        Slot_->GetRuntimeData()->LastBarrierTimestamp.store(barrierTimestamp);
    }


    TTimestamp GetLatestTimestamp() const
    {
        return Bootstrap_->GetLatestTimestamp();
    }

    TDuration ComputeTransactionSerializationLag() const
    {
        if (PreparedTransactions_.empty()) {
            return TDuration::Zero();
        }

        auto latestTimestamp = GetLatestTimestamp();
        auto minPrepareTimestamp = PreparedTransactions_.begin()->first;
        if (minPrepareTimestamp > latestTimestamp) {
            return TDuration::Zero();
        }

        return TimestampDiffToDuration(minPrepareTimestamp, latestTimestamp).second;
    }


    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        Profiler.Update(TransactionSerializationLagCounter_, NProfiling::DurationToValue(ComputeTransactionSerializationLag()));
    }


    void OnPeriodicBarrierCheck()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        LOG_DEBUG("Running periodic barrier check (BarrierTimestamp: %llx, MinPrepareTimestamp: %llx)",
            TransientBarrierTimestamp_,
            GetMinPrepareTimestamp());

        CheckBarrier();
    }

    void CheckBarrier()
    {
        if (!IsLeader()) {
            return;
        }

        auto minPrepareTimestamp = GetMinPrepareTimestamp();
        if (minPrepareTimestamp <= TransientBarrierTimestamp_) {
            return;
        }

        LOG_DEBUG("Committing transaction barrier (Timestamp: %llx -> %llx)",
            TransientBarrierTimestamp_,
            minPrepareTimestamp);

        TransientBarrierTimestamp_ = minPrepareTimestamp;

        NTabletNode::NProto::TReqHandleTransactionBarrier request;
        request.set_timestamp(TransientBarrierTimestamp_);
        CreateMutation(HydraManager_, request)
            ->CommitAndLog(Logger);
    }

    void RegisterPrepareTimestamp(TTransaction* transaction)
    {
        if (transaction->GetForeign()) {
            return;
        }
        auto prepareTimestamp = transaction->GetPrepareTimestamp();
        if (prepareTimestamp == NullTimestamp) {
            return;
        }
        YCHECK(PreparedTransactions_.emplace(prepareTimestamp, transaction).second);
    }

    void UnregisterPrepareTimestamp(TTransaction* transaction)
    {
        if (transaction->GetForeign()) {
            return;
        }
        auto prepareTimestamp = transaction->GetPrepareTimestamp();
        if (prepareTimestamp == NullTimestamp) {
            return;
        }
        auto pair = std::make_pair(prepareTimestamp, transaction);
        auto it = PreparedTransactions_.find(pair);
        YCHECK(it != PreparedTransactions_.end());
        PreparedTransactions_.erase(it);
        CheckBarrier();
    }

    void UpdateLastSerializedCommitTimestamp(TTransaction* transaction)
    {
        auto commitTimestamp = transaction->GetCommitTimestamp();
        auto cellTag = transaction->GetCellTag();

        if (auto lastTimestampIt = LastSerializedCommitTimestamps_.find(cellTag)) {
            YCHECK(commitTimestamp > lastTimestampIt->second);
            lastTimestampIt->second = commitTimestamp;
        } else {
            YCHECK(LastSerializedCommitTimestamps_.insert(std::make_pair(cellTag, commitTimestamp)).second);
        }
    }

    void UpdateMinCommitTimestamp(const std::vector<TTransaction*>& heap)
    {
        if (heap.empty()) {
            return;
        }

        auto timestamp = heap.front()->GetCommitTimestamp();
        MinCommitTimestamp_ = std::min(timestamp, MinCommitTimestamp_.Get(timestamp));
    }

    static bool SerializingTransactionHeapComparer(
        const TTransaction* lhs,
        const TTransaction* rhs)
    {
        Y_ASSERT(lhs->IsCommitted());
        Y_ASSERT(rhs->IsCommitted());
        return lhs->GetCommitTimestamp() < rhs->GetCommitTimestamp();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerConfigPtr config,
    TTabletSlotPtr slot,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        slot,
        bootstrap))
{ }

TTransactionManager::~TTransactionManager() = default;

std::unique_ptr<TMutation> TTransactionManager::CreateRegisterTransactionActionsMutation(
    TCtxRegisterTransactionActionsPtr context)
{
    return Impl_->CreateRegisterTransactionActionsMutation(std::move(context));
}

IYPathServicePtr TTransactionManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

TTransaction* TTransactionManager::GetOrCreateTransaction(
    const TTransactionId& transactionId,
    TTimestamp startTimestamp,
    TDuration timeout,
    bool transient,
    const TString& user,
    bool* fresh)
{
    return Impl_->GetOrCreateTransaction(
        transactionId,
        startTimestamp,
        timeout,
        transient,
        user,
        fresh);
}

TTransaction* TTransactionManager::MakeTransactionPersistent(const TTransactionId& transactionId)
{
    return Impl_->MakeTransactionPersistent(transactionId);
}

void TTransactionManager::DropTransaction(TTransaction* transaction)
{
    Impl_->DropTransaction(transaction);
}

std::vector<TTransaction*> TTransactionManager::GetTransactions()
{
    return Impl_->GetTransactions();
}

void TTransactionManager::RegisterTransactionActionHandlers(
    const TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
    const TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
    const TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor)
{
    Impl_->RegisterTransactionActionHandlers(
        prepareActionDescriptor,
        commitActionDescriptor,
        abortActionDescriptor);
}

void TTransactionManager::PrepareTransactionCommit(
    const TTransactionId& transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    Impl_->PrepareTransactionCommit(transactionId, persistent, prepareTimestamp);
}

void TTransactionManager::PrepareTransactionAbort(const TTransactionId& transactionId, bool force)
{
    Impl_->PrepareTransactionAbort(transactionId, force);
}

void TTransactionManager::CommitTransaction(const TTransactionId& transactionId, TTimestamp commitTimestamp)
{
    Impl_->CommitTransaction(transactionId, commitTimestamp);
}

void TTransactionManager::AbortTransaction(const TTransactionId& transactionId, bool force)
{
    Impl_->AbortTransaction(transactionId, force);
}

void TTransactionManager::PingTransaction(const TTransactionId& transactionId, bool pingAncestors)
{
    Impl_->PingTransaction(transactionId, pingAncestors);
}

TTimestamp TTransactionManager::GetMinPrepareTimestamp()
{
    return Impl_->GetMinPrepareTimestamp();
}

TTimestamp TTransactionManager::GetMinCommitTimestamp()
{
    return Impl_->GetMinCommitTimestamp();
}

DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionStarted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*, bool), TransactionPrepared, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionCommitted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionSerialized, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionAborted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionTransientReset, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
