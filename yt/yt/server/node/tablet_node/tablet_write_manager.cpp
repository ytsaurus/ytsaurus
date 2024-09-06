#include "tablet_write_manager.h"

#include "private.h"
#include "backup_manager.h"
#include "serialize.h"
#include "sorted_dynamic_store.h"
#include "sorted_store_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "transaction.h"
#include "transaction_manager.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

namespace NYT::NTabletNode {

using namespace NChaosClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTabletWriteManager
    : public ITabletWriteManager
{
public:
    TTabletWriteManager(
        TTablet* tablet,
        ITabletContext* tabletContext)
        : Tablet_(tablet)
        , Context_(tabletContext)
        , Host_(Context_->GetTabletWriteManagerHost().Get())
        , Logger(TabletNodeLogger().WithTag("TabletId: %v", Tablet_->GetId()))
    {
        // May be null in unittests.
        if (const auto& memoryUsageTracker = Context_->GetNodeMemoryUsageTracker()) {
            WriteLogsMemoryTrackerGuard_ = TMemoryUsageTrackerGuard::Acquire(
                memoryUsageTracker->WithCategory(EMemoryCategory::TabletDynamic),
                0 /*size*/,
                MemoryUsageGranularity);
        }
    }

    TWriteContext TransientWriteRows(
        TTransaction* transaction,
        IWireProtocolReader* reader,
        EAtomicity atomicity,
        bool versioned,
        int rowCount,
        i64 dataWeight) override
    {
        auto context = atomicity == EAtomicity::None
            ? TWriteContext{}
            : CreateWriteContext(transaction);
        context.Phase = EWritePhase::Prelock;

        auto lockless =
            atomicity == EAtomicity::None ||
            Tablet_->IsPhysicallyOrdered() ||
            Tablet_->IsPhysicallyLog() ||
            versioned;
        context.Lockless = lockless;

        if (lockless) {
            // Skip the whole message.
            reader->SetCurrent(reader->GetEnd());
            context.RowCount = rowCount;
            context.DataWeight = dataWeight;
        } else {
            const auto& storeManager = Tablet_->GetStoreManager();
            storeManager->ExecuteWrites(reader, &context);
        }

        return context;
    }

    void AtomicLeaderWriteRows(
        TTransaction* transaction,
        TTransactionGeneration generation,
        const TTransactionWriteRecord& writeRecord,
        bool lockless) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        // Note that the scope below affects only the transient state.
        // As a consequence, if the transient generation was promoted ahead of us, we should not do
        // anything here.
        auto writeContext = CreateWriteContext(transaction);
        if (transaction->GetTransientGeneration() == generation && !lockless) {
            auto transientWriteState = GetOrCreateTransactionTransientWriteState(transaction->GetId());
            auto& prelockedRows = transientWriteState->PrelockedRows;

            for (int index = 0; index < writeRecord.RowCount; ++index) {
                YT_ASSERT(!prelockedRows.empty());
                auto rowRef = prelockedRows.front();
                prelockedRows.pop();
                if (Host_->ValidateAndDiscardRowRef(rowRef)) {
                    rowRef.StoreManager->ConfirmRow(&writeContext, rowRef);
                }
            }

            if (writeContext.HasSharedWriteLocks) {
                transaction->SetHasSharedWriteLocks(true);
            }

            YT_LOG_DEBUG(
                "Prelocked rows confirmed (TransactionId: %v, RowCount: %v)",
                transaction->GetId(),
                writeRecord.RowCount);
        }

        EnqueueTransactionWriteRecord(transaction, writeRecord, lockless);
    }

    void AtomicFollowerWriteRows(
        TTransaction* transaction,
        const TTransactionWriteRecord& writeRecord,
        bool lockless) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!lockless) {
            LockRows(transaction, writeRecord);
        }

        EnqueueTransactionWriteRecord(transaction, writeRecord, lockless);
    }

    void NonAtomicWriteRows(
        TTransactionId transactionId,
        const TTransactionWriteRecord& writeRecord,
        bool isLeader) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto reader = CreateWireProtocolReader(writeRecord.Data);
        TWriteContext context{
            .Phase = EWritePhase::Commit,
            .CommitTimestamp = TimestampFromTransactionId(transactionId),
            .HunkChunksInfo = writeRecord.HunkChunksInfo
        };
        const auto& storeManager = Tablet_->GetStoreManager();
        YT_VERIFY(storeManager->ExecuteWrites(reader.get(), &context));
        YT_VERIFY(writeRecord.RowCount == context.RowCount);

        if (isLeader) {
            auto counters = Tablet_->GetTableProfiler()->GetCommitCounters(GetCurrentProfilingUser());
            counters->RowCount.Increment(writeRecord.RowCount);
            counters->DataWeight.Increment(writeRecord.DataWeight);
        }

        FinishCommit(/*transaction*/ nullptr, transactionId, context.CommitTimestamp);

        YT_LOG_DEBUG(
            "Non-atomic rows committed (TransactionId: %v, "
            "RowCount: %v, WriteRecordSize: %v, ActualTimestamp: %v)",
            transactionId,
            writeRecord.RowCount,
            writeRecord.Data.Size(),
            context.CommitTimestamp);
    }

    void WriteDelayedRows(
        TTransaction* transaction,
        const TTransactionWriteRecord& writeRecord,
        bool lockless) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());
        YT_VERIFY(lockless);

        EnqueueTransactionWriteRecord(
            transaction,
            writeRecord,
            lockless);
    }

    void OnTransactionPrepared(TTransaction* transaction, bool persistent) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext() == persistent);

        // Fast path.
        if (!HasWriteState(transaction->GetId())) {
            return;
        }

        PrepareLockedRows(transaction);
        PrepareLocklessRows(transaction, persistent);

        if (!persistent) {
            return;
        }

        auto persistentWriteState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        YT_VERIFY(!std::exchange(persistentWriteState->RowsPrepared, true));

        // NB: This only makes sense for persistently prepared transactions since only these participate in 2PC
        // and may cause issues with committed rows not being visible.
        InsertPreparedTransactionToBarrier(transaction, persistentWriteState);

        if (IsReplicatorWrite(transaction) &&
            Tablet_->GetBackupCheckpointTimestamp() &&
            !persistentWriteState->LocklessWriteLog.Empty())
        {
            auto checkpointTimestamp = Tablet_->GetBackupCheckpointTimestamp();
            auto backupStage = Tablet_->GetBackupStage();
            if (transaction->GetStartTimestamp() <= checkpointTimestamp &&
                (backupStage == EBackupStage::AwaitingReplicationFinish ||
                    backupStage == EBackupStage::RespondedToMasterSuccess))
            {
                // It is obviously possible to receive a transaction with start_ts < checkpoint_ts even
                // after tablet has passed backup checkpoint. What is less obvious is that max_allowed_commit_timestamp
                // set by replicator cannot save us from such transaction being committed as it may have
                // commit_ts < checkpoint_ts: replication transactions and barrier timestamp use different
                // clocks, so needed happened-before relation cannot be established. We must reject such
                // transaction in any case.
                //
                // Hopefully, per-tablet barrier timestamps will allow for a cleaner code.
                THROW_ERROR_EXCEPTION("Cannot replicate rows into tablet %v since it has already passed "
                    "backup checkpoint and transaction start timestamp is less than checkpoint timestamp",
                    Tablet_->GetId())
                    << TErrorAttribute("start_timestamp", transaction->GetStartTimestamp())
                    << TErrorAttribute("checkpoint_timestamp", Tablet_->GetBackupCheckpointTimestamp());
            }
        }
    }

    void OnTransactionCommitted(TTransaction* transaction) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        // Fast path.
        if (!HasWriteState(transaction->GetId())) {
            return;
        }

        auto commitTimestamp = transaction->GetCommitTimestamp();

        auto persistentWriteState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        auto transientWriteState = GetOrCreateTransactionTransientWriteState(transaction->GetId());

        YT_VERIFY(transientWriteState->PrelockedRows.empty());

        auto updateProfileCounters = [&] (const TTransactionWriteLog& log) {
            for (const auto& record : log) {
                auto counters = Tablet_->GetTableProfiler()->GetCommitCounters(GetCurrentProfilingUser());
                counters->RowCount.Increment(record.RowCount);
                counters->DataWeight.Increment(record.DataWeight);
            }
        };
        updateProfileCounters(persistentWriteState->LocklessWriteLog);
        updateProfileCounters(persistentWriteState->LockedWriteLog);

        // COMPAT(ponasenko-rs)
        if (auto reign = static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign);
            reign < ETabletReign::SharedWriteLocks || !NeedsSortedSharedWriteSerialization(transaction))
        {
            CommitLockedRows(transaction);
        }

        if (NeedsLocklessSerialization(transaction)) {
            TCompactVector<TTableReplicaInfo*, 16> syncReplicas;
            for (const auto& writeRecord : persistentWriteState->LocklessWriteLog) {
                Tablet_->UpdateLastWriteTimestamp(commitTimestamp);

                for (auto replicaId : writeRecord.SyncReplicaIds) {
                    if (auto* replicaInfo = Tablet_->FindReplicaInfo(replicaId)) {
                        syncReplicas.push_back(replicaInfo);
                    }
                }
            }

            SortUnique(syncReplicas);
            for (auto* replicaInfo : syncReplicas) {
                const auto* tablet = replicaInfo->GetTablet();
                auto oldCurrentReplicationTimestamp = replicaInfo->GetCurrentReplicationTimestamp();
                auto newCurrentReplicationTimestamp = std::max(oldCurrentReplicationTimestamp, commitTimestamp);
                replicaInfo->SetCurrentReplicationTimestamp(newCurrentReplicationTimestamp);
                YT_LOG_DEBUG(
                    "Sync replicated rows committed (TransactionId: %v, ReplicaId: %v, CurrentReplicationTimestamp: %v -> %v, "
                    "TotalRowCount: %v)",
                    transaction->GetId(),
                    replicaInfo->GetId(),
                    oldCurrentReplicationTimestamp,
                    newCurrentReplicationTimestamp,
                    tablet->GetTotalRowCount());
            }

            if (!syncReplicas.empty()) {
                Host_->AdvanceReplicatedTrimmedRowCount(Tablet_, transaction);
            }
        } else {
            CommitLocklessRows(transaction, /*delayed*/ false);
        }

        if (NeedsSerialization(transaction)) {
            YT_LOG_DEBUG(
                "Transaction requires serialization in tablet (TransactionId: %v)",
                transaction->GetId());

            transaction->SerializingTabletIds().insert(Tablet_->GetId());
        } else {
            OnTransactionFinished(transaction);
        }
    }

    void OnTransactionAborted(TTransaction* transaction) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        AbortLocklessRows(transaction);
        AbortLockedRows(transaction);
        AbortPrelockedRows(transaction);

        OnTransactionFinished(transaction);
    }

    void OnTransactionSerialized(TTransaction* transaction) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto transientWriteState = GetOrCreateTransactionTransientWriteState(transaction->GetId());
        YT_VERIFY(transientWriteState->PrelockedRows.empty());

        // COMPAT(ponasenko-rs)
        if (auto reign = static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign);
            reign < ETabletReign::SharedWriteLocks)
        {
            YT_VERIFY(transientWriteState->LockedRows.empty());
            CommitLocklessRows(transaction, /*delayed*/ true);
        } else {
            if (transientWriteState->LockedRows.empty()) {
                CommitLocklessRows(transaction, /*delayed*/ true);
            } else {
                CommitLockedRows(transaction);
            }
        }

        EraseOrCrash(transaction->SerializingTabletIds(), Tablet_->GetId());
        YT_VERIFY(!NeedsSerialization(transaction));

        OnTransactionFinished(transaction);
    }

    void OnTransactionTransientReset(TTransaction* transaction) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!Tablet_->GetStoreManager()) {
            // NB: OnStopLeading can be called prior to OnAfterSnapshotLoaded.
            // In this case, tablet does not have store manager initialized and
            // relock cannot be performed, however no rows are actually locked, so
            // we can just do nothing.
            if (auto transientWriteState = FindTransactionTransientWriteState(transaction->GetId())) {
                YT_VERIFY(transientWriteState->PrelockedRows.empty());
                YT_VERIFY(transientWriteState->LockedRows.empty());
                YT_VERIFY(!transaction->GetTransient());
            }

            return;
        }

        // TODO: Some keys may be both prelocked and referenced in write log
        // in different generations, so this code is incorrect if tablet write
        // retries are enabled.
        AbortPrelockedRows(transaction);

        // If transaction is transient, it is going to be removed, so we drop its write states.
        if (transaction->GetTransient()) {
            EraseOrCrash(TransactionIdToTransientWriteState_, transaction->GetId());
        }
    }

    void OnTransientGenerationPromoted(TTransaction* transaction) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AbortPrelockedRows(transaction);
        AbortLockedRows(transaction);
    }

    void OnPersistentGenerationPromoted(TTransaction* transaction) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        DropTransactionWriteLogs(transaction);
    }

    bool NeedsSerialization(TTransaction* transaction) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // COMPAT(ponasenko-rs)
        if (auto reign = static_cast<ETabletReign>(GetCurrentMutationContext()->Request().Reign);
            reign < ETabletReign::SharedWriteLocks)
        {
            return NeedsLocklessSerialization(transaction);
        }

        return NeedsLocklessSerialization(transaction) || NeedsSortedSharedWriteSerialization(transaction);
    }

    void UpdateReplicationProgress(TTransaction* transaction) override
    {
        YT_VERIFY(transaction->Actions().empty());

        auto commitTimestamp = transaction->GetCommitTimestamp();

        auto progress = Tablet_->RuntimeData()->ReplicationProgress.Acquire();
        auto maxTimestamp = GetReplicationProgressMaxTimestamp(*progress);
        if (maxTimestamp >= commitTimestamp) {
            YT_LOG_ALERT("Tablet replication progress is beyond current serialized transaction commit timestamp "
                "(TabletId: %v, TransactionId: %v, CommitTimestamp: %v, MaxReplicationProgressTimestamp: %v, ReplicationProgress: %v)",
                Tablet_->GetId(),
                transaction->GetId(),
                commitTimestamp,
                maxTimestamp,
                static_cast<TReplicationProgress>(*progress));
        } else {
            auto newProgress = AdvanceReplicationProgress(*progress, commitTimestamp);
            progress = New<TRefCountedReplicationProgress>(std::move(newProgress));
            Tablet_->RuntimeData()->ReplicationProgress.Store(progress);

            YT_LOG_DEBUG("Replication progress updated (TabletId: %v, TransactionId: %v, ReplicationProgress: %v)",
                Tablet_->GetId(),
                transaction->GetId(),
                static_cast<TReplicationProgress>(*progress));
        }
    }

    void BuildOrchidYson(TTransaction* transaction, IYsonConsumer* consumer) override
    {
        const auto& transientWriteState = FindTransactionTransientWriteState(transaction->GetId());
        const auto& persistentWriteState = FindTransactionPersistentWriteState(transaction->GetId());
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("locked_row_count").Value(transientWriteState ? transientWriteState->LockedRows.size() : 0)
                .Item("prelocked_row_count").Value(transientWriteState ? transientWriteState->PrelockedRows.size() : 0)
                .Item("locked_write_log_size").Value(persistentWriteState ? persistentWriteState->LockedWriteLog.Size() : 0)
                .Item("lockless_write_log_size").Value(persistentWriteState ? persistentWriteState->LocklessWriteLog.Size() : 0)
            .EndMap();
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        YT_VERIFY(
            transaction->GetPersistentState() == ETransactionState::Committed ||
            transaction->GetPersistentState() == ETransactionState::Serialized ||
            transaction->GetPersistentState() == ETransactionState::Aborted);

        if (transaction->GetPersistentState() != ETransactionState::Aborted) {
            FinishCommit(transaction, transaction->GetId(), transaction->GetCommitTimestamp());
        }

        Tablet_->RecomputeReplicaStatuses();

        RemovePreparedTransactionFromBarrier(transaction);
        DropTransactionWriteLogs(transaction);
        TransactionIdToPersistentWriteState_.erase(transaction->GetId());
        TransactionIdToTransientWriteState_.erase(transaction->GetId());

        YT_LOG_DEBUG(
            "Transaction finished in tablet (TransactionId: %v)",
            transaction->GetId());
    }

    bool HasUnfinishedTransientTransactions() const override
    {
        return !TransactionIdToTransientWriteState_.empty();
    }

    bool HasUnfinishedPersistentTransactions() const override
    {
        return !TransactionIdToPersistentWriteState_.empty();
    }

    void StartEpoch() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // NB: Could be null in tests.
        if (!Host_) {
            return;
        }

        const auto& transactionManager = Host_->GetTransactionManager();
        for (const auto& [transactionId, writeState] : TransactionIdToPersistentWriteState_) {
            if (writeState->RowsPrepared) {
                auto* transaction = transactionManager->GetPersistentTransaction(transactionId);
                InsertPreparedTransactionToBarrier(transaction, writeState);
            }
        }
    }

    void StopEpoch() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (const auto& [transactionId, writeState] : TransactionIdToPersistentWriteState_) {
            writeState->PreparedBarrierCookie = InvalidAsyncBarrierCookie;
        }

        const auto& runtimeData = Tablet_->RuntimeData();
        runtimeData->PreparedTransactionBarrier.Clear(TError("Epoch stopped"));
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionIdToTransientWriteState_.clear();
        TransactionIdToPersistentWriteState_.clear();

        WriteLogsMemoryTrackerGuard_.SetSize(0);
    }

    void Save(TSaveContext& context) const override
    {
        using NYT::Save;

        TMapSerializer<TDefaultSerializer, TNonNullableIntrusivePtrSerializer<TDefaultSerializer>>::Save(context, TransactionIdToPersistentWriteState_);
    }

    void Load(TLoadContext& context) override
    {
        using NYT::Load;

        TMapSerializer<TDefaultSerializer, TNonNullableIntrusivePtrSerializer<TDefaultSerializer>>::Load(context, TransactionIdToPersistentWriteState_);
    }

    TCallback<void(TSaveContext&)> AsyncSave() override
    {
        std::vector<std::pair<TTransactionId, TCallback<void(TSaveContext&)>>> transactions;
        transactions.reserve(TransactionIdToPersistentWriteState_.size());
        for (const auto& [transactionId, writeState] : TransactionIdToPersistentWriteState_) {
            transactions.emplace_back(transactionId, writeState->AsyncSave());
        }

        return BIND([transactions = std::move(transactions)] (TSaveContext& context) mutable {
            using NYT::Save;

            SortBy(transactions, [] (const auto& pair) { return pair.first; });
            for (const auto& [transactionId, callback] : transactions) {
                Save(context, transactionId);
                callback(context);
            }
        });
    }

    void AsyncLoad(TLoadContext& context) override
    {
        using NYT::Load;

        for (int index = 0; index < std::ssize(TransactionIdToPersistentWriteState_); ++index) {
            auto transactionId = Load<TTransactionId>(context);
            const auto& writeState = GetOrCrash(TransactionIdToPersistentWriteState_, transactionId);
            writeState->AsyncLoad(context);
        }
    }

    void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& transactionManager = Host_->GetTransactionManager();
        for (const auto& [transactionId, writeState] : TransactionIdToPersistentWriteState_) {
            auto* transaction = transactionManager->GetPersistentTransaction(transactionId);

            for (const auto& writeRecord : writeState->LockedWriteLog) {
                LockRows(transaction, writeRecord);
                UpdateWriteRecordCounters(transaction, writeRecord);
            }

            for (const auto& writeRecord : writeState->LocklessWriteLog) {
                UpdateWriteRecordCounters(transaction, writeRecord);
            }

            if (writeState->RowsPrepared) {
                PrepareLockedRows(transaction);
                PrepareLocklessRows(transaction, /*persistent*/ true, /*snapshotLoading*/ true);
            }
        }

        Tablet_->RecomputeReplicaStatuses();
        Tablet_->RecomputeCommittedReplicationRowIndices();
    }

private:
    TTablet* const Tablet_;
    ITabletContext* const Context_;
    ITabletWriteManagerHost* const Host_;

    const NLogging::TLogger Logger;

    // NB: Write logs are generally much smaller than dynamic stores,
    // so we don't worry about per-pool management here.
    TMemoryUsageTrackerGuard WriteLogsMemoryTrackerGuard_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    struct TTransactionPersistentWriteState final
    {
        TTransactionWriteLog LocklessWriteLog;
        TTransactionWriteLog LockedWriteLog;

        bool RowsPrepared = false;

        // NB: Not persisted. Only valid during an epoch.
        TAsyncBarrierCookie PreparedBarrierCookie = InvalidAsyncBarrierCookie;

        void Save(TSaveContext& context) const
        {
            using NYT::Save;

            Save(context, RowsPrepared);
        }

        void Load(TLoadContext& context)
        {
            using NYT::Load;

            Load(context, RowsPrepared);
        }

        TCallback<void(TSaveContext&)> AsyncSave()
        {
            return BIND([
                locklessWriteLogSnapshot = LocklessWriteLog.MakeSnapshot(),
                lockedWriteLogSnapshot = LockedWriteLog.MakeSnapshot()
            ] (TSaveContext& context) {
                using NYT::Save;

                Save(context, locklessWriteLogSnapshot);
                Save(context, lockedWriteLogSnapshot);
            });
        }

        void AsyncLoad(TLoadContext& context)
        {
            using NYT::Load;

            Load(context, LocklessWriteLog);
            Load(context, LockedWriteLog);
        }
    };
    using TTransactionPersistentWriteStatePtr = TIntrusivePtr<TTransactionPersistentWriteState>;

    struct TTransactionTransientWriteState final
    {
        TRingQueue<TSortedDynamicRowRef> PrelockedRows;
        std::vector<TSortedDynamicRowRef> LockedRows;
    };
    using TTransactionTransientWriteStatePtr = TIntrusivePtr<TTransactionTransientWriteState>;

    THashMap<TTransactionId, TTransactionPersistentWriteStatePtr> TransactionIdToPersistentWriteState_;
    THashMap<TTransactionId, TTransactionTransientWriteStatePtr> TransactionIdToTransientWriteState_;

    TTransactionPersistentWriteStatePtr FindTransactionPersistentWriteState(TTransactionId transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(TypeFromId(transactionId) != EObjectType::NonAtomicTabletTransaction);

        return GetOrDefault(TransactionIdToPersistentWriteState_, transactionId);
    }

    TTransactionPersistentWriteStatePtr GetOrCreateTransactionPersistentWriteState(TTransactionId transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());
        YT_VERIFY(TypeFromId(transactionId) != EObjectType::NonAtomicTabletTransaction);

        auto it = TransactionIdToPersistentWriteState_.find(transactionId);
        if (it == TransactionIdToPersistentWriteState_.end()) {
            auto writeState = New<TTransactionPersistentWriteState>();
            EmplaceOrCrash(TransactionIdToPersistentWriteState_, transactionId, writeState);
            return writeState;
        } else {
            return it->second;
        }
    }

    TTransactionTransientWriteStatePtr FindTransactionTransientWriteState(TTransactionId transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(TypeFromId(transactionId) != EObjectType::NonAtomicTabletTransaction);

        return GetOrDefault(TransactionIdToTransientWriteState_, transactionId);
    }

    TTransactionTransientWriteStatePtr GetOrCreateTransactionTransientWriteState(TTransactionId transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(TypeFromId(transactionId) != EObjectType::NonAtomicTabletTransaction);

        auto it = TransactionIdToTransientWriteState_.find(transactionId);
        if (it == TransactionIdToTransientWriteState_.end()) {
            auto writeState = New<TTransactionTransientWriteState>();
            EmplaceOrCrash(TransactionIdToTransientWriteState_, transactionId, writeState);
            return writeState;
        } else {
            return it->second;
        }
    }

    //! Returns true if transaction has either transient or persistent
    //! write state and false otherwise.
    bool HasWriteState(TTransactionId transactionId)
    {
        return
            FindTransactionTransientWriteState(transactionId) ||
            FindTransactionPersistentWriteState(transactionId);
    }

    void InsertPreparedTransactionToBarrier(TTransaction* transaction, const TTransactionPersistentWriteStatePtr& writeState)
    {
        if (!Tablet_->IsPhysicallyOrdered()) {
            return;
        }

        // Transaction is already inserted into the barrier.
        if (writeState->PreparedBarrierCookie != InvalidAsyncBarrierCookie) {
            return;
        }

        YT_LOG_DEBUG("Transaction inserted into per-tablet barrier (TransactionId: %v)",
            transaction->GetId());

        const auto& runtimeData = Tablet_->RuntimeData();
        auto cookie = runtimeData->PreparedTransactionBarrier.Insert();
        writeState->PreparedBarrierCookie = cookie;
    }

    void RemovePreparedTransactionFromBarrier(TTransaction* transaction)
    {
        if (!Tablet_->IsPhysicallyOrdered()) {
            return;
        }

        auto writeState = FindTransactionPersistentWriteState(transaction->GetId());
        if (!writeState) {
            return;
        }

        auto cookie = std::exchange(writeState->PreparedBarrierCookie, InvalidAsyncBarrierCookie);
        if (cookie == InvalidAsyncBarrierCookie) {
            return;
        }

        YT_LOG_DEBUG("Transaction removed from per-tablet barrier (TransactionId: %v)",
            transaction->GetId());

        const auto& runtimeData = Tablet_->RuntimeData();
        runtimeData->PreparedTransactionBarrier.Remove(cookie);
    }

    void UpdateWriteRecordCounters(
        TTransaction* transaction,
        const TTransactionWriteRecord& writeRecord,
        int multiplier = 1)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        WriteLogsMemoryTrackerGuard_.IncreaseSize(writeRecord.GetByteSize() * multiplier);
        bool replicatorWrite = IsReplicatorWrite(transaction);
        IncrementTabletPendingWriteRecordCount(replicatorWrite, multiplier);
    }

    void EnqueueTransactionWriteRecord(
        TTransaction* transaction,
        const TTransactionWriteRecord& writeRecord,
        bool lockless)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto persistentWriteState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        auto* writeLog = lockless ? &persistentWriteState->LocklessWriteLog : &persistentWriteState->LockedWriteLog;
        writeLog->Enqueue(writeRecord);

        UpdateWriteRecordCounters(transaction, writeRecord);

        YT_LOG_DEBUG(
            "Write record enqueued (TransactionId: %v, Size: %v, RowCount: %v, Lockless: %v)",
            transaction->GetId(),
            writeRecord.DataWeight,
            writeRecord.RowCount,
            lockless);
    }

    void DropTransactionWriteLog(
        TTransaction* transaction,
        TTransactionWriteLog* writeLog)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        for (const auto& writeRecord : *writeLog) {
            UpdateWriteRecordCounters(transaction, writeRecord, /*multiplier*/ -1);
        }
        writeLog->Clear();
    }

    void DropTransactionWriteLogs(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        const auto& persistentWriteState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        auto lockedRowCount = GetWriteLogRowCount(persistentWriteState->LockedWriteLog);
        auto locklessRowCount = GetWriteLogRowCount(persistentWriteState->LocklessWriteLog);

        YT_LOG_DEBUG_IF(
            lockedRowCount > 0 || locklessRowCount > 0,
            "Dropping transaction write logs "
            "(TransactionId: %v, LockedRowCount: %v, LocklessRowCount: %v)",
            transaction->GetId(),
            lockedRowCount,
            locklessRowCount);

        DropTransactionWriteLog(transaction, &persistentWriteState->LockedWriteLog);
        DropTransactionWriteLog(transaction, &persistentWriteState->LocklessWriteLog);
    }

    void PrepareLocklessRows(TTransaction* transaction, bool persistent, bool snapshotLoading = false)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(!persistent || HasHydraContext());

        if (!persistent) {
            return;
        }

        auto persistentWriteState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        if (IsReplicatorWrite(transaction) && !persistentWriteState->LocklessWriteLog.Empty()) {
            Tablet_->PreparedReplicatorTransactionIds().insert(transaction->GetId());
        }

        if (!NeedsLocklessSerialization(transaction)) {
            return;
        }

        const auto& locklessWriteLog = persistentWriteState->LocklessWriteLog;

        if (!snapshotLoading) {
            for (const auto& writeRecord : locklessWriteLog) {
                // TODO(ifsmirnov): No bulk insert into replicated tables. Remove this check?
                const auto& lockManager = Tablet_->GetLockManager();
                if (auto error = lockManager->ValidateTransactionConflict(transaction->GetStartTimestamp());
                    !error.IsOK())
                {
                    THROW_ERROR error << TErrorAttribute("tablet_id", Tablet_->GetId());
                }

                ValidateSyncReplicaSet(writeRecord.SyncReplicaIds);
                for (auto& [replicaId, replicaInfo] : Tablet_->Replicas()) {
                    ValidateReplicaWritable(replicaInfo);
                }
            }
        }

        UpdateLocklessRowCounters(transaction, ETransactionState::PersistentCommitPrepared, snapshotLoading);
    }

    void CommitLocklessRows(TTransaction* transaction, bool delayed)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto writeState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        auto& locklessWriteLog = writeState->LocklessWriteLog;
        if (locklessWriteLog.Empty()) {
            return;
        }

        auto commitTimestamp = transaction->GetCommitTimestamp();

        int committedRowCount = 0;
        TCompactFlatMap<TTableReplicaInfo*, int, 8> replicaToCommittedRowCount;
        for (const auto& record : locklessWriteLog) {
            auto context = CreateWriteContext(transaction);
            context.HunkChunksInfo = record.HunkChunksInfo;
            context.Phase = EWritePhase::Commit;
            context.CommitTimestamp = commitTimestamp;

            auto reader = CreateWireProtocolReader(record.Data);

            const auto& storeManager = Tablet_->GetStoreManager();
            YT_VERIFY(storeManager->ExecuteWrites(reader.get(), &context));
            YT_VERIFY(context.RowCount == record.RowCount);

            committedRowCount += record.RowCount;

            for (auto replicaId : record.SyncReplicaIds) {
                auto* replicaInfo = Tablet_->FindReplicaInfo(replicaId);
                if (!replicaInfo) {
                    continue;
                }

                replicaToCommittedRowCount[replicaInfo] += record.RowCount;
            }
        }

        YT_LOG_DEBUG(
            "Lockless rows committed (TransactionId: %v, RowCount: %v)",
            transaction->GetId(),
            committedRowCount);

        if (delayed && Tablet_->IsPhysicallyLog()) {
            auto oldDelayedLocklessRowCount = Tablet_->GetDelayedLocklessRowCount();
            auto newDelayedLocklessRowCount = oldDelayedLocklessRowCount - committedRowCount;
            Tablet_->SetDelayedLocklessRowCount(newDelayedLocklessRowCount);
            Tablet_->RecomputeReplicaStatuses();
            YT_LOG_DEBUG(
                "Delayed lockless rows committed (TransactionId: %v, DelayedLocklessRowCount: %v -> %v)",
                transaction->GetId(),
                oldDelayedLocklessRowCount,
                newDelayedLocklessRowCount);

            for (auto [replicaInfo, rowCount] : replicaToCommittedRowCount) {
                auto oldCommittedReplicationRowIndex = replicaInfo->GetCommittedReplicationRowIndex();
                auto newCommittedReplicationRowIndex = oldCommittedReplicationRowIndex + rowCount;
                replicaInfo->SetCommittedReplicationRowIndex(newCommittedReplicationRowIndex);

                YT_LOG_DEBUG(
                    "Delayed lockless rows committed "
                    "(TransactionId: %v, TabletId: %v, ReplicaId: %v, CommittedReplicationRowIndex: %v -> %v, TotalRowCount: %v)",
                    transaction->GetId(),
                    Tablet_->GetId(),
                    replicaInfo->GetId(),
                    oldCommittedReplicationRowIndex,
                    newCommittedReplicationRowIndex,
                    Tablet_->GetTotalRowCount());
            }
        }

        if (IsReplicatorWrite(transaction)) {
            if (Tablet_->PreparedReplicatorTransactionIds().erase(transaction->GetId()) == 0) {
                YT_LOG_ALERT("Unknown replicator transaction committed (%v, TransactionId: %v)",
                    Tablet_->GetLoggingTag(),
                    transaction->GetId());
            }

            // May be null in tests.
            if (const auto& backupManager = Host_->GetBackupManager()) {
                backupManager->ValidateReplicationTransactionCommit(Tablet_, transaction);
                backupManager->OnReplicatorWriteTransactionFinished(Tablet_);
            }
        }

        DropTransactionWriteLog(transaction, &locklessWriteLog);
    }

    void AbortLocklessRows(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto writeState = FindTransactionPersistentWriteState(transaction->GetId());
        if (!writeState) {
            return;
        }

        // Rows are not prepared - nothing to abort.
        if (!writeState->RowsPrepared) {
            return;
        }

        UpdateLocklessRowCounters(transaction, ETransactionState::Aborted);

        if (IsReplicatorWrite(transaction) && !writeState->LocklessWriteLog.Empty()) {
            if (Tablet_->PreparedReplicatorTransactionIds().erase(transaction->GetId()) == 0) {
                YT_LOG_DEBUG("Unknown replicator transaction aborted (%v, TransactionId: %v)",
                    Tablet_->GetLoggingTag(),
                    transaction->GetId());
            }

            // May be null in tests.
            if (const auto& backupManager = Host_->GetBackupManager()) {
                backupManager->OnReplicatorWriteTransactionFinished(Tablet_);
            }
        }
    }

    void UpdateLocklessRowCounters(
        TTransaction* transaction,
        ETransactionState state,
        bool snapshotLoading = false)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());
        YT_VERIFY(
            state == ETransactionState::PersistentCommitPrepared ||
            state == ETransactionState::Aborted);
        if (snapshotLoading) {
            YT_VERIFY(state == ETransactionState::PersistentCommitPrepared);
        }

        if (!NeedsLocklessSerialization(transaction)) {
            return;
        }

        int multiplier = state == ETransactionState::PersistentCommitPrepared ? 1 : -1;

        TCompactFlatMap<TTableReplicaInfo*, int, 8> replicaToRowCount;
        int rowCount = 0;

        auto persistentWriteState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        const auto& locklessWriteLog = persistentWriteState->LocklessWriteLog;
        for (const auto& writeRecord : locklessWriteLog) {
            for (auto replicaId : writeRecord.SyncReplicaIds) {
                auto* replicaInfo = Tablet_->FindReplicaInfo(replicaId);
                if (!replicaInfo) {
                    continue;
                }
                replicaToRowCount[replicaInfo] += writeRecord.RowCount;
            }

            rowCount += writeRecord.RowCount;
        }

        // NB: Replication row index is stored into snapshot, so we do not recompute it
        // in OnAfterSnapshotLoaded.
        if (!snapshotLoading) {
            for (auto [replicaInfo, rowCount] : replicaToRowCount) {
                const auto* tablet = replicaInfo->GetTablet();
                auto oldCurrentReplicationRowIndex = replicaInfo->GetCurrentReplicationRowIndex();
                auto newCurrentReplicationRowIndex = oldCurrentReplicationRowIndex + rowCount * multiplier;
                replicaInfo->SetCurrentReplicationRowIndex(newCurrentReplicationRowIndex);
                YT_LOG_DEBUG(
                    "Sync replicated rows %v (TransactionId: %v, ReplicaId: %v, CurrentReplicationRowIndex: %v -> %v, "
                    "TotalRowCount: %v)",
                    state == ETransactionState::Aborted ? "aborted" : "prepared",
                    transaction->GetId(),
                    replicaInfo->GetId(),
                    oldCurrentReplicationRowIndex,
                    newCurrentReplicationRowIndex,
                    tablet->GetTotalRowCount());
            }
        }

        if (rowCount > 0 && Tablet_->IsPhysicallyLog()) {
            auto oldDelayedLocklessRowCount = Tablet_->GetDelayedLocklessRowCount();
            auto newDelayedLocklessRowCount = oldDelayedLocklessRowCount + rowCount * multiplier;
            Tablet_->SetDelayedLocklessRowCount(newDelayedLocklessRowCount);
            Tablet_->RecomputeReplicaStatuses();
            YT_LOG_DEBUG(
                "Delayed lockless rows %v (TransactionId: %v, DelayedLocklessRowCount: %v -> %v)",
                state == ETransactionState::Aborted ? "aborted" : "prepared",
                transaction->GetId(),
                oldDelayedLocklessRowCount,
                newDelayedLocklessRowCount);
        }
    }

    void LockRows(
        TTransaction* transaction,
        const TTransactionWriteRecord& writeRecord)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto reader = CreateWireProtocolReader(writeRecord.Data);
        auto context = CreateWriteContext(transaction);
        context.Phase = EWritePhase::Lock;

        const auto& storeManager = Tablet_->GetStoreManager();
        YT_VERIFY(storeManager->ExecuteWrites(reader.get(), &context));

        if (context.HasSharedWriteLocks) {
            transaction->SetHasSharedWriteLocks(true);
        }

        YT_LOG_DEBUG(
            "Rows locked (TransactionId: %v, RowCount: %v)",
            transaction->GetId(),
            context.RowCount);
    }

    void PrepareLockedRows(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto transactionId = transaction->GetId();

        auto prepareRow = [&] (const TSortedDynamicRowRef& rowRef) {
            // NB: Don't call ValidateAndDiscardRowRef, row refs are just scanned.
            if (rowRef.Store->GetStoreState() != EStoreState::Orphaned) {
                rowRef.StoreManager->PrepareRow(transaction, rowRef);
            }
        };

        auto transientWriteState = GetOrCreateTransactionTransientWriteState(transactionId);
        const auto& lockedRows = transientWriteState->LockedRows;
        for (const auto& lockedRow : lockedRows) {
            prepareRow(lockedRow);
        }

        YT_LOG_DEBUG_IF(
            std::ssize(lockedRows) > 0,
            "Locked rows prepared (TransactionId: %v, LockedRowCount: %v)",
            transaction->GetId(),
            lockedRows.size());

        auto& prelockedRows = transientWriteState->PrelockedRows;
        for (const auto& prelockedRow : TRingQueueIterableWrapper(prelockedRows)) {
            prepareRow(prelockedRow);
        }

        YT_LOG_DEBUG_IF(
            std::ssize(prelockedRows) > 0,
            "Prelocked rows prepared (TransactionId: %v, PrelockedRowCount: %v)",
            transactionId,
            prelockedRows.size());
    }

    void CommitLockedRows(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto persistentWriteState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        auto transientWriteState = GetOrCreateTransactionTransientWriteState(transaction->GetId());

        YT_VERIFY(transientWriteState->PrelockedRows.empty());
        auto& lockedRows = transientWriteState->LockedRows;
        auto& writeLog = persistentWriteState->LockedWriteLog;
        auto lockedRowCount = lockedRows.size();

        if (lockedRows.empty()) {
            return;
        }

        std::optional<int> keyMismatchIndex;
        auto shuffleLockedRows = Host_->GetConfig()->ShuffleLockedRows;

        if (shuffleLockedRows) {
            YT_LOG_DEBUG("Shuffling locked rows (TransactionId: %v, LockedRowCount: %v)",
                transaction->GetId(),
                lockedRows.size());

            if (std::ssize(lockedRows) == 2) {
                std::reverse(lockedRows.begin(), lockedRows.end());
            } else {
                std::reverse(lockedRows.begin() + std::ssize(lockedRows) / 2, lockedRows.end());
            }
        }

        auto writeLogIterator = writeLog.Begin();
        auto writeLogReader = CreateWireProtocolReader((*writeLogIterator).Data);

        for (int index = 0; index < std::ssize(lockedRows); ++index) {
            const auto& rowRef = lockedRows[index];
            while (writeLogReader->IsFinished()) {
                ++writeLogIterator;
                YT_VERIFY(writeLogIterator != writeLog.End());
                writeLogReader = CreateWireProtocolReader((*writeLogIterator).Data);
            }

            auto* tablet = rowRef.StoreManager->GetTablet();
            auto command = writeLogReader->ReadWriteCommand(
                tablet->TableSchemaData(),
                /*captureValues*/ false);

            if (!Host_->ValidateAndDiscardRowRef(rowRef)) {
                continue;
            }

            if (!rowRef.StoreManager->CommitRow(transaction, command, rowRef)) {
                keyMismatchIndex = index;
                break;
            }

            Host_->OnTabletRowUnlocked(tablet);
        }

        if (keyMismatchIndex) {
            if (!shuffleLockedRows) {
                YT_LOG_ALERT("Key mismatch between locked row list and immediate locked write log detected "
                    "(MismatchIndex: %v)",
                    *keyMismatchIndex);
            }

            using TCommandList =
                std::vector<
                    std::pair<
                        TUnversionedValueRange,
                        TWireProtocolWriteCommand
                    >
                >;
            TCommandList commands;

            auto rowBuffer = New<TRowBuffer>();
            for (const auto& writeRecord : writeLog) {
                auto keyColumnCount = Tablet_->GetPhysicalSchema()->GetKeyColumnCount();
                auto getKey = [&] (TUnversionedRow row) {
                    YT_VERIFY(static_cast<int>(row.GetCount()) >= keyColumnCount);
                    return ToKeyRef(row, keyColumnCount);
                };

                auto reader = CreateWireProtocolReader(writeRecord.Data, rowBuffer);
                while (!reader->IsFinished()) {
                    auto command = reader->ReadWriteCommand(
                        Tablet_->TableSchemaData(),
                        /*captureValues*/ true);

                    TUnversionedValueRange key;
                    Visit(command,
                        [&] (const TWriteRowCommand& command) { key = getKey(command.Row); },
                        [&] (const TDeleteRowCommand& command) { key = getKey(command.Row); },
                        [&] (const TWriteAndLockRowCommand& command) { key = getKey(command.Row); },
                        [&] (auto) { YT_ABORT(); });
                    commands.emplace_back(key, command);
                }
            }

            const auto& comparer = Tablet_->GetRowKeyComparer();

            std::sort(commands.begin(), commands.end(), [&] (const auto& lhs, const auto& rhs) {
                return comparer(lhs.first, rhs.first) < 0;
            });
            for (int index = 0; index + 1 < std::ssize(commands); ++index) {
                const auto& key = commands[index].first;
                const auto& nextKey = commands[index + 1].first;
                // All keys must be different.
                YT_VERIFY(comparer(key, nextKey) < 0);
            }

            for (int index = *keyMismatchIndex; index < std::ssize(lockedRows); ++index) {
                const auto& lockedRow = lockedRows[index];
                if (!Host_->ValidateAndDiscardRowRef(lockedRow)) {
                    continue;
                }

                const auto& row = lockedRow.Row;
                auto commandIt = std::lower_bound(
                    commands.begin(),
                    commands.end(),
                    row,
                    [&] (const auto& command, const auto& row) {
                        return comparer(command.first, row) < 0;
                    });

                const auto& [commandKey, command] = *commandIt;
                YT_VERIFY(comparer(commandKey, row) == 0);

                YT_VERIFY(lockedRow.StoreManager->CommitRow(transaction, command, lockedRow));

                ++lockedRowCount;
                Host_->OnTabletRowUnlocked(Tablet_);
            }
        }

        DropTransactionWriteLog(transaction, &writeLog);
        lockedRows.clear();

        YT_LOG_DEBUG(
            "Locked rows committed (TransactionId: %v, LockedRowCount: %v)",
            transaction->GetId(),
            lockedRowCount);
    }

    void AbortPrelockedRows(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto writeState = GetOrCreateTransactionTransientWriteState(transaction->GetId());
        auto& prelockedRows = writeState->PrelockedRows;
        auto prelockedRowCount = prelockedRows.size();

        for (const auto& prelockedRow : TRingQueueIterableWrapper(prelockedRows)) {
            if (Host_->ValidateAndDiscardRowRef(prelockedRow)) {
                prelockedRow.StoreManager->AbortRow(transaction, prelockedRow);
                Host_->OnTabletRowUnlocked(Tablet_);
            }
        }

        prelockedRows.clear();

        YT_LOG_DEBUG_IF(
            prelockedRowCount != 0,
            "Prelocked rows aborted (TransactionId: %v, RowCount: %v)",
            transaction->GetId(),
            prelockedRowCount);
    }

    void AbortLockedRows(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto writeState = FindTransactionTransientWriteState(transaction->GetId());
        if (!writeState) {
            return;
        }

        auto& lockedRows = writeState->LockedRows;
        auto lockedRowCount = lockedRows.size();

        for (const auto& lockedRow : lockedRows) {
            if (Host_->ValidateAndDiscardRowRef(lockedRow)) {
                lockedRow.StoreManager->AbortRow(transaction, lockedRow);
                Host_->OnTabletRowUnlocked(Tablet_);
            }
        }

        lockedRows.clear();

        YT_LOG_DEBUG_IF(lockedRowCount > 0,
            "Locked rows aborted (TransactionId: %v, RowCount: %v)",
            transaction->GetId(),
            lockedRowCount);
    }

    void FinishCommit(
        TTransaction* transaction,
        TTransactionId transactionId,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        const auto& hydraManager = Host_->GetHydraManager();

        if (transaction &&
            !transaction->GetForeign() &&
            transaction->GetPrepareTimestamp() != NullTimestamp &&
            Tablet_->GetAtomicity() == EAtomicity::Full &&
            hydraManager &&
            hydraManager->GetAutomatonState() == EPeerState::Leading)
        {
            auto unflushedTimestamp = Tablet_->GetUnflushedTimestamp();
            YT_LOG_ALERT_IF(unflushedTimestamp > commitTimestamp,
                "Inconsistent unflushed timestamp (UnflushedTimestamp: %v, CommitTimestamp: %v)",
                unflushedTimestamp,
                commitTimestamp);
        }

        Tablet_->UpdateLastCommitTimestamp(commitTimestamp);

        if (Tablet_->IsPhysicallyOrdered()) {
            auto oldTotalRowCount = Tablet_->GetTotalRowCount();
            Tablet_->UpdateTotalRowCount();
            Tablet_->GetStoreManager()->UpdateCommittedStoreRowCount();
            auto newTotalRowCount = Tablet_->GetTotalRowCount();
            YT_LOG_DEBUG_IF(oldTotalRowCount != newTotalRowCount,
                "Tablet total row count updated (TabletId: %v, TotalRowCount: %v -> %v)",
                Tablet_->GetId(),
                oldTotalRowCount,
                newTotalRowCount);
        }

        YT_LOG_DEBUG(
            "Finished transaction commit in tablet (TabletId: %v, TransactionId: %v, CommitTimestamp: %v)",
            Tablet_->GetId(),
            transactionId,
            commitTimestamp);
    }


    static bool IsReplicatorWrite(const NRpc::TAuthenticationIdentity& identity)
    {
        return identity.User == NSecurityClient::ReplicatorUserName;
    }

    static bool IsReplicatorWrite(TTransaction* transaction)
    {
        return IsReplicatorWrite(transaction->AuthenticationIdentity());
    }

    void IncrementTabletPendingWriteRecordCount(bool replicatorWrite, int delta)
    {
        if (replicatorWrite) {
            Tablet_->SetPendingReplicatorWriteRecordCount(Tablet_->GetPendingReplicatorWriteRecordCount() + delta);
        } else {
            Tablet_->SetPendingUserWriteRecordCount(Tablet_->GetPendingUserWriteRecordCount() + delta);
        }
    }

    void ValidateSyncReplicaSet(const TSyncReplicaIdList& syncReplicaIds)
    {
        for (auto replicaId : syncReplicaIds) {
            const auto* replicaInfo = Tablet_->FindReplicaInfo(replicaId);
            if (!replicaInfo) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::SyncReplicaIsNotKnown,
                    "Synchronous replica %v is not known for tablet %v",
                    replicaId,
                    Tablet_->GetId());
            }
            if (replicaInfo->GetMode() != ETableReplicaMode::Sync) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::SyncReplicaIsNotInSyncMode,
                    "Replica %v of tablet %v is not in sync mode",
                    replicaId,
                    Tablet_->GetId());
            }
        }

        for (const auto& [replicaId, replicaInfo] : Tablet_->Replicas()) {
            if (replicaInfo.GetMode() == ETableReplicaMode::Sync) {
                if (std::find(syncReplicaIds.begin(), syncReplicaIds.end(), replicaId) == syncReplicaIds.end()) {
                    THROW_ERROR_EXCEPTION(
                        NTabletClient::EErrorCode::SyncReplicaIsNotWritten,
                        "Synchronous replica %v of tablet %v is not being written by client",
                        replicaId,
                        Tablet_->GetId());
                }
            }
        }
    }

    void ValidateReplicaStatus(ETableReplicaStatus expected, const TTableReplicaInfo& replicaInfo) const
    {
        YT_LOG_ALERT_IF(
            replicaInfo.GetStatus() != expected,
            "Table replica status mismatch "
            "(Expected: %v, Actual: %v, CurrentReplicationRowIndex: %v, TotalRowCount: %v, DelayedLocklessRowCount: %v, Mode: %v)",
            expected,
            replicaInfo.GetStatus(),
            replicaInfo.GetCurrentReplicationRowIndex(),
            Tablet_->GetTotalRowCount(),
            Tablet_->GetDelayedLocklessRowCount(),
            replicaInfo.GetMode());
    }

    void ValidateReplicaWritable(const TTableReplicaInfo& replicaInfo)
    {
        auto currentReplicationRowIndex = replicaInfo.GetCurrentReplicationRowIndex();
        auto totalRowCount = Tablet_->GetTotalRowCount();
        auto delayedLocklessRowCount = Tablet_->GetDelayedLocklessRowCount();
        switch (replicaInfo.GetMode()) {
            case ETableReplicaMode::Sync: {
                if (currentReplicationRowIndex < totalRowCount + delayedLocklessRowCount) {
                    if (replicaInfo.GetState() == ETableReplicaState::Enabled) {
                        ValidateReplicaStatus(ETableReplicaStatus::SyncCatchingUp, replicaInfo);
                    } else {
                        ValidateReplicaStatus(ETableReplicaStatus::SyncNotWritable, replicaInfo);
                    }
                    THROW_ERROR_EXCEPTION(
                        "Replica %v of tablet %v is not synchronously writeable since some rows are not replicated yet",
                        replicaInfo.GetId(),
                        Tablet_->GetId())
                        << TErrorAttribute("current_replication_row_index", currentReplicationRowIndex)
                        << TErrorAttribute("total_row_count", totalRowCount)
                        << TErrorAttribute("delayed_lockless_row_count", delayedLocklessRowCount);
                }
                if (currentReplicationRowIndex > totalRowCount + delayedLocklessRowCount) {
                    YT_LOG_ALERT(
                        "Current replication row index is too high (TabletId: %v, ReplicaId: %v, "
                        "CurrentReplicationRowIndex: %v, TotalRowCount: %v, DelayedLocklessRowCount: %v)",
                        Tablet_->GetId(),
                        replicaInfo.GetId(),
                        currentReplicationRowIndex,
                        totalRowCount,
                        delayedLocklessRowCount);
                }
                if (replicaInfo.GetState() != ETableReplicaState::Enabled) {
                    ValidateReplicaStatus(ETableReplicaStatus::SyncNotWritable, replicaInfo);
                    THROW_ERROR_EXCEPTION(
                        "Replica %v is not synchronously writeable since it is in %Qlv state",
                        replicaInfo.GetId(),
                        replicaInfo.GetState());
                }
                ValidateReplicaStatus(ETableReplicaStatus::SyncInSync, replicaInfo);
                YT_VERIFY(!replicaInfo.GetPreparedReplicationTransactionId());
                break;
            }

            case ETableReplicaMode::Async:
                if (currentReplicationRowIndex > totalRowCount) {
                    ValidateReplicaStatus(ETableReplicaStatus::AsyncNotWritable, replicaInfo);
                    THROW_ERROR_EXCEPTION(
                        "Replica %v of tablet %v is not asynchronously writeable: some synchronous writes are still in progress",
                        replicaInfo.GetId(),
                        Tablet_->GetId())
                        << TErrorAttribute("current_replication_row_index", currentReplicationRowIndex)
                        << TErrorAttribute("total_row_count", totalRowCount);
                }

                if (currentReplicationRowIndex >= totalRowCount + delayedLocklessRowCount) {
                    ValidateReplicaStatus(ETableReplicaStatus::AsyncInSync, replicaInfo);
                } else {
                    ValidateReplicaStatus(ETableReplicaStatus::AsyncCatchingUp, replicaInfo);
                }

                break;

            default:
                YT_ABORT();
        }
    }

    void ValidateWriteBarrier(bool replicatorWrite)
    {
        if (replicatorWrite) {
            if (Tablet_->GetInFlightUserMutationCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::ReplicatorWriteBlockedByUser,
                    "Tablet cannot accept replicator writes since some user mutations are still in flight")
                    << TErrorAttribute("tablet_id", Tablet_->GetId())
                    << TErrorAttribute("table_path", Tablet_->GetTablePath())
                    << TErrorAttribute("in_flight_mutation_count", Tablet_->GetInFlightUserMutationCount());
            }
            if (Tablet_->GetPendingUserWriteRecordCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::ReplicatorWriteBlockedByUser,
                    "Tablet cannot accept replicator writes since some user writes are still pending")
                    << TErrorAttribute("tablet_id", Tablet_->GetId())
                    << TErrorAttribute("table_path", Tablet_->GetTablePath())
                    << TErrorAttribute("pending_write_record_count", Tablet_->GetPendingUserWriteRecordCount());
            }
        } else {
            if (Tablet_->GetInFlightReplicatorMutationCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::UserWriteBlockedByReplicator,
                    "Tablet cannot accept user writes since some replicator mutations are still in flight")
                    << TErrorAttribute("tablet_id", Tablet_->GetId())
                    << TErrorAttribute("table_path", Tablet_->GetTablePath())
                    << TErrorAttribute("in_flight_mutation_count", Tablet_->GetInFlightReplicatorMutationCount());
            }
            if (Tablet_->GetPendingReplicatorWriteRecordCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::UserWriteBlockedByReplicator,
                    "Tablet cannot accept user writes since some replicator writes are still pending")
                    << TErrorAttribute("tablet_id", Tablet_->GetId())
                    << TErrorAttribute("table_path", Tablet_->GetTablePath())
                    << TErrorAttribute("pending_write_record_count", Tablet_->GetPendingReplicatorWriteRecordCount());
            }
        }
    }

    void ValidateTransactionActive(TTransaction* transaction)
    {
        if (transaction->GetTransientState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }
    }

    bool NeedsLocklessSerialization(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto persistentWriteState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        return
            !persistentWriteState->LocklessWriteLog.Empty() &&
            Tablet_->GetCommitOrdering() == ECommitOrdering::Strong;
    }

    bool NeedsSortedSharedWriteSerialization(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto persistentWriteState = GetOrCreateTransactionPersistentWriteState(transaction->GetId());
        return transaction->GetHasSharedWriteLocks() &&
            !persistentWriteState->LockedWriteLog.Empty();
    }

    TWriteContext CreateWriteContext(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto transientWriteState = GetOrCreateTransactionTransientWriteState(transaction->GetId());
        return TWriteContext{
            .Transaction = transaction,
            .PrelockedRows = &transientWriteState->PrelockedRows,
            .LockedRows = &transientWriteState->LockedRows,
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletWriteManagerPtr CreateTabletWriteManager(
    TTablet* tablet,
    ITabletContext* tabletContext)
{
    return New<TTabletWriteManager>(tablet, tabletContext);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
