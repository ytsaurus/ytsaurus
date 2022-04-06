#include "tablet_cell_write_manager.h"

#include "automaton.h"
#include "tablet.h"
#include "sorted_store_manager.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "serialize.h"
#include "sorted_dynamic_store.h"
#include "store_manager.h"

#include <yt/yt/server/lib/hydra_common/automaton.h>
#include <yt/yt/server/lib/hydra_common/mutation.h>
#include <yt/yt/server/lib/hydra_common/hydra_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>
#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/compact_flat_map.h>

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

using namespace NChaosClient;
using namespace NClusterNode;
using namespace NCompression;
using namespace NHydra;
using namespace NLogging;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTransactionClient;

class TTabletCellWriteManager
    : public ITabletCellWriteManager
    , public TTabletAutomatonPart
{
public:
    TTabletCellWriteManager(
        ITabletCellWriteManagerHostPtr host,
        ISimpleHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        TMemoryUsageTrackerGuard&& writeLogsMemoryTrackerGuard,
        IInvokerPtr automatonInvoker)
        : TTabletAutomatonPart(
            host->GetCellId(),
            std::move(hydraManager),
            std::move(automaton),
            std::move(automatonInvoker))
        , Host_(std::move(host))
        , ChangelogCodec_(GetCodec(Host_->GetConfig()->ChangelogCodec))
        , WriteLogsMemoryTrackerGuard_(std::move(writeLogsMemoryTrackerGuard))
    {
        RegisterMethod(BIND(&TTabletCellWriteManager::HydraFollowerWriteRows, Unretained(this)));
    }

    // ITabletCellWriteManager overrides.

    void Initialize() override
    {
        const auto& transactionManager = Host_->GetTransactionManager();
        transactionManager->SubscribeTransactionPrepared(BIND(&TTabletCellWriteManager::OnTransactionPrepared, MakeWeak(this)));
        transactionManager->SubscribeTransactionCommitted(BIND(&TTabletCellWriteManager::OnTransactionCommitted, MakeWeak(this)));
        transactionManager->SubscribeTransactionSerialized(BIND(&TTabletCellWriteManager::OnTransactionSerialized, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TTabletCellWriteManager::OnTransactionAborted, MakeWeak(this)));
        transactionManager->SubscribeTransactionTransientReset(BIND(&TTabletCellWriteManager::OnTransactionTransientReset, MakeWeak(this)));
    }

    void Write(
        const TTabletSnapshotPtr& tabletSnapshot,
        TTransactionId transactionId,
        TTimestamp transactionStartTimestamp,
        TDuration transactionTimeout,
        TTransactionSignature signature,
        TTransactionGeneration generation,
        int rowCount,
        size_t dataWeight,
        bool versioned,
        const TSyncReplicaIdList& syncReplicaIds,
        TWireProtocolReader* reader,
        TFuture<void>* commitResult) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& identity = NRpc::GetCurrentAuthenticationIdentity();
        bool replicatorWrite = IsReplicatorWrite(identity);

        TTablet* tablet = nullptr;
        const auto& transactionManager = Host_->GetTransactionManager();

        auto atomicity = AtomicityFromTransactionId(transactionId);
        if (atomicity == EAtomicity::None) {
            ValidateClientTimestamp(transactionId);
        }

        if (generation > InitialTransactionGeneration) {
            if (versioned) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::WriteRetryIsImpossible,
                    "Retrying versioned writes is not supported");
            }
            if (replicatorWrite) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::WriteRetryIsImpossible,
                    "Retrying replicator writes is not supported");
            }
            if (atomicity == EAtomicity::None) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::WriteRetryIsImpossible,
                    "Retrying non-atomic writes is not supported");
            }
        }

        tabletSnapshot->TabletRuntimeData->ModificationTime = NProfiling::GetInstant();

        auto actualizeTablet = [&] {
            if (!tablet) {
                tablet = Host_->GetTabletOrThrow(tabletSnapshot->TabletId);
                tablet->ValidateMountRevision(tabletSnapshot->MountRevision);
                ValidateTabletMounted(tablet);
            }
        };

        actualizeTablet();

        if (atomicity == EAtomicity::Full) {
            const auto& lockManager = tablet->GetLockManager();
            auto error = lockManager->ValidateTransactionConflict(transactionStartTimestamp);
            if (!error.IsOK()) {
                THROW_ERROR error
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("transaction_id", transactionId);
            }
        }

        while (!reader->IsFinished()) {
            // NB: No yielding beyond this point.
            // May access tablet and transaction.

            actualizeTablet();

            ValidateTabletStoreLimit(tablet);

            auto poolTag = Host_->GetDynamicOptions()->EnableTabletDynamicMemoryLimit
                ? tablet->GetPoolTagByMemoryCategory(EMemoryCategory::TabletDynamic)
                : std::nullopt;
            Host_->ValidateMemoryLimit(poolTag);
            ValidateWriteBarrier(replicatorWrite, tablet);

            auto tabletId = tablet->GetId();
            const auto& storeManager = tablet->GetStoreManager();

            TTransaction* transaction = nullptr;
            bool transactionIsFresh = false;
            bool updateReplicationProgress = false;
            if (atomicity == EAtomicity::Full) {
                transaction = transactionManager->GetOrCreateTransaction(
                    transactionId,
                    transactionStartTimestamp,
                    transactionTimeout,
                    true,
                    &transactionIsFresh);
                ValidateTransactionActive(transaction);

                if (generation > transaction->GetTransientGeneration()) {
                    // Promote transaction transient generation and clear the transaction transient state.
                    // In particular, we abort all rows that were prelocked or locked by the previous batches of our generation,
                    // but that is perfectly fine.
                    PromoteTransientGeneration(transaction, generation);
                } else if (generation < transaction->GetTransientGeneration()) {
                    // We may get here in two sitations. The first one is when Write RPC call was late to arrive,
                    // while the second one is trickier. It happens in the case when next generation arrived while our
                    // fiber was waiting on the blocked row. In both cases we are not going to enqueue any more mutations
                    // in order to ensure monotonicity of mutation generations which is an important invariant.
                    YT_LOG_DEBUG(
                        "Stopping obsolete generation write (TabletId: %v, TransactionId: %v, Generation: %x, TransientGeneration: %x)",
                        tabletId,
                        transactionId,
                        generation,
                        transaction->GetTransientGeneration());
                    // Client already decided to go on with the next generation of rows, so we are ok to even ignore
                    // possible commit errors. Note that the result of this particular write does not affect the outcome of the
                    // transaction any more, so we are safe to lose some of freshly enqueued mutations.
                    *commitResult = VoidFuture;
                    return;
                }

                updateReplicationProgress = tablet->GetReplicationCardId() && !versioned;
            }

            TWriteContext context;
            context.Phase = EWritePhase::Prelock;
            context.Transaction = transaction;

            auto readerBefore = reader->GetCurrent();
            auto adjustedSignature = signature;
            auto lockless =
                atomicity == EAtomicity::None ||
                tablet->IsPhysicallyOrdered() ||
                tablet->IsPhysicallyLog() ||
                versioned;
            if (lockless) {
                // Skip the whole message.
                reader->SetCurrent(reader->GetEnd());
                context.RowCount = rowCount;
                context.DataWeight = dataWeight;
            } else {
                storeManager->ExecuteWrites(reader, &context);
                if (!reader->IsFinished()) {
                    adjustedSignature = 0;
                }
                YT_LOG_DEBUG_IF(context.RowCount > 0, "Rows prelocked (TransactionId: %v, TabletId: %v, RowCount: %v, Generation: %x, Signature: %x)",
                    transactionId,
                    tabletId,
                    context.RowCount,
                    generation,
                    adjustedSignature);
            }
            auto readerAfter = reader->GetCurrent();

            if (atomicity == EAtomicity::Full) {
                transaction->SetTransientSignature(transaction->GetTransientSignature() + adjustedSignature);
            }

            if (readerBefore != readerAfter) {
                auto recordData = reader->Slice(readerBefore, readerAfter);
                auto compressedRecordData = ChangelogCodec_->Compress(recordData);
                TTransactionWriteRecord writeRecord(tabletId, recordData, context.RowCount, context.DataWeight, syncReplicaIds);

                PrelockedTablets_.push(tablet);
                LockTablet(tablet);

                IncrementTabletInFlightMutationCount(tablet, replicatorWrite, +1);

                TReqWriteRows hydraRequest;
                ToProto(hydraRequest.mutable_transaction_id(), transactionId);
                hydraRequest.set_transaction_start_timestamp(transactionStartTimestamp);
                hydraRequest.set_transaction_timeout(ToProto<i64>(transactionTimeout));
                ToProto(hydraRequest.mutable_tablet_id(), tabletId);
                hydraRequest.set_mount_revision(tablet->GetMountRevision());
                hydraRequest.set_codec(static_cast<int>(ChangelogCodec_->GetId()));
                hydraRequest.set_compressed_data(ToString(compressedRecordData));
                hydraRequest.set_signature(adjustedSignature);
                hydraRequest.set_generation(generation);
                hydraRequest.set_lockless(lockless);
                hydraRequest.set_row_count(writeRecord.RowCount);
                hydraRequest.set_data_weight(writeRecord.DataWeight);
                hydraRequest.set_update_replication_progress(updateReplicationProgress);
                ToProto(hydraRequest.mutable_sync_replica_ids(), syncReplicaIds);
                NRpc::WriteAuthenticationIdentityToProto(&hydraRequest, identity);

                auto mutation = CreateMutation(HydraManager_, hydraRequest);
                mutation->SetHandler(BIND_DONT_CAPTURE_TRACE_CONTEXT(
                    &TTabletCellWriteManager::HydraLeaderWriteRows,
                    MakeStrong(this),
                    transactionId,
                    tablet->GetMountRevision(),
                    adjustedSignature,
                    generation,
                    lockless,
                    writeRecord,
                    identity,
                    updateReplicationProgress));
                mutation->SetCurrentTraceContext();
                *commitResult = mutation->Commit().As<void>();

                auto counters = tablet->GetTableProfiler()->GetWriteCounters(GetCurrentProfilingUser());
                counters->RowCount.Increment(writeRecord.RowCount);
                counters->DataWeight.Increment(writeRecord.DataWeight);
            } else if (transactionIsFresh) {
                transactionManager->DropTransaction(transaction);
            }

            // NB: Yielding is now possible.
            // Cannot neither access tablet, nor transaction.
            if (context.BlockedStore) {
                context.BlockedStore->WaitOnBlockedRow(
                    context.BlockedRow,
                    context.BlockedLockMask,
                    context.BlockedTimestamp);
                tablet = nullptr;
            }

            context.Error.ThrowOnError();
        }
    }

    // TTabletAutomatonPart overrides.

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::OnStopLeading();

        while (!PrelockedTablets_.empty()) {
            auto* tablet = PrelockedTablets_.front();
            PrelockedTablets_.pop();
            UnlockTablet(tablet);
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::Clear();

        WriteLogsMemoryTrackerGuard_.SetSize(0);
    }

    void OnAfterSnapshotLoaded() noexcept override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& transactionManager = Host_->GetTransactionManager();
        auto transactions = transactionManager->GetTransactions();
        for (auto* transaction : transactions) {
            YT_VERIFY(!transaction->GetTransient());

            bool replicatorWrite = IsReplicatorWrite(transaction);

            for (const auto& record : transaction->ImmediateLockedWriteLog()) {
                auto* tablet = Host_->FindTablet(record.TabletId);
                if (!tablet) {
                    // NB: Tablet could be missing if it was, e.g., forcefully removed.
                    continue;
                }

                WriteLogsMemoryTrackerGuard_.IncrementSize(record.GetByteSize());
                IncrementTabletPendingWriteRecordCount(tablet, replicatorWrite, +1);

                TWireProtocolReader reader(record.Data);
                const auto& storeManager = tablet->GetStoreManager();

                TWriteContext context;
                context.Phase = EWritePhase::Lock;
                context.Transaction = transaction;
                YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));
            }

            for (const auto& record : transaction->ImmediateLocklessWriteLog()) {
                auto* tablet = Host_->FindTablet(record.TabletId);
                if (!tablet) {
                    // NB: Tablet could be missing if it was, e.g., forcefully removed.
                    continue;
                }

                WriteLogsMemoryTrackerGuard_.IncrementSize(record.GetByteSize());
                IncrementTabletPendingWriteRecordCount(tablet, replicatorWrite, +1);

                LockTablet(tablet);
                transaction->LockedTablets().push_back(tablet);
            }

            for (const auto& record : transaction->DelayedLocklessWriteLog()) {
                auto* tablet = Host_->FindTablet(record.TabletId);
                if (!tablet) {
                    // NB: Tablet could be missing if it was, e.g., forcefully removed.
                    continue;
                }

                WriteLogsMemoryTrackerGuard_.IncrementSize(record.GetByteSize());
                IncrementTabletPendingWriteRecordCount(tablet, replicatorWrite, +1);

                LockTablet(tablet);
                transaction->LockedTablets().push_back(tablet);

                if (tablet->IsPhysicallyLog() && transaction->GetRowsPrepared()) {
                    tablet->SetDelayedLocklessRowCount(tablet->GetDelayedLocklessRowCount() + record.RowCount);
                }
            }

            if (transaction->GetPersistentState() == ETransactionState::PersistentCommitPrepared) {
                PrepareLockedRows(transaction);
            }
        }

        for (const auto& [tabletId, tablet] : Host_->Tablets()) {
            tablet->RecomputeReplicaStatuses();
            tablet->RecomputeCommittedReplicationRowIndices();
        }
    }

private:
    const ITabletCellWriteManagerHostPtr Host_;
    ICodec* const ChangelogCodec_;

    TRingQueue<TTablet*> PrelockedTablets_;

    // NB: Write logs are generally much smaller than dynamic stores,
    // so we don't worry about per-pool management here.
    TMemoryUsageTrackerGuard WriteLogsMemoryTrackerGuard_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void HydraLeaderWriteRows(
        TTransactionId transactionId,
        NHydra::TRevision mountRevision,
        TTransactionSignature signature,
        TTransactionGeneration generation,
        bool lockless,
        const TTransactionWriteRecord& writeRecord,
        const NRpc::TAuthenticationIdentity& identity,
        bool updateReplicationProgress,
        TMutationContext* /*context*/) noexcept
    {
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);
        bool replicatorWrite = IsReplicatorWrite(identity);

        auto atomicity = AtomicityFromTransactionId(transactionId);

        auto* tablet = PrelockedTablets_.front();
        PrelockedTablets_.pop();
        YT_VERIFY(tablet->GetId() == writeRecord.TabletId);
        auto finallyGuard = Finally([&] {
            UnlockTablet(tablet);
        });

        IncrementTabletInFlightMutationCount(tablet, replicatorWrite, -1);

        if (mountRevision != tablet->GetMountRevision()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Mount revision mismatch; write ignored "
                "(%v, TransactionId: %v, MutationMountRevision: %llx, CurrentMountRevision: %llx)",
                tablet->GetLoggingTag(),
                transactionId,
                mountRevision,
                tablet->GetMountRevision());
            return;
        }

        TTransaction* transaction = nullptr;
        switch (atomicity) {
            case EAtomicity::Full: {
                const auto& transactionManager = Host_->GetTransactionManager();
                transaction = transactionManager->MakeTransactionPersistent(transactionId);

                YT_LOG_DEBUG_IF(
                    IsMutationLoggingEnabled(),
                    "Performing atomic write as leader (TabletId: %v, TransactionId: %v, BatchGeneration: %x, "
                    "TransientGeneration: %x, PersistentGeneration: %x)",
                    writeRecord.TabletId,
                    transactionId,
                    generation,
                    transaction->GetTransientGeneration(),
                    transaction->GetPersistentGeneration());

                // Monotonicity of persistent generations is ensured by the early finish in #Write whenever the
                // current batch is obsolete.
                YT_VERIFY(generation >= transaction->GetPersistentGeneration());
                YT_VERIFY(generation <= transaction->GetTransientGeneration());
                if (generation > transaction->GetPersistentGeneration()) {
                    // Promote persistent generation and also clear current persistent transaction state (i.e. write logs).
                    PromotePersistentGeneration(transaction, generation);
                }

                // Note that the scope below affects only the transient state.
                // As a consequence, if the transient generation was promoted ahead of us, we should not do
                // anything here.
                if (generation == transaction->GetTransientGeneration()) {
                    if (lockless) {
                        transaction->LockedTablets().push_back(tablet);
                        LockTablet(tablet);

                        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Prelocked tablet confirmed (TabletId: %v, TransactionId: %v, "
                            "RowCount: %v, LockCount: %v)",
                            writeRecord.TabletId,
                            transactionId,
                            writeRecord.RowCount,
                            tablet->GetTabletLockCount());
                    } else {
                        auto& prelockedRows = transaction->PrelockedRows();
                        for (int index = 0; index < writeRecord.RowCount; ++index) {
                            YT_ASSERT(!prelockedRows.empty());
                            auto rowRef = prelockedRows.front();
                            prelockedRows.pop();
                            if (Host_->ValidateAndDiscardRowRef(rowRef)) {
                                rowRef.StoreManager->ConfirmRow(transaction, rowRef);
                            }
                        }

                        YT_LOG_DEBUG("Prelocked rows confirmed (TabletId: %v, TransactionId: %v, RowCount: %v)",
                            writeRecord.TabletId,
                            transactionId,
                            writeRecord.RowCount);
                    }
                }

                // Scope below actually affects the persistent state, so it should be executed in any case,
                // even if the generation of a batch is behind the current transient generation.
                {
                    bool immediate = tablet->GetCommitOrdering() == ECommitOrdering::Weak;
                    auto* writeLog = immediate
                        ? (lockless ? &transaction->ImmediateLocklessWriteLog() : &transaction->ImmediateLockedWriteLog())
                        : &transaction->DelayedLocklessWriteLog();
                    EnqueueTransactionWriteRecord(transaction, tablet, writeLog, writeRecord, signature);

                    YT_LOG_DEBUG_UNLESS(writeLog == &transaction->ImmediateLockedWriteLog(),
                        "Rows batched (TabletId: %v, TransactionId: %v, WriteRecordSize: %v, RowCount: %v, Generation: %x, Immediate: %v, Lockless: %v)",
                        writeRecord.TabletId,
                        transactionId,
                        writeRecord.GetByteSize(),
                        writeRecord.RowCount,
                        generation,
                        immediate,
                        lockless);
                }

                if (updateReplicationProgress) {
                    // Update replication progress for queue replicas so async replicas can pull from them as fast as possible.
                    // NB: This replication progress update is a best effort and does not require tablet locking.
                    transaction->TabletsToUpdateReplicationProgress().insert(tablet->GetId());
                }

                break;
            }

            case EAtomicity::None: {
                // This is ensured by a corresponding check in #Write.
                YT_VERIFY(generation == InitialTransactionGeneration);

                if (tablet->GetState() == ETabletState::Orphaned) {
                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet is orphaned; non-atomic write ignored "
                        "(%v, TransactionId: %v)",
                        tablet->GetLoggingTag(),
                        transactionId);
                    return;
                }

                TWireProtocolReader reader(writeRecord.Data);
                TWriteContext context;
                context.Phase = EWritePhase::Commit;
                context.CommitTimestamp = TimestampFromTransactionId(transactionId);
                const auto& storeManager = tablet->GetStoreManager();
                YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));
                YT_VERIFY(writeRecord.RowCount == context.RowCount);

                auto counters = tablet->GetTableProfiler()->GetCommitCounters(GetCurrentProfilingUser());
                counters->RowCount.Increment(writeRecord.RowCount);
                counters->DataWeight.Increment(writeRecord.DataWeight);

                FinishTabletCommit(tablet, nullptr, context.CommitTimestamp);

                YT_LOG_DEBUG("Non-atomic rows committed (TransactionId: %v, TabletId: %v, "
                    "RowCount: %v, WriteRecordSize: %v, ActualTimestamp: %llx)",
                    transactionId,
                    writeRecord.TabletId,
                    writeRecord.RowCount,
                    writeRecord.Data.Size(),
                    context.CommitTimestamp);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    void HydraFollowerWriteRows(TReqWriteRows* request) noexcept
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto atomicity = AtomicityFromTransactionId(transactionId);
        auto transactionStartTimestamp = request->transaction_start_timestamp();
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());
        auto signature = request->signature();
        auto generation = request->generation();
        auto lockless = request->lockless();
        auto rowCount = request->row_count();
        auto dataWeight = request->data_weight();
        auto syncReplicaIds = FromProto<TSyncReplicaIdList>(request->sync_replica_ids());
        auto updateReplicationProgress = request->update_replication_progress();

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = Host_->FindTablet(tabletId);
        if (!tablet) {
            // NB: Tablet could be missing if it was, e.g., forcefully removed.
            return;
        }

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            // Same as above.
            return;
        }

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto codecId = FromProto<ECodec>(request->codec());
        auto* codec = GetCodec(codecId);
        auto compressedRecordData = TSharedRef::FromString(request->compressed_data());
        auto recordData = codec->Decompress(compressedRecordData);
        TTransactionWriteRecord writeRecord(tabletId, recordData, rowCount, dataWeight, syncReplicaIds);
        TWireProtocolReader reader(recordData);

        const auto& storeManager = tablet->GetStoreManager();

        switch (atomicity) {
            case EAtomicity::Full: {
                const auto& transactionManager = Host_->GetTransactionManager();
                auto* transaction = transactionManager->GetOrCreateTransaction(
                    transactionId,
                    transactionStartTimestamp,
                    transactionTimeout,
                    false);

                YT_LOG_DEBUG_IF(
                    IsMutationLoggingEnabled(),
                    "Performing atomic write as follower (TabletId: %v, TransactionId: %v, BatchGeneration: %x, PersistentGeneration: %x)",
                    tabletId,
                    transactionId,
                    generation,
                    transaction->GetPersistentGeneration());

                // This invariant holds during recovery.
                YT_VERIFY(transaction->GetPersistentGeneration() == transaction->GetTransientGeneration());
                // Monotonicity of persistent generations is ensured by the early finish in #Write whenever the
                // current batch is obsolete.
                YT_VERIFY(transaction->GetPersistentGeneration() <= generation);
                if (generation > transaction->GetPersistentGeneration()) {
                    // While in recovery, we are responsible for keeping both transient and persistent state up-to-date.
                    // Hence, generation promotion must be handles as a combination of transient and persistent generation promotions
                    // from the regular leader case.
                    PromoteTransientGeneration(transaction, generation);
                    PromotePersistentGeneration(transaction, generation);
                }

                bool immediate = tablet->GetCommitOrdering() == ECommitOrdering::Weak;
                auto* writeLog = immediate
                    ? (lockless ? &transaction->ImmediateLocklessWriteLog() : &transaction->ImmediateLockedWriteLog())
                    : &transaction->DelayedLocklessWriteLog();
                EnqueueTransactionWriteRecord(transaction, tablet, writeLog, writeRecord, signature);

                if (immediate && !lockless) {
                    TWriteContext context;
                    context.Phase = EWritePhase::Lock;
                    context.Transaction = transaction;
                    YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Rows locked (TransactionId: %v, TabletId: %v, RowCount: %v, "
                        "WriteRecordSize: %v, Signature: %x)",
                        transactionId,
                        tabletId,
                        context.RowCount,
                        writeRecord.GetByteSize(),
                        signature);
                } else {
                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Rows batched (TransactionId: %v, TabletId: %v, "
                        "WriteRecordSize: %v, Signature: %x)",
                        transactionId,
                        tabletId,
                        writeRecord.GetByteSize(),
                        signature);

                    transaction->LockedTablets().push_back(tablet);
                    auto lockCount = LockTablet(tablet);

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet locked (TabletId: %v, TransactionId: %v, LockCount: %v)",
                        writeRecord.TabletId,
                        transactionId,
                        lockCount);
                }

                if (updateReplicationProgress) {
                    // Update replication progress for queue replicas so async replicas can pull from them as fast as possible.
                    // NB: This replication progress update is a best effort and does not require tablet locking.
                    transaction->TabletsToUpdateReplicationProgress().insert(tablet->GetId());
                }

                break;
            }


            case EAtomicity::None: {
                // This is ensured by a corresponding check in #Write.
                YT_VERIFY(generation == InitialTransactionGeneration);

                TWriteContext context;
                context.Phase = EWritePhase::Commit;
                context.CommitTimestamp = TimestampFromTransactionId(transactionId);

                YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));

                FinishTabletCommit(tablet, nullptr, context.CommitTimestamp);

                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Non-atomic rows committed (TransactionId: %v, TabletId: %v, "
                    "RowCount: %v, WriteRecordSize: %v, Signature: %x)",
                    transactionId,
                    tabletId,
                    context.RowCount,
                    writeRecord.GetByteSize(),
                    signature);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    void ValidateReplicaStatus(ETableReplicaStatus expected, TTablet* tablet, const TTableReplicaInfo& replicaInfo) const
    {
        YT_LOG_ALERT_IF(
            IsMutationLoggingEnabled() && replicaInfo.GetStatus() != expected,
            "Table replica status mismatch "
            "(Expected: %v, Actual: %v, CurrentReplicationRowIndex: %v, TotalRowCount: %v, DelayedLocklessRowCount: %v, Mode: %v)",
            expected,
            replicaInfo.GetStatus(),
            replicaInfo.GetCurrentReplicationRowIndex(),
            tablet->GetTotalRowCount(),
            tablet->GetDelayedLocklessRowCount(),
            replicaInfo.GetMode());
    }

    void ValidateReplicaWritable(TTablet* tablet, const TTableReplicaInfo& replicaInfo)
    {
        auto currentReplicationRowIndex = replicaInfo.GetCurrentReplicationRowIndex();
        auto totalRowCount = tablet->GetTotalRowCount();
        auto delayedLocklessRowCount = tablet->GetDelayedLocklessRowCount();
        switch (replicaInfo.GetMode()) {
            case ETableReplicaMode::Sync: {
                if (currentReplicationRowIndex < totalRowCount + delayedLocklessRowCount) {
                    if (replicaInfo.GetState() == ETableReplicaState::Enabled) {
                        ValidateReplicaStatus(ETableReplicaStatus::SyncCatchingUp, tablet, replicaInfo);
                    } else {
                        ValidateReplicaStatus(ETableReplicaStatus::SyncNotWritable, tablet, replicaInfo);
                    }
                    THROW_ERROR_EXCEPTION(
                        "Replica %v of tablet %v is not synchronously writeable since some rows are not replicated yet",
                        replicaInfo.GetId(),
                        tablet->GetId())
                        << TErrorAttribute("current_replication_row_index", currentReplicationRowIndex)
                        << TErrorAttribute("total_row_count", totalRowCount)
                        << TErrorAttribute("delayed_lockless_row_count", delayedLocklessRowCount);
                }
                if (currentReplicationRowIndex > totalRowCount + delayedLocklessRowCount) {
                    YT_LOG_ALERT_IF(
                        IsMutationLoggingEnabled(),
                        "Current replication row index is too high (TabletId: %v, ReplicaId: %v, "
                        "CurrentReplicationRowIndex: %v, TotalRowCount: %v, DelayedLocklessRowCount: %v)",
                        tablet->GetId(),
                        replicaInfo.GetId(),
                        currentReplicationRowIndex,
                        totalRowCount,
                        delayedLocklessRowCount);
                }
                if (replicaInfo.GetState() != ETableReplicaState::Enabled) {
                    ValidateReplicaStatus(ETableReplicaStatus::SyncNotWritable, tablet, replicaInfo);
                    THROW_ERROR_EXCEPTION(
                        "Replica %v is not synchronously writeable since it is in %Qlv state",
                         replicaInfo.GetId(),
                         replicaInfo.GetState());
                }
                ValidateReplicaStatus(ETableReplicaStatus::SyncInSync, tablet, replicaInfo);
                YT_VERIFY(!replicaInfo.GetPreparedReplicationTransactionId());
                break;
            }

            case ETableReplicaMode::Async:
                if (currentReplicationRowIndex > totalRowCount) {
                    ValidateReplicaStatus(ETableReplicaStatus::AsyncNotWritable, tablet, replicaInfo);
                    THROW_ERROR_EXCEPTION(
                        "Replica %v of tablet %v is not asynchronously writeable: some synchronous writes are still in progress",
                        replicaInfo.GetId(),
                        tablet->GetId())
                        << TErrorAttribute("current_replication_row_index", currentReplicationRowIndex)
                        << TErrorAttribute("total_row_count", totalRowCount);
                }

                if (currentReplicationRowIndex >= totalRowCount + delayedLocklessRowCount) {
                    ValidateReplicaStatus(ETableReplicaStatus::AsyncInSync, tablet, replicaInfo);
                } else {
                    ValidateReplicaStatus(ETableReplicaStatus::AsyncCatchingUp, tablet, replicaInfo);
                }

                break;

            default:
                YT_ABORT();
        }
    }

    static void ValidateSyncReplicaSet(TTablet* tablet, const TSyncReplicaIdList& syncReplicaIds)
    {
        for (auto replicaId : syncReplicaIds) {
            const auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
            if (!replicaInfo) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::SyncReplicaIsNotKnown,
                    "Synchronous replica %v is not known for tablet %v",
                    replicaId,
                    tablet->GetId());
            }
            if (replicaInfo->GetMode() != ETableReplicaMode::Sync) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::SyncReplicaIsNotInSyncMode,
                    "Replica %v of tablet %v is not in sync mode",
                    replicaId,
                    tablet->GetId());
            }
        }

        for (const auto& [replicaId, replicaInfo] : tablet->Replicas()) {
            if (replicaInfo.GetMode() == ETableReplicaMode::Sync) {
                if (std::find(syncReplicaIds.begin(), syncReplicaIds.end(), replicaId) == syncReplicaIds.end()) {
                    THROW_ERROR_EXCEPTION(
                        NTabletClient::EErrorCode::SyncReplicaIsNotWritten,
                        "Synchronous replica %v of tablet %v is not being written by client",
                        replicaId,
                        tablet->GetId());
                }
            }
        }
    }

    void OnTransactionPrepared(TTransaction* transaction, bool persistent)
    {
        PrepareLockedRows(transaction);

        // The rest only makes sense for persistent prepare.
        // In particular, all writes to replicated tables currently involve 2PC.
        if (!persistent) {
            return;
        }

        TCompactFlatMap<TTableReplicaInfo*, int, 8> replicaToRowCount;
        TCompactFlatMap<TTablet*, int, 8> tabletToRowCount;
        for (const auto& writeRecord : transaction->DelayedLocklessWriteLog()) {
            auto* tablet = Host_->GetTabletOrThrow(writeRecord.TabletId);

            if (!tablet->IsPhysicallyLog()) {
                continue;
            }

            // TODO(ifsmirnov): No bulk insert into replicated tables. Remove this check?
            const auto& lockManager = tablet->GetLockManager();
            if (auto error = lockManager->ValidateTransactionConflict(transaction->GetStartTimestamp());
                !error.IsOK())
            {
                THROW_ERROR error << TErrorAttribute("tablet_id", tablet->GetId());
            }

            ValidateSyncReplicaSet(tablet, writeRecord.SyncReplicaIds);
            for (auto& [replicaId, replicaInfo] : tablet->Replicas()) {
                ValidateReplicaWritable(tablet, replicaInfo);
                if (replicaInfo.GetMode() == ETableReplicaMode::Sync) {
                    replicaToRowCount[&replicaInfo] += writeRecord.RowCount;
                }
            }

            tabletToRowCount[tablet] += writeRecord.RowCount;
        }

        for (auto [replicaInfo, rowCount] : replicaToRowCount) {
            const auto* tablet = replicaInfo->GetTablet();
            auto oldCurrentReplicationRowIndex = replicaInfo->GetCurrentReplicationRowIndex();
            auto newCurrentReplicationRowIndex = oldCurrentReplicationRowIndex + rowCount;
            replicaInfo->SetCurrentReplicationRowIndex(newCurrentReplicationRowIndex);
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Sync replicated rows prepared (TransactionId: %v, TabletId: %v, ReplicaId: %v, CurrentReplicationRowIndex: %v -> %v, "
                "TotalRowCount: %v)",
                transaction->GetId(),
                tablet->GetId(),
                replicaInfo->GetId(),
                oldCurrentReplicationRowIndex,
                newCurrentReplicationRowIndex,
                tablet->GetTotalRowCount());
        }

        for (auto [tablet, rowCount] : tabletToRowCount) {
            auto oldDelayedLocklessRowCount = tablet->GetDelayedLocklessRowCount();
            auto newDelayedLocklessRowCount = oldDelayedLocklessRowCount + rowCount;
            tablet->SetDelayedLocklessRowCount(newDelayedLocklessRowCount);
            tablet->RecomputeReplicaStatuses();
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Delayed lockless rows prepared (TransactionId: %v, TabletId: %v, DelayedLocklessRowCount: %v -> %v)",
                transaction->GetId(),
                tablet->GetId(),
                oldDelayedLocklessRowCount,
                newDelayedLocklessRowCount);
        }

        YT_VERIFY(!transaction->GetRowsPrepared());
        transaction->SetRowsPrepared(true);
    }

    void OnTransactionCommitted(TTransaction* transaction) noexcept
    {
        auto commitTimestamp = transaction->GetCommitTimestamp();

        YT_VERIFY(transaction->PrelockedRows().empty());
        auto& lockedRows = transaction->LockedRows();
        int lockedRowCount = 0;
        for (const auto& rowRef : lockedRows) {
            if (!Host_->ValidateAndDiscardRowRef(rowRef)) {
                continue;
            }

            ++lockedRowCount;
            FinishTabletCommit(rowRef.Store->GetTablet(), transaction, commitTimestamp);
            auto* tablet = rowRef.StoreManager->GetTablet();
            rowRef.StoreManager->CommitRow(transaction, rowRef);
            Host_->OnTabletRowUnlocked(tablet);
        }
        lockedRows.clear();

        int locklessRowCount = 0;
        TCompactVector<TTablet*, 16> locklessTablets;
        for (const auto& record : transaction->ImmediateLocklessWriteLog()) {
            auto* tablet = Host_->FindTablet(record.TabletId);
            if (!tablet) {
                continue;
            }

            locklessTablets.push_back(tablet);

            TWriteContext context;
            context.Phase = EWritePhase::Commit;
            context.Transaction = transaction;
            context.CommitTimestamp = commitTimestamp;

            TWireProtocolReader reader(record.Data);

            const auto& storeManager = tablet->GetStoreManager();
            YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));
            YT_VERIFY(context.RowCount == record.RowCount);

            locklessRowCount += context.RowCount;
        }

        for (auto* tablet : locklessTablets) {
            FinishTabletCommit(tablet, transaction, commitTimestamp);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled() && (lockedRowCount + locklessRowCount > 0),
            "Immediate rows committed (TransactionId: %v, LockedRowCount: %v, LocklessRowCount: %v)",
            transaction->GetId(),
            lockedRowCount,
            locklessRowCount);

        TCompactVector<TTableReplicaInfo*, 16> syncReplicas;
        TCompactVector<TTablet*, 16> syncReplicaTablets;
        for (const auto& writeRecord : transaction->DelayedLocklessWriteLog()) {
            auto* tablet = Host_->FindTablet(writeRecord.TabletId);
            if (!tablet) {
                continue;
            }

            tablet->UpdateLastWriteTimestamp(commitTimestamp);

            if (!writeRecord.SyncReplicaIds.empty()) {
                syncReplicaTablets.push_back(tablet);
            }

            for (auto replicaId : writeRecord.SyncReplicaIds) {
                auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
                if (!replicaInfo) {
                    continue;
                }

                syncReplicas.push_back(replicaInfo);
            }
        }

        SortUnique(syncReplicas);
        for (auto* replicaInfo : syncReplicas) {
            const auto* tablet = replicaInfo->GetTablet();
            auto oldCurrentReplicationTimestamp = replicaInfo->GetCurrentReplicationTimestamp();
            auto newCurrentReplicationTimestamp = std::max(oldCurrentReplicationTimestamp, commitTimestamp);
            replicaInfo->SetCurrentReplicationTimestamp(newCurrentReplicationTimestamp);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Sync replicated rows committed "
                "(TransactionId: %v, TabletId: %v, ReplicaId: %v, CurrentReplicationTimestamp: %llx -> %llx, TotalRowCount: %v)",
                transaction->GetId(),
                tablet->GetId(),
                replicaInfo->GetId(),
                oldCurrentReplicationTimestamp,
                newCurrentReplicationTimestamp,
                tablet->GetTotalRowCount());
        }

        SortUnique(syncReplicaTablets);
        for (auto* tablet : syncReplicaTablets) {
            Host_->AdvanceReplicatedTrimmedRowCount(tablet, transaction);
        }

        if (transaction->DelayedLocklessWriteLog().Empty()) {
            UnlockLockedTablets(transaction);
        }

        auto updateProfileCounters = [&] (const TTransactionWriteLog& log) {
            for (const auto& record : log) {
                auto* tablet = Host_->FindTablet(record.TabletId);
                if (!tablet) {
                    continue;
                }

                auto counters = tablet->GetTableProfiler()->GetCommitCounters(GetCurrentProfilingUser());
                counters->RowCount.Increment(record.RowCount);
                counters->DataWeight.Increment(record.DataWeight);
            }
        };
        updateProfileCounters(transaction->ImmediateLockedWriteLog());
        updateProfileCounters(transaction->ImmediateLocklessWriteLog());
        updateProfileCounters(transaction->DelayedLocklessWriteLog());

        DropTransactionWriteLog(transaction, &transaction->ImmediateLockedWriteLog());
        DropTransactionWriteLog(transaction, &transaction->ImmediateLocklessWriteLog());
    }

    void OnTransactionSerialized(TTransaction* transaction) noexcept
    {
        YT_VERIFY(transaction->PrelockedRows().empty());
        YT_VERIFY(transaction->LockedRows().empty());

        auto commitTimestamp = transaction->GetCommitTimestamp();

        int rowCount = 0;
        TCompactFlatMap<TTablet*, int, 16> tabletToRowCount;
        TCompactFlatMap<TTableReplicaInfo*, int, 8> replicaToRowCount;
        for (const auto& record : transaction->DelayedLocklessWriteLog()) {
            auto* tablet = Host_->FindTablet(record.TabletId);
            if (!tablet) {
                continue;
            }

            TWriteContext context;
            context.Phase = EWritePhase::Commit;
            context.Transaction = transaction;
            context.CommitTimestamp = commitTimestamp;

            TWireProtocolReader reader(record.Data);

            const auto& storeManager = tablet->GetStoreManager();
            YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));
            YT_VERIFY(context.RowCount == record.RowCount);

            tabletToRowCount[tablet] += record.RowCount;
            rowCount += context.RowCount;

            for (auto replicaId : record.SyncReplicaIds) {
                auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
                if (!replicaInfo) {
                    continue;
                }

                replicaToRowCount[replicaInfo] += record.RowCount;
            }
        }

        for (auto [tablet, rowCount] : tabletToRowCount) {
            FinishTabletCommit(tablet, transaction, commitTimestamp);

            if (!tablet->IsPhysicallyLog()) {
                continue;
            }

            auto oldDelayedLocklessRowCount = tablet->GetDelayedLocklessRowCount();
            auto newDelayedLocklessRowCount = oldDelayedLocklessRowCount - rowCount;
            tablet->SetDelayedLocklessRowCount(newDelayedLocklessRowCount);
            tablet->RecomputeReplicaStatuses();
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Delayed lockless rows committed (TransactionId: %v, TabletId: %v, DelayedLocklessRowCount: %v -> %v, TotalRowCount: %v)",
                transaction->GetId(),
                tablet->GetId(),
                oldDelayedLocklessRowCount,
                newDelayedLocklessRowCount,
                tablet->GetTotalRowCount());
        }

        for (auto [replicaInfo, rowCount] : replicaToRowCount) {
            auto* tablet = replicaInfo->GetTablet();
            auto oldCommittedReplicationRowIndex = replicaInfo->GetCommittedReplicationRowIndex();
            auto newCommittedReplicationRowIndex = oldCommittedReplicationRowIndex + rowCount;
            replicaInfo->SetCommittedReplicationRowIndex(newCommittedReplicationRowIndex);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Delayed lockless rows committed "
                "(TransactionId: %v, TabletId: %v, ReplicaId: %v, CommittedReplicationRowIndex: %v -> %v, TotalRowCount: %v)",
                transaction->GetId(),
                tablet->GetId(),
                replicaInfo->GetId(),
                oldCommittedReplicationRowIndex,
                newCommittedReplicationRowIndex,
                tablet->GetTotalRowCount());
        }

        for (auto tabletId: transaction->TabletsToUpdateReplicationProgress()) {
            auto* tablet = Host_->FindTablet(tabletId);
            if (!tablet) {
                continue;
            }

            YT_VERIFY(transaction->Actions().empty());

            auto progress = tablet->RuntimeData()->ReplicationProgress.Load();
            auto maxTimestamp = GetReplicationProgressMaxTimestamp(*progress);
            if (maxTimestamp >= commitTimestamp) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Tablet replication progress is beyond current serialized transaction commit timestamp "
                    "(TabletId: %v, TransactionId: %v, CommitTimestamp: %llx, MaxReplicationProgressTimestamp: %llx, ReplicatiomProgress: %v)",
                    tabletId,
                    transaction->GetId(),
                    commitTimestamp,
                    maxTimestamp,
                    static_cast<TReplicationProgress>(*progress));
            } else {
                auto newProgress = AdvanceReplicationProgress(*progress, commitTimestamp);
                progress = New<TRefCountedReplicationProgress>(std::move(newProgress));
                tablet->RuntimeData()->ReplicationProgress.Store(progress);

                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Replication progress updated (TabetId: %v, TrnsactionId: %v, ReplicationProgress: %v)",
                    tabletId,
                    transaction->GetId(),
                    static_cast<TReplicationProgress>(*progress));
            }
        }

        if (transaction->DelayedLocklessWriteLog().Empty()) {
            return;
        }

        UnlockLockedTablets(transaction);

        DropTransactionWriteLog(transaction, &transaction->DelayedLocklessWriteLog());
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        YT_VERIFY(HasMutationContext());

        // OnTransactionAborted is always invoked by the transaction supervisor after
        // preparing the transaction abort during which new write mutations are prevented from scheduling.
        // Thus, by this moment there should be no mutations in flight.
        YT_VERIFY(transaction->PrelockedRows().empty());

        AbortLockedRows(transaction);

        auto lockedTabletCount = transaction->LockedTablets().size();
        UnlockLockedTablets(transaction);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled() && lockedTabletCount > 0,
            "Locked tablets unlocked (TransactionId: %v, TabletCount: %v)",
            transaction->GetId(),
            lockedTabletCount);

        if (transaction->GetRowsPrepared()) {
            TCompactFlatMap<TTableReplicaInfo*, int, 8> replicaToRowCount;
            TCompactFlatMap<TTablet*, int, 8> tabletToRowCount;
            for (const auto& writeRecord : transaction->DelayedLocklessWriteLog()) {
                auto* tablet = Host_->FindTablet(writeRecord.TabletId);
                if (!tablet || !tablet->IsPhysicallyLog()) {
                    continue;
                }

                for (auto replicaId : writeRecord.SyncReplicaIds) {
                    auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
                    if (!replicaInfo) {
                        continue;
                    }
                    replicaToRowCount[replicaInfo] += writeRecord.RowCount;
                }

                tabletToRowCount[tablet] += writeRecord.RowCount;
            }

            for (auto [replicaInfo, rowCount] : replicaToRowCount) {
                const auto* tablet = replicaInfo->GetTablet();
                auto oldCurrentReplicationRowIndex = replicaInfo->GetCurrentReplicationRowIndex();
                auto newCurrentReplicationRowIndex = oldCurrentReplicationRowIndex - rowCount;
                replicaInfo->SetCurrentReplicationRowIndex(newCurrentReplicationRowIndex);
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Sync replicated rows aborted (TransactionId: %v, TabletId: %v, ReplicaId: %v, CurrentReplicationRowIndex: %v -> %v, "
                    "TotalRowCount: %v)",
                    transaction->GetId(),
                    tablet->GetId(),
                    replicaInfo->GetId(),
                    oldCurrentReplicationRowIndex,
                    newCurrentReplicationRowIndex,
                    tablet->GetTotalRowCount());
            }

            for (auto [tablet, rowCount] : tabletToRowCount) {
                auto oldDelayedLocklessRowCount = tablet->GetDelayedLocklessRowCount();
                auto newDelayedLocklessRowCount = oldDelayedLocklessRowCount - rowCount;
                tablet->SetDelayedLocklessRowCount(newDelayedLocklessRowCount);
                tablet->RecomputeReplicaStatuses();
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Delayed lockless rows aborted (TransactionId: %v, TabletId: %v, DelayedLocklessRowCount: %v -> %v)",
                    transaction->GetId(),
                    tablet->GetId(),
                    oldDelayedLocklessRowCount,
                    newDelayedLocklessRowCount);
            }
        }

        DropTransactionWriteLogs(transaction);
    }

    //! This method erases write logs properly, also discounting them from the memory tracker
    //! and the versioned/unversioned barrier counters.
    void DropTransactionWriteLogs(TTransaction* transaction)
    {
        YT_VERIFY(HasMutationContext());

        auto immediateLockedRowCount = GetWriteLogRowCount(transaction->ImmediateLockedWriteLog());
        auto immediateLocklessRowCount = GetWriteLogRowCount(transaction->ImmediateLocklessWriteLog());
        auto delayedLocklessRowCount = GetWriteLogRowCount(transaction->DelayedLocklessWriteLog());

        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled() && (immediateLockedRowCount > 0 || immediateLocklessRowCount > 0 || delayedLocklessRowCount > 0),
            "Dropping transaction write logs (ImmediateLockedRowCount: %v, ImmediateLocklessRowCount: %v, DelayedLocklessRowCount: %v)",
            immediateLockedRowCount,
            immediateLocklessRowCount,
            delayedLocklessRowCount);

        DropTransactionWriteLog(transaction, &transaction->ImmediateLockedWriteLog());
        DropTransactionWriteLog(transaction, &transaction->ImmediateLocklessWriteLog());
        DropTransactionWriteLog(transaction, &transaction->DelayedLocklessWriteLog());
    }

    //! This method aborts all rows that are prelocked by the transaction and erases
    //! the (transient) list of prelocked row refs.
    void AbortPrelockedRows(TTransaction* transaction)
    {
        // This method may be called either with or without a mutation context.

        auto prelockedRowCount = transaction->PrelockedRows().size();
        for (const auto& rowRef : TRingQueueIterableWrapper(transaction->PrelockedRows())) {
            if (Host_->ValidateAndDiscardRowRef(rowRef)) {
                auto* tablet = rowRef.StoreManager->GetTablet();
                rowRef.StoreManager->AbortRow(transaction, rowRef);
                Host_->OnTabletRowUnlocked(tablet);
            }
        }

        transaction->PrelockedRows().clear();

        YT_LOG_DEBUG_IF(
            prelockedRowCount != 0,
            "Prelocked rows aborted (TransactionId: %v, RowCount: %v)",
            transaction->GetId(),
            prelockedRowCount);
    }

    //! This method aborts all rows that are locked by the transaction and erases
    //! the (transient) list of locked row refs.
    //! NB: it does not erase ImmediateLockedWriteLog as it is a part of the persistent state.
    void AbortLockedRows(TTransaction* transaction)
    {
        // This method may be called either with or without a mutation context.

        auto lockedRowCount = transaction->LockedRows().size();
        for (const auto& rowRef : transaction->LockedRows()) {
            if (Host_->ValidateAndDiscardRowRef(rowRef)) {
                auto* tablet = rowRef.StoreManager->GetTablet();
                rowRef.StoreManager->AbortRow(transaction, rowRef);
                Host_->OnTabletRowUnlocked(tablet);
            }
        }

        transaction->LockedRows().clear();

        YT_LOG_DEBUG_IF(lockedRowCount > 0,
            "Locked rows aborted (TransactionId: %v, RowCount: %v)",
            transaction->GetId(),
            lockedRowCount);
    }

    //! This method promotes transaction transient generation and also resets its transient state.
    //! In particular, it aborts all row locks in sorted dynamic stores induced by the transaction,
    //! and resets (transient) lists of prelocked and locked row refs.
    void PromoteTransientGeneration(TTransaction* transaction, TTransactionGeneration generation)
    {
        // This method may be called either with or without a mutation context.

        YT_LOG_DEBUG(
            "Promoting transaction transient generation (TransactionId: %v, TransientGeneration: %x -> %x)",
            transaction->GetId(),
            transaction->GetTransientGeneration(),
            generation);

        transaction->SetTransientGeneration(generation);
        transaction->SetTransientSignature(InitialTransactionSignature);

        // We must abort both prelocked and locked rows to prevent new generation of rows from
        // conflicts with earlier generations. Note that locks in the dynamic store are essentially transient.
        AbortPrelockedRows(transaction);
        AbortLockedRows(transaction);

        // NB: it is ok not to unlock prelocked tablets since tablet locking is a lifetime ensurance mechanism
        // in contrast to row prelocking/locking which is a conflict prevention mechanism. Moreover, we do not
        // want the tablet to become fully unlocked while we still have in flight mutations, so it is better not
        // to touch tablet locks here at all.
    }

    //! This method promotes transaction persistent generation and also resets its persistent state by
    //! clearing all associated write logs.
    void PromotePersistentGeneration(TTransaction* transaction, TTransactionGeneration generation)
    {
        YT_VERIFY(HasMutationContext());

        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Promoting transaction persistent generation (TransactionId: %v, PersistentGeneration: %x -> %x)",
            transaction->GetId(),
            transaction->GetPersistentGeneration(),
            generation);

        transaction->SetPersistentGeneration(generation);
        transaction->SetPersistentSignature(InitialTransactionSignature);

        DropTransactionWriteLogs(transaction);
    }

    void OnTransactionTransientReset(TTransaction* transaction)
    {
        AbortPrelockedRows(transaction);
    }

    void FinishTabletCommit(
        TTablet* tablet,
        TTransaction* transaction,
        TTimestamp timestamp)
    {
        if (transaction &&
            !transaction->GetForeign() &&
            transaction->GetPrepareTimestamp() != NullTimestamp &&
            tablet->GetAtomicity() == EAtomicity::Full &&
            HydraManager_->GetAutomatonState() == EPeerState::Leading)
        {
            YT_VERIFY(tablet->GetUnflushedTimestamp() <= timestamp);
        }

        tablet->UpdateLastCommitTimestamp(timestamp);

        if (tablet->IsPhysicallyOrdered()) {
            auto oldTotalRowCount = tablet->GetTotalRowCount();
            tablet->UpdateTotalRowCount();
            auto newTotalRowCount = tablet->GetTotalRowCount();
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled() && oldTotalRowCount != newTotalRowCount,
                "Tablet total row count updated (TabletId: %v, TotalRowCount: %v -> %v)",
                tablet->GetId(),
                oldTotalRowCount,
                newTotalRowCount);
        }
    }

    void EnqueueTransactionWriteRecord(
        TTransaction* transaction,
        TTablet* tablet,
        TTransactionWriteLog* writeLog,
        const TTransactionWriteRecord& record,
        TTransactionSignature signature)
    {
        WriteLogsMemoryTrackerGuard_.IncrementSize(record.GetByteSize());
        writeLog->Enqueue(record);
        transaction->SetPersistentSignature(transaction->GetPersistentSignature() + signature);

        bool replicatorWrite = IsReplicatorWrite(transaction);
        IncrementTabletPendingWriteRecordCount(tablet, replicatorWrite, +1);
    }

    void DropTransactionWriteLog(
        TTransaction* transaction,
        TTransactionWriteLog* writeLog)
    {
        bool replicatorWrite = IsReplicatorWrite(transaction);

        i64 byteSize = 0;
        for (const auto& record : *writeLog) {
            byteSize += record.GetByteSize();

            auto* tablet = Host_->FindTablet(record.TabletId);
            if (!tablet) {
                continue;
            }

            IncrementTabletPendingWriteRecordCount(tablet, replicatorWrite, -1);
        }

        WriteLogsMemoryTrackerGuard_.IncrementSize(-byteSize);
        writeLog->Clear();
    }

    void PrepareLockedRows(TTransaction* transaction)
    {
        auto prepareRow = [&] (const TSortedDynamicRowRef& rowRef) {
            // NB: Don't call ValidateAndDiscardRowRef, row refs are just scanned.
            if (rowRef.Store->GetStoreState() != EStoreState::Orphaned) {
                rowRef.StoreManager->PrepareRow(transaction, rowRef);
            }
        };

        auto lockedRowCount = transaction->LockedRows().size();
        auto prelockedRowCount = transaction->PrelockedRows().size();

        for (const auto& rowRef : transaction->LockedRows()) {
            prepareRow(rowRef);
        }

        for (auto it = transaction->PrelockedRows().begin();
        it != transaction->PrelockedRows().end();
        transaction->PrelockedRows().move_forward(it))
        {
            prepareRow(*it);
        }

        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled() && (lockedRowCount + prelockedRowCount > 0),
            "Locked rows prepared (TransactionId: %v, LockedRowCount: %v, PrelockedRowCount: %v)",
            transaction->GetId(),
            lockedRowCount,
            prelockedRowCount);
    }

    void ValidateClientTimestamp(TTransactionId transactionId)
    {
        auto clientTimestamp = TimestampFromTransactionId(transactionId);
        auto serverTimestamp = Host_->GetLatestTimestamp();
        auto clientInstant = TimestampToInstant(clientTimestamp).first;
        auto serverInstant = TimestampToInstant(serverTimestamp).first;
        auto clientTimestampThreshold = Host_->GetConfig()->ClientTimestampThreshold;
        if (clientInstant > serverInstant + clientTimestampThreshold ||
            clientInstant < serverInstant - clientTimestampThreshold)
        {
            THROW_ERROR_EXCEPTION("Transaction timestamp is off limits, check the local clock readings")
            << TErrorAttribute("client_timestamp", clientTimestamp)
            << TErrorAttribute("server_timestamp", serverTimestamp);
        }
    }

    void ValidateTabletStoreLimit(TTablet* tablet)
    {
        const auto& mountConfig = tablet->GetSettings().MountConfig;
        auto storeCount = std::ssize(tablet->StoreIdMap());
        auto storeLimit = mountConfig->MaxStoresPerTablet;
        if (storeCount >= storeLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many stores in tablet, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << TErrorAttribute("store_count", storeCount)
                << TErrorAttribute("store_limit", storeLimit);
        }

        auto overlappingStoreCount = tablet->GetOverlappingStoreCount();
        auto overlappingStoreLimit = mountConfig->MaxOverlappingStoreCount;
        if (overlappingStoreCount >= overlappingStoreLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many overlapping stores in tablet, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << TErrorAttribute("overlapping_store_count", overlappingStoreCount)
                << TErrorAttribute("overlapping_store_limit", overlappingStoreLimit);
        }

        auto edenStoreCount = tablet->GetEdenStoreCount();
        auto edenStoreCountLimit = mountConfig->MaxEdenStoresPerTablet;
        if (edenStoreCount >= edenStoreCountLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many eden stores in tablet, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << TErrorAttribute("eden_store_count", edenStoreCount)
                << TErrorAttribute("eden_store_limit", edenStoreCountLimit);
        }

        auto dynamicStoreCount = tablet->GetDynamicStoreCount();
        if (dynamicStoreCount >= DynamicStoreCountLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many dynamic stores in tablet, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << TErrorAttribute("dynamic_store_count", dynamicStoreCount)
                << TErrorAttribute("dynamic_store_count_limit", DynamicStoreCountLimit);
        }

        auto overflow = tablet->GetStoreManager()->CheckOverflow();
        if (!overflow.IsOK()) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Active store is overflown, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << overflow;
        }
    }

    static bool IsReplicatorWrite(const NRpc::TAuthenticationIdentity& identity)
    {
        return identity.User == NSecurityClient::ReplicatorUserName;
    }

    static bool IsReplicatorWrite(TTransaction* transaction)
    {
        return IsReplicatorWrite(transaction->AuthenticationIdentity());
    }

    static void IncrementTabletInFlightMutationCount(TTablet* tablet, bool replicatorWrite, int delta)
    {
        if (replicatorWrite) {
            tablet->SetInFlightReplicatorMutationCount(tablet->GetInFlightReplicatorMutationCount() + delta);
        } else {
            tablet->SetInFlightUserMutationCount(tablet->GetInFlightUserMutationCount() + delta);
        }
    }

    static void IncrementTabletPendingWriteRecordCount(TTablet* tablet, bool replicatorWrite, int delta)
    {
        if (replicatorWrite) {
            tablet->SetPendingReplicatorWriteRecordCount(tablet->GetPendingReplicatorWriteRecordCount() + delta);
        } else {
            tablet->SetPendingUserWriteRecordCount(tablet->GetPendingUserWriteRecordCount() + delta);
        }
    }

    static void ValidateWriteBarrier(bool replicatorWrite, TTablet* tablet)
    {
        if (replicatorWrite) {
            if (tablet->GetInFlightUserMutationCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::ReplicatorWriteBlockedByUser,
                    "Tablet cannot accept replicator writes since some user mutations are still in flight")
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("table_path", tablet->GetTablePath())
                    << TErrorAttribute("in_flight_mutation_count", tablet->GetInFlightUserMutationCount());
            }
            if (tablet->GetPendingUserWriteRecordCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::ReplicatorWriteBlockedByUser,
                    "Tablet cannot accept replicator writes since some user writes are still pending")
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("table_path", tablet->GetTablePath())
                    << TErrorAttribute("pending_write_record_count", tablet->GetPendingUserWriteRecordCount());
            }
        } else {
            if (tablet->GetInFlightReplicatorMutationCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::UserWriteBlockedByReplicator,
                    "Tablet cannot accept user writes since some replicator mutations are still in flight")
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("table_path", tablet->GetTablePath())
                    << TErrorAttribute("in_flight_mutation_count", tablet->GetInFlightReplicatorMutationCount());
            }
            if (tablet->GetPendingReplicatorWriteRecordCount() > 0) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::UserWriteBlockedByReplicator,
                    "Tablet cannot accept user writes since some replicator writes are still pending")
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("table_path", tablet->GetTablePath())
                    << TErrorAttribute("pending_write_record_count", tablet->GetPendingReplicatorWriteRecordCount());
            }
        }
    }

    void ValidateTransactionActive(TTransaction* transaction)
    {
        if (transaction->GetTransientState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }
    }

    i64 LockTablet(TTablet* tablet)
    {
        return tablet->Lock();
    }

    i64 UnlockTablet(TTablet* tablet)
    {
        auto lockCount = tablet->Unlock();
        Host_->OnTabletUnlocked(tablet);
        return lockCount;
    }

    void UnlockLockedTablets(TTransaction* transaction)
    {
        auto& tablets = transaction->LockedTablets();
        while (!tablets.empty()) {
            auto* tablet = tablets.back();
            tablets.pop_back();
            UnlockTablet(tablet);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletCellWriteManagerPtr CreateTabletCellWriteManager(
    ITabletCellWriteManagerHostPtr host,
    ISimpleHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    TMemoryUsageTrackerGuard&& writeLogsMemoryTrackerGuard,
    IInvokerPtr automatonInvoker)
{
    return New<TTabletCellWriteManager>(
        std::move(host),
        std::move(hydraManager),
        std::move(automaton),
        std::move(writeLogsMemoryTrackerGuard),
        std::move(automatonInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
