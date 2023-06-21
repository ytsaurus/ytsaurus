#include "table_replicator.h"

#include "hint_manager.h"
#include "private.h"
#include "relative_replication_throttler.h"
#include "replication_log.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_reader.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"
#include "transaction_manager.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NHydra;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const int TabletRowsPerRead = 1000;

////////////////////////////////////////////////////////////////////////////////

class TTableReplicator::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        TTableReplicaInfo* replicaInfo,
        NNative::IConnectionPtr localConnection,
        ITabletSlotPtr slot,
        ITabletSnapshotStorePtr tabletSnapshotStore,
        IHintManagerPtr hintManager,
        IInvokerPtr workerInvoker,
        EWorkloadCategory workloadCategory,
        IThroughputThrottlerPtr nodeOutThrottler)
        : Config_(std::move(config))
        , LocalConnection_(std::move(localConnection))
        , Slot_(std::move(slot))
        , TabletSnapshotStore_(std::move(tabletSnapshotStore))
        , HintManager_(std::move(hintManager))
        , WorkerInvoker_(std::move(workerInvoker))
        , TabletId_(tablet->GetId())
        , MountRevision_(tablet->GetMountRevision())
        , TableSchema_(tablet->GetTableSchema())
        , NameTable_(TNameTable::FromSchema(*TableSchema_))
        , ReplicaId_(replicaInfo->GetId())
        , ClusterName_(replicaInfo->GetClusterName())
        , ReplicaPath_(replicaInfo->GetReplicaPath())
        , MountConfig_(tablet->GetSettings().MountConfig)
        , Logger(TabletNodeLogger.WithTag("%v, ReplicaId: %v",
            tablet->GetLoggingTag(),
            ReplicaId_))
        , ReplicationLogParser_(CreateReplicationLogParser(TableSchema_, MountConfig_, workloadCategory, Logger))
        , WorkloadCategory_(workloadCategory)
        , Throttler_(CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            std::move(nodeOutThrottler),
            CreateReconfigurableThroughputThrottler(MountConfig_->ReplicationThrottler, Logger)}))
        , RelativeThrottler_(CreateRelativeReplicationThrottler(
            MountConfig_->RelativeReplicationThrottler))
    { }

    void Enable()
    {
        Disable();

        FiberFuture_ = BIND(&TImpl::FiberMain, MakeWeak(this))
            .AsyncVia(Slot_->GetHydraManager()->GetAutomatonCancelableContext()->CreateInvoker(WorkerInvoker_))
            .Run();

        YT_LOG_INFO("Replicator fiber started");
    }

    void Disable()
    {
        if (FiberFuture_) {
            FiberFuture_.Cancel(TError("Replicator disabled"));
            YT_LOG_INFO("Replicator fiber stopped");
        }
        FiberFuture_.Reset();
    }

private:
    const TTabletManagerConfigPtr Config_;
    const NNative::IConnectionPtr LocalConnection_;
    const ITabletSlotPtr Slot_;
    const ITabletSnapshotStorePtr TabletSnapshotStore_;
    const IHintManagerPtr HintManager_;
    const IInvokerPtr WorkerInvoker_;

    const TTabletId TabletId_;
    const TRevision MountRevision_;
    const TTableSchemaPtr TableSchema_;
    const TNameTablePtr NameTable_;
    const TTableReplicaId ReplicaId_;
    const TString ClusterName_;
    const TYPath ReplicaPath_;
    const TTableMountConfigPtr MountConfig_;

    const NLogging::TLogger Logger;

    const IReplicationLogParserPtr ReplicationLogParser_;
    const EWorkloadCategory WorkloadCategory_;
    const IThroughputThrottlerPtr Throttler_;
    const IRelativeReplicationThrottlerPtr RelativeThrottler_;

    TFuture<void> FiberFuture_;

    void FiberMain()
    {
        while (true) {
            TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("TableReplicator"));
            NProfiling::TWallTimer timer;
            FiberIteration();
            TDelayedExecutor::WaitForDuration(MountConfig_->ReplicationTickPeriod - timer.GetElapsedTime());
        }
    }

    void FiberIteration()
    {
        TTableReplicaSnapshotPtr replicaSnapshot;
        try {
            auto tabletSnapshot = TabletSnapshotStore_->FindTabletSnapshot(TabletId_, MountRevision_);
            if (!tabletSnapshot) {
                THROW_ERROR_EXCEPTION("No tablet snapshot is available")
                    << HardErrorAttribute;
            }

            replicaSnapshot = tabletSnapshot->FindReplicaSnapshot(ReplicaId_);
            if (!replicaSnapshot) {
                THROW_ERROR_EXCEPTION("No table replica snapshot is available")
                    << HardErrorAttribute;
            }

            auto alienConnection = LocalConnection_->GetClusterDirectory()->FindConnection(ClusterName_);
            if (!alienConnection) {
                THROW_ERROR_EXCEPTION("Replica cluster %Qv is not known", ClusterName_)
                    << HardErrorAttribute;
            }


            const auto& tabletRuntimeData = tabletSnapshot->TabletRuntimeData;
            const auto& replicaRuntimeData = replicaSnapshot->RuntimeData;
            const auto& counters = replicaSnapshot->Counters;
            auto countError = Finally([&] {
                if (std::uncaught_exception()) {
                    counters.ReplicationErrorCount.Increment();
                }
            });

            std::optional<TDuration> ThrottleTime;
            std::optional<TDuration> RelativeThrottleTime;
            std::optional<TDuration> TransactionStartTime;
            std::optional<TDuration> TransactionCommitTime;
            std::optional<TDuration> RowsReadTime;
            std::optional<TDuration> RowsWriteTime;

            {
                auto throttleFuture = Throttler_->Throttle(1);
                if (!throttleFuture.IsSet()) {
                    TEventTimerGuard timerGuard(counters.ReplicationThrottleTime);
                    YT_LOG_DEBUG("Started waiting for replication throttling");
                    WaitFor(throttleFuture)
                        .ThrowOnError();
                    YT_LOG_DEBUG("Finished waiting for replication throttling");
                    ThrottleTime = timerGuard.GetElapsedTime();
                }
            }

            {
                auto throttleFuture = RelativeThrottler_->Throttle();
                if (!throttleFuture.IsSet()) {
                    TEventTimerGuard timerGuard(counters.ReplicationThrottleTime);
                    YT_LOG_DEBUG("Started waiting for relative replication throttling");
                    WaitFor(throttleFuture)
                        .ThrowOnError();
                    YT_LOG_DEBUG("Finished waiting for relative replication throttling");
                    RelativeThrottleTime = timerGuard.GetElapsedTime();
                }
            }

            // YT-8542: Fetch the last barrier timestamp _first_ to ensure proper serialization between
            // replicator and tablet slot threads.
            auto lastBarrierTimestamp = Slot_->GetRuntimeData()->BarrierTimestamp.load();
            auto lastReplicationRowIndex = replicaRuntimeData->CurrentReplicationRowIndex.load();
            auto lastReplicationTimestamp = replicaRuntimeData->LastReplicationTimestamp.load();
            auto totalRowCount = tabletRuntimeData->TotalRowCount.load();
            auto backupCheckpointTimestamp = tabletRuntimeData->BackupCheckpointTimestamp.load();
            if (replicaRuntimeData->PreparedReplicationRowIndex > lastReplicationRowIndex) {
                // Some log rows are prepared for replication, hence replication cannot proceed.
                // Seeing this is not typical since we're waiting for the replication commit to complete (see below).
                // However we may occasionally run into this check on epoch change or when commit times out
                // due to broken replica participant.
                replicaRuntimeData->Error.Store(TError());
                return;
            }

            auto updateCountersGuard = Finally([&] {
                auto rowCount = std::max(
                    static_cast<i64>(0),
                    tabletRuntimeData->TotalRowCount.load() - replicaRuntimeData->CurrentReplicationRowIndex.load());
                const auto& timestampProvider = LocalConnection_->GetTimestampProvider();
                auto time = (rowCount == 0)
                    ? TDuration::Zero()
                    : TimestampToInstant(timestampProvider->GetLatestTimestamp()).second - TimestampToInstant(replicaRuntimeData->CurrentReplicationTimestamp).first;

                counters.LagRowCount.Update(rowCount);
                counters.LagTime.Update(time);
            });

            if (HintManager_->IsReplicaClusterBanned(ClusterName_)) {
                YT_LOG_DEBUG("Skipping table replication iteration due to ban of replica cluster (ClusterName: %v)",
                    ClusterName_);
                return;
            }

            auto isVersioned = TableSchema_->IsSorted()
                ? replicaRuntimeData->PreserveTimestamps.load()
                : replicaRuntimeData->PreserveTimestamps.load() && ReplicationLogParser_->GetTimestampColumnId();

            if (totalRowCount <= lastReplicationRowIndex) {
                // All committed rows are replicated.
                if (lastReplicationTimestamp < lastBarrierTimestamp) {
                    replicaRuntimeData->LastReplicationTimestamp.store(lastBarrierTimestamp);
                }
                replicaRuntimeData->Error.Store(TError());
                return;
            }

            NNative::ITransactionPtr localTransaction;
            ITransactionPtr alienTransaction;
            {
                TEventTimerGuard timerGuard(counters.ReplicationTransactionStartTime);

                YT_LOG_DEBUG("Starting replication transactions");

                auto localClient = LocalConnection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));
                localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet))
                    .ValueOrThrow();

                if (backupCheckpointTimestamp && localTransaction->GetStartTimestamp() >= backupCheckpointTimestamp) {
                    YT_LOG_DEBUG("Skipping table replication iteration since tablet has passed backup checkpoint");
                    return;
                }

                auto alienClient = alienConnection->CreateClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));

                auto alienTableMountCache = alienClient->GetTableMountCache();
                auto replicaTableInfo = WaitFor(alienTableMountCache->GetTableInfo(ReplicaPath_))
                    .ValueOrThrow();

                if (replicaTableInfo->IsSorted() != TableSchema_->IsSorted()) {
                    THROW_ERROR_EXCEPTION("Replicated table and replica table should be either both sorted or both ordered, "
                        "but replicated table is %v and replica table is %v",
                        TableSchema_->IsSorted() ? "sorted": "ordered",
                        replicaTableInfo->IsSorted() ? "sorted" :  "ordered")
                        << HardErrorAttribute;
                    return;
                }

                TAlienTransactionStartOptions transactionStartOptions;
                if (!isVersioned) {
                    transactionStartOptions.Atomicity = replicaRuntimeData->Atomicity;
                }
                transactionStartOptions.StartTimestamp = localTransaction->GetStartTimestamp();

                alienTransaction = WaitFor(StartAlienTransaction(localTransaction, alienClient, transactionStartOptions))
                    .ValueOrThrow();

                YT_LOG_DEBUG("Replication transactions started (TransactionId: %v)",
                    localTransaction->GetId());
                TransactionStartTime = timerGuard.GetElapsedTime();
            }

            TRowBufferPtr rowBuffer;
            std::vector<TRowModification> replicationRows;

            i64 startRowIndex = lastReplicationRowIndex;
            bool checkPrevReplicationRowIndex = true;
            i64 newReplicationRowIndex;
            TTimestamp newReplicationTimestamp;
            i64 batchRowCount;
            i64 batchDataWeight;

            // TODO(savrus): profile chunk reader statistics.
            TClientChunkReadOptions chunkReadOptions{
                .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletReplication),
                .ReadSessionId = TReadSessionId::Create()
            };

            {
                TEventTimerGuard timerGuard(counters.ReplicationRowsReadTime);
                auto readReplicationBatch = [&] {
                    return ReadReplicationBatch(
                        MountConfig_,
                        tabletSnapshot,
                        replicaSnapshot,
                        chunkReadOptions,
                        startRowIndex,
                        &replicationRows,
                        &rowBuffer,
                        &newReplicationRowIndex,
                        &newReplicationTimestamp,
                        &batchRowCount,
                        &batchDataWeight,
                        isVersioned);
                };

                if (!readReplicationBatch()) {
                    checkPrevReplicationRowIndex = false;

                    auto startRowIndexOrNullopt = ReplicationLogParser_->ComputeStartRowIndex(
                        tabletSnapshot,
                        replicaSnapshot->StartReplicationTimestamp,
                        chunkReadOptions,
                        /*lowerRowIndex*/ std::nullopt,
                        [] {
                            THROW_ERROR_EXCEPTION("No replication log rows are available")
                                << HardErrorAttribute;
                        });
                    YT_VERIFY(startRowIndexOrNullopt);

                    startRowIndex = *startRowIndexOrNullopt;
                    YT_VERIFY(readReplicationBatch());
                }

                RowsReadTime = timerGuard.GetElapsedTime();
            }

            RelativeThrottler_->OnReplicationBatchProcessed(newReplicationTimestamp);

            if (replicationRows.empty()) {
                THROW_ERROR_EXCEPTION("Replication reader returned zero rows")
                    << HardErrorAttribute;
                return;
            }

            {
                TEventTimerGuard timerGuard(counters.ReplicationRowsWriteTime);

                TModifyRowsOptions options;
                options.UpstreamReplicaId = ReplicaId_;
                alienTransaction->ModifyRows(
                    ReplicaPath_,
                    NameTable_,
                    MakeSharedRange(std::move(replicationRows), std::move(rowBuffer)),
                    options);

                RowsWriteTime = timerGuard.GetElapsedTime();
            }

            {
                NProto::TReqReplicateRows req;
                ToProto(req.mutable_tablet_id(), TabletId_);
                ToProto(req.mutable_replica_id(), ReplicaId_);
                if (checkPrevReplicationRowIndex) {
                    req.set_prev_replication_row_index(startRowIndex);
                }
                req.set_new_replication_row_index(newReplicationRowIndex);
                req.set_new_replication_timestamp(newReplicationTimestamp);
                localTransaction->AddAction(Slot_->GetCellId(), MakeTransactionActionData(req));
            }

            {
                TEventTimerGuard timerGuard(counters.ReplicationTransactionCommitTime);
                YT_LOG_DEBUG("Started committing replication transaction");

                TTransactionCommitOptions commitOptions;
                commitOptions.CoordinatorCellId = Slot_->GetCellId();
                commitOptions.Force2PC = true;
                commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
                commitOptions.GeneratePrepareTimestamp = !replicaRuntimeData->PreserveTimestamps;
                if (backupCheckpointTimestamp) {
                    commitOptions.MaxAllowedCommitTimestamp = backupCheckpointTimestamp;
                }
                WaitFor(localTransaction->Commit(commitOptions))
                    .ThrowOnError();

                YT_LOG_DEBUG("Finished committing replication transaction");
                TransactionCommitTime = timerGuard.GetElapsedTime();
            }

            if (lastReplicationTimestamp > newReplicationTimestamp) {
                YT_LOG_ERROR("Non-monotonic change to last replication timestamp attempted; ignored (LastReplicationTimestamp: %v -> %v)",
                    lastReplicationTimestamp,
                    newReplicationTimestamp);
            } else {
                replicaRuntimeData->LastReplicationTimestamp.store(newReplicationTimestamp);
            }
            replicaRuntimeData->Error.Store(TError());

            counters.ReplicationBatchRowCount.Record(batchRowCount);
            counters.ReplicationBatchDataWeight.Record(batchDataWeight);
            counters.ReplicationRowCount.Increment(batchRowCount);
            counters.ReplicationDataWeight.Increment(batchDataWeight);
            YT_LOG_DEBUG("Rows replicated (RowCount: %v, DataWeigth: %v, ThrottleTime: %v, RelativeThrottleTime: %v, "
                "TransactionStartTime: %v, RowsReadTime: %v, RowsWriteTime: %v, TransactionCommitTime: %v)",
                batchRowCount,
                batchDataWeight,
                ThrottleTime,
                RelativeThrottleTime,
                TransactionStartTime,
                RowsReadTime,
                RowsWriteTime,
                TransactionCommitTime);
        } catch (const std::exception& ex) {
            TError error(ex);
            if (replicaSnapshot) {
                replicaSnapshot->RuntimeData->Error.Store(
                    error << TErrorAttribute("tablet_id", TabletId_));
            }
            if (error.Attributes().Get<bool>("hard", false)) {
                DoHardBackoff(error);
            } else if (error.FindMatching(NTabletClient::EErrorCode::UpstreamReplicaMismatch)) {
                DoHardBackoff(error);
            } else if (error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded)) {
                DoHardBackoff(error);
            } else {
                DoSoftBackoff(error);
            }
        }
    }

    bool ReadReplicationBatch(
        const TTableMountConfigPtr& mountConfig,
        const TTabletSnapshotPtr& tabletSnapshot,
        const TTableReplicaSnapshotPtr& replicaSnapshot,
        const TClientChunkReadOptions& chunkReadOptions,
        i64 startRowIndex,
        std::vector<TRowModification>* replicationRows,
        TRowBufferPtr* rowBuffer,
        i64* newReplicationRowIndex,
        TTimestamp* newReplicationTimestamp,
        i64* batchRowCount,
        i64* batchDataWeight,
        bool isVersioned)
    {
        auto sessionId = TReadSessionId::Create();
        YT_LOG_DEBUG("Started building replication batch (StartRowIndex: %v, ReadSessionId: %v)",
            startRowIndex,
            sessionId);

        auto reader = CreateSchemafulRangeTabletReader(
            tabletSnapshot,
            TColumnFilter(),
            MakeRowBound(startRowIndex),
            MakeRowBound(std::numeric_limits<i64>::max()),
            /*timestampRange*/ {},
            chunkReadOptions,
            /*tabletThrottlerKind*/ std::nullopt,
            WorkloadCategory_);

        int timestampCount = 0;
        int rowCount = 0;
        i64 currentRowIndex = startRowIndex;
        i64 dataWeight = 0;

        *rowBuffer = New<TRowBuffer>();
        replicationRows->clear();

        std::vector<TUnversionedRow> readerRows;
        readerRows.reserve(TabletRowsPerRead);

        // This default only makes sense if the batch turns out to be empty.
        auto prevTimestamp = replicaSnapshot->RuntimeData->CurrentReplicationTimestamp.load();

        // Throttling control.
        i64 dataWeightToThrottle = 0;
        auto acquireThrottler = [&] () {
            Throttler_->Acquire(dataWeightToThrottle);
            dataWeightToThrottle = 0;
        };
        auto isThrottlerOverdraft = [&] {
            if (!Throttler_->IsOverdraft()) {
                return false;
            }
            YT_LOG_DEBUG("Bandwidth limit reached; interrupting batch (QueueTotalCount: %v)",
                Throttler_->GetQueueTotalAmount());
            return true;
        };

        bool tooMuch = false;

        while (!tooMuch) {
            auto batch = reader->Read();
            if (!batch) {
                break;
            }

            if (batch->IsEmpty()) {
                YT_LOG_DEBUG("Waiting for replicated rows from tablet reader (StartRowIndex: %v)",
                    currentRowIndex);
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            auto readerRows = batch->MaterializeRows();

            YT_LOG_DEBUG("Got replicated rows from tablet reader (StartRowIndex: %v, RowCount: %v)",
                currentRowIndex,
                readerRows.size());

            for (auto row : readerRows) {
                TTypeErasedRow replicationRow;
                ERowModificationType modificationType;
                i64 rowIndex;
                TTimestamp timestamp;

                ReplicationLogParser_->ParseLogRow(
                    tabletSnapshot,
                    row,
                    *rowBuffer,
                    &replicationRow,
                    &modificationType,
                    &rowIndex,
                    &timestamp,
                    isVersioned);

                if (timestamp <= replicaSnapshot->StartReplicationTimestamp) {
                    YT_VERIFY(row.GetHeader() == readerRows[0].GetHeader());
                    YT_LOG_INFO("Replication log row violates timestamp bound (StartReplicationTimestamp: %v, LogRecordTimestamp: %v)",
                        replicaSnapshot->StartReplicationTimestamp,
                        timestamp);
                    return false;
                }

                if (currentRowIndex != rowIndex) {
                    THROW_ERROR_EXCEPTION("Replication log row index mismatch in tablet %v: expected %v, got %v",
                        tabletSnapshot->TabletId,
                        currentRowIndex,
                        rowIndex)
                        << HardErrorAttribute;
                }

                if (timestamp != prevTimestamp) {
                    acquireThrottler();

                    if (rowCount >= mountConfig->MaxRowsPerReplicationCommit ||
                        dataWeight >= mountConfig->MaxDataWeightPerReplicationCommit ||
                        timestampCount >= mountConfig->MaxTimestampsPerReplicationCommit ||
                        isThrottlerOverdraft())
                    {
                        tooMuch = true;
                        break;
                    }

                    ++timestampCount;
                }

                ++currentRowIndex;
                ++rowCount;

                auto rowDataWeight = GetDataWeight(row);
                dataWeight += rowDataWeight;
                dataWeightToThrottle += rowDataWeight;
                replicationRows->push_back({modificationType, replicationRow, TLockMask()});
                prevTimestamp = timestamp;
            }
        }
        acquireThrottler();

        *newReplicationRowIndex = startRowIndex + rowCount;
        *newReplicationTimestamp = prevTimestamp;
        *batchRowCount = rowCount;
        *batchDataWeight = dataWeight;

        YT_LOG_DEBUG("Finished building replication batch (StartRowIndex: %v, RowCount: %v, DataWeight: %v, "
            "NewReplicationRowIndex: %v, NewReplicationTimestamp: %v)",
            startRowIndex,
            rowCount,
            dataWeight,
            *newReplicationRowIndex,
            *newReplicationTimestamp);

        return true;
    }


    void DoSoftBackoff(const TError& error)
    {
        YT_LOG_INFO(error, "Doing soft backoff");
        TDelayedExecutor::WaitForDuration(Config_->ReplicatorSoftBackoffTime);
    }

    void DoHardBackoff(const TError& error)
    {
        YT_LOG_INFO(error, "Doing hard backoff");
        TDelayedExecutor::WaitForDuration(Config_->ReplicatorHardBackoffTime);
    }

    static TLegacyOwningKey MakeRowBound(i64 rowIndex)
    {
        return MakeUnversionedOwningRow(
            -1, // tablet id, fake
            rowIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

TTableReplicator::TTableReplicator(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    TTableReplicaInfo* replicaInfo,
    NNative::IConnectionPtr localConnection,
    ITabletSlotPtr slot,
    ITabletSnapshotStorePtr tabletSnapshotStore,
    IHintManagerPtr hintManager,
    IInvokerPtr workerInvoker,
    EWorkloadCategory workloadCategory,
    IThroughputThrottlerPtr nodeOutThrottler)
    : Impl_(New<TImpl>(
        std::move(config),
        tablet,
        replicaInfo,
        std::move(localConnection),
        std::move(slot),
        std::move(tabletSnapshotStore),
        std::move(hintManager),
        std::move(workerInvoker),
        workloadCategory,
        std::move(nodeOutThrottler)))
{ }

TTableReplicator::~TTableReplicator() = default;

void TTableReplicator::Enable()
{
    Impl_->Enable();
}

void TTableReplicator::Disable()
{
    Impl_->Disable();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
