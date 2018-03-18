#include "table_replicator.h"
#include "tablet.h"
#include "slot_manager.h"
#include "tablet_slot.h"
#include "tablet_reader.h"
#include "tablet_manager.h"
#include "transaction_manager.h"
#include "config.h"
#include "private.h"

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/name_table.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_transaction.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/transaction_client/action.h>
#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/misc/workload.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/misc/finally.h>

namespace NYT {
namespace NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto MountConfigUpdatePeriod = TDuration::Seconds(3);
static const auto ReplicationTickPeriod = TDuration::MilliSeconds(100);
static const int TabletRowsPerRead = 1000;
static const auto HardErrorAttribute = TErrorAttribute("hard", true);

////////////////////////////////////////////////////////////////////////////////

class TTableReplicator::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        TTableReplicaInfo* replicaInfo,
        INativeConnectionPtr localConnection,
        TTabletSlotPtr slot,
        TSlotManagerPtr slotManager,
        IInvokerPtr workerInvoker)
        : Config_(std::move(config))
        , LocalConnection_(std::move(localConnection))
        , Slot_(std::move(slot))
        , SlotManager_(std::move(slotManager))
        , WorkerInvoker_(std::move(workerInvoker))
        , TabletId_(tablet->GetId())
        , TableSchema_(tablet->TableSchema())
        , ReplicaId_(replicaInfo->GetId())
        , ClusterName_(replicaInfo->GetClusterName())
        , ReplicaPath_(replicaInfo->GetReplicaPath())
        , Logger(NLogging::TLogger(TabletNodeLogger)
            .AddTag("TabletId: %v, ReplicaId: %v",
                TabletId_,
                ReplicaId_))
        , MountConfigUpdateExecutor_(New<TPeriodicExecutor>(
            Slot_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Read),
            BIND(&TImpl::OnUpdateMountConfig, MakeWeak(this)),
            MountConfigUpdatePeriod))
        , Throttler_(CreateReconfigurableThroughputThrottler(
            tablet->GetConfig()->ReplicationThrottler,
            Logger,
            replicaInfo->GetReplicatorProfiler()))
    {
        MountConfigUpdateExecutor_->Start();
    }

    void Enable()
    {
        Disable();

        FiberFuture_ = BIND(&TImpl::FiberMain, MakeWeak(this))
            .AsyncVia(Slot_->GetHydraManager()->GetAutomatonCancelableContext()->CreateInvoker(WorkerInvoker_))
            .Run();

        LOG_INFO("Replicator fiber started");
    }

    void Disable()
    {
        if (FiberFuture_) {
            FiberFuture_.Cancel();
            FiberFuture_.Reset();

            LOG_INFO("Replicator fiber stopped");
        }
    }

private:
    const TTabletManagerConfigPtr Config_;
    const INativeConnectionPtr LocalConnection_;
    const TTabletSlotPtr Slot_;
    const TSlotManagerPtr SlotManager_;
    const IInvokerPtr WorkerInvoker_;

    const TTabletId TabletId_;
    const TTableSchema TableSchema_;
    const TTableReplicaId ReplicaId_;
    const TString ClusterName_;
    const TYPath ReplicaPath_;

    const NLogging::TLogger Logger;

    const TPeriodicExecutorPtr MountConfigUpdateExecutor_;
    const IReconfigurableThroughputThrottlerPtr Throttler_;

    TFuture<void> FiberFuture_;

    TSpinLock MountConfigLock_;
    TTableMountConfigPtr MountConfig_;


    TTableMountConfigPtr GetMountConfig()
    {
        auto guard = Guard(MountConfigLock_);
        return MountConfig_;
    }

    void SetMountConfig(TTableMountConfigPtr config)
    {
        if (config) {
            Throttler_->Reconfigure(config->ReplicationThrottler);
        }
        auto guard = Guard(MountConfigLock_);
        MountConfig_ = std::move(config);
    }

    void OnUpdateMountConfig()
    {
        const auto& tabletManager = Slot_->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(TabletId_);
        SetMountConfig(tablet ? tablet->GetConfig() : nullptr);
    }


    void FiberMain()
    {
        while (true) {
            TDelayedExecutor::WaitForDuration(ReplicationTickPeriod);
            FiberIteration();
        }
    }

    bool CheckThrottler(i64 dataWeight)
    {
        Throttler_->Acquire(dataWeight);
        if (Throttler_->IsOverdraft()) {
            LOG_DEBUG("Bandwidth limit is reached (TotalCount: %v, DataWeight: %v)",
                Throttler_->GetQueueTotalCount(),
                dataWeight);
            return false;
        }
        return true;
    }

    void FiberIteration()
    {
        TTableReplicaSnapshotPtr replicaSnapshot;
        try {
            auto tabletSnapshot = SlotManager_->FindTabletSnapshot(TabletId_);
            if (!tabletSnapshot) {
                THROW_ERROR_EXCEPTION("No tablet snapshot is available")
                    << HardErrorAttribute;
            }

            replicaSnapshot = tabletSnapshot->FindReplicaSnapshot(ReplicaId_);
            if (!replicaSnapshot) {
                THROW_ERROR_EXCEPTION("No table replica snapshot is available")
                    << HardErrorAttribute;
            }

            auto mountConfig = GetMountConfig();
            if (!mountConfig) {
                THROW_ERROR_EXCEPTION("No mount configuration is available");
            }

            auto foreignConnection = LocalConnection_->GetClusterDirectory()->FindConnection(ClusterName_);
            if (!foreignConnection) {
                THROW_ERROR_EXCEPTION("Replica cluster %Qv is not known", ClusterName_)
                    << HardErrorAttribute;
            }

            if (Throttler_->IsOverdraft()) {
                LOG_DEBUG("Bandwidth limit reached; skipping iteration (TotalCount: %v)",
                    Throttler_->GetQueueTotalCount());
                return;
            }

            const auto& tabletRuntimeData = tabletSnapshot->RuntimeData;
            const auto& replicaRuntimeData = replicaSnapshot->RuntimeData;

            // YT-8542: Fetch the last barrier timestamp _first_ to ensure proper serialization between
            // replicator and tablet slot threads.
            auto lastBarrierTimestamp = Slot_->GetRuntimeData()->LastBarrierTimestamp.load();
            auto lastReplicationRowIndex = replicaRuntimeData->CurrentReplicationRowIndex.load();
            auto lastReplicationTimestamp = replicaRuntimeData->LastReplicationTimestamp.load();
            auto totalRowCount = tabletRuntimeData->TotalRowCount.load();
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
                auto time = (rowCount == 0)
                    ? TDuration::Zero()
                    : TimestampDiffToDuration(replicaRuntimeData->CurrentReplicationTimestamp, tabletRuntimeData->LastWriteTimestamp).second;
                auto* counters = replicaSnapshot->Counters;
                if (counters) {
                    TabletNodeProfiler.Update(counters->LagRowCount, rowCount);
                    TabletNodeProfiler.Update(counters->LagTime, NProfiling::DurationToValue(time));
                }
            });

            if (totalRowCount <= lastReplicationRowIndex) {
                // All committed rows are replicated.
                if (lastReplicationTimestamp < lastBarrierTimestamp) {
                    replicaRuntimeData->LastReplicationTimestamp.store(lastBarrierTimestamp);
                }
                replicaRuntimeData->Error.Store(TError());
                return;
            }

            LOG_DEBUG("Starting replication transactions");

            auto localClient = LocalConnection_->CreateNativeClient(TClientOptions(NSecurityClient::ReplicatorUserName));
            auto localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet))
                .ValueOrThrow();

            auto foreignClient = foreignConnection->CreateClient(TClientOptions(NSecurityClient::ReplicatorUserName));
            auto foreignTransaction = WaitFor(localTransaction->StartForeignTransaction(foreignClient))
                .ValueOrThrow();

            YCHECK(localTransaction->GetId() == foreignTransaction->GetId());
            LOG_DEBUG("Replication transactions started (TransactionId: %v)",
                localTransaction->GetId());

            TRowBufferPtr rowBuffer;
            std::vector<TVersionedRow> replicationRows;

            i64 startRowIndex = lastReplicationRowIndex;
            i64 newReplicationRowIndex;
            TTimestamp newReplicationTimestamp;

            auto readReplicationBatch = [&] () {
                return ReadReplicationBatch(
                    mountConfig,
                    tabletSnapshot,
                    replicaSnapshot,
                    startRowIndex,
                    &replicationRows,
                    &rowBuffer,
                    &newReplicationRowIndex,
                    &newReplicationTimestamp);
            };

            if (!readReplicationBatch()) {
                startRowIndex = ComputeStartRowIndex(
                    mountConfig,
                    tabletSnapshot,
                    replicaSnapshot);
                YCHECK(readReplicationBatch());
            }

            {
                TModifyRowsOptions options;
                options.UpstreamReplicaId = ReplicaId_;
                foreignTransaction->WriteRows(
                    ReplicaPath_,
                    TNameTable::FromSchema(TableSchema_),
                    MakeSharedRange(std::move(replicationRows), std::move(rowBuffer)),
                    options);
            }

            {
                NProto::TReqReplicateRows req;
                ToProto(req.mutable_tablet_id(), TabletId_);
                ToProto(req.mutable_replica_id(), ReplicaId_);
                req.set_new_replication_row_index(newReplicationRowIndex);
                req.set_new_replication_timestamp(newReplicationTimestamp);
                localTransaction->AddAction(Slot_->GetCellId(), MakeTransactionActionData(req));
            }

            LOG_DEBUG("Started committing replication transaction");
            {
                TTransactionCommitOptions commitOptions;
                commitOptions.CoordinatorCellId = Slot_->GetCellId();
                commitOptions.Force2PC = true;
                commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
                WaitFor(localTransaction->Commit(commitOptions))
                    .ThrowOnError();
            }
            LOG_DEBUG("Finished committing replication transaction");

            if (lastReplicationTimestamp > newReplicationTimestamp) {
                LOG_ERROR("Non-monotonic change to last replication timestamp attempted; ignored (LastReplicationTimestamp: %llx -> %llx)",
                    lastReplicationTimestamp,
                    newReplicationTimestamp);
            } else {
                replicaRuntimeData->LastReplicationTimestamp.store(newReplicationTimestamp);
            }
            replicaRuntimeData->Error.Store(TError());
        } catch (const std::exception& ex) {
            TError error(ex);
            if (replicaSnapshot) {
                replicaSnapshot->RuntimeData->Error.Store(
                    error << TErrorAttribute("tablet_id", TabletId_));
            }
            if (error.Attributes().Get<bool>("hard", false)) {
                DoHardBackoff(error);
            } else {
                DoSoftBackoff(error);
            }
        }
    }

    TTimestamp ReadLogRowTimestamp(
        const TTableMountConfigPtr& mountConfig,
        const TTabletSnapshotPtr& tabletSnapshot,
        i64 rowIndex)
    {
        auto reader = CreateSchemafulTabletReader(
            tabletSnapshot,
            TColumnFilter(),
            MakeRowBound(rowIndex),
            MakeRowBound(rowIndex + 1),
            NullTimestamp,
            TWorkloadDescriptor(EWorkloadCategory::SystemReplication),
            TReadSessionId());

        std::vector<TUnversionedRow> readerRows;
        readerRows.reserve(1);

        while (true) {
            if (!reader->Read(&readerRows)) {
                THROW_ERROR_EXCEPTION("Missing row %v in replication log of tablet %v",
                    rowIndex,
                    tabletSnapshot->TabletId)
                    << HardErrorAttribute;
            }

            if (readerRows.empty()) {
                LOG_DEBUG(
                    "Waiting for log row from tablet reader (RowIndex: %v)",
                    rowIndex);
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            // One row is enough.
            break;
        }

        YCHECK(readerRows.size() == 1);

        i64 actualRowIndex;
        TTimestamp timestamp;
        ParseLogRow(
            tabletSnapshot,
            mountConfig,
            readerRows[0],
            nullptr,
            nullptr,
            &actualRowIndex,
            &timestamp);

        YCHECK(actualRowIndex == rowIndex);

        LOG_DEBUG("Replication log row timestamp is read (RowIndex: %v, Timestamp: %llx)",
            rowIndex,
            timestamp);

        return timestamp;
    }

    i64 ComputeStartRowIndex(
        const TTableMountConfigPtr& mountConfig,
        const TTabletSnapshotPtr& tabletSnapshot,
        const TTableReplicaSnapshotPtr& replicaSnapshot)
    {
        auto trimmedRowCount = tabletSnapshot->RuntimeData->TrimmedRowCount.load();
        auto totalRowCount = tabletSnapshot->RuntimeData->TotalRowCount.load();

        auto rowIndexLo = trimmedRowCount;
        auto rowIndexHi = totalRowCount;
        if (rowIndexLo == rowIndexHi) {
            THROW_ERROR_EXCEPTION("No replication log rows are available")
                << HardErrorAttribute;
        }

        auto startReplicationTimestamp = replicaSnapshot->StartReplicationTimestamp;

        LOG_DEBUG("Started computing replication start row index (StartReplicationTimestamp: %llx, RowIndexLo: %v, RowIndexHi: %v)",
            startReplicationTimestamp,
            rowIndexLo,
            rowIndexHi);

        while (rowIndexLo < rowIndexHi - 1) {
            auto rowIndexMid = rowIndexLo + (rowIndexHi - rowIndexLo) / 2;
            auto timestampMid = ReadLogRowTimestamp(mountConfig, tabletSnapshot, rowIndexMid);
            if (timestampMid <= startReplicationTimestamp) {
                rowIndexLo = rowIndexMid;
            } else {
                rowIndexHi = rowIndexMid;
            }
        }

        auto startRowIndex = rowIndexLo;
        auto startTimestamp = NullTimestamp;
        while (startRowIndex < totalRowCount) {
            startTimestamp = ReadLogRowTimestamp(mountConfig, tabletSnapshot, startRowIndex);
            if (startTimestamp > startReplicationTimestamp) {
                break;
            }
            ++startRowIndex;
        }

        LOG_DEBUG("Finished computing replication start row index (StartRowIndex: %v, StartTimestamp: %llx)",
            startRowIndex,
            startTimestamp);

        return startRowIndex;
    }

    bool ReadReplicationBatch(
        const TTableMountConfigPtr& mountConfig,
        const TTabletSnapshotPtr& tabletSnapshot,
        const TTableReplicaSnapshotPtr& replicaSnapshot,
        i64 startRowIndex,
        std::vector<TVersionedRow>* replicationRows,
        TRowBufferPtr* rowBuffer,
        i64* newReplicationRowIndex,
        TTimestamp* newReplicationTimestamp)
    {
        auto sessionId = TReadSessionId::Create();
        LOG_DEBUG("Started building replication batch (StartRowIndex: %v, ReadSessionId: %v)",
            startRowIndex,
            sessionId);

        auto reader = CreateSchemafulTabletReader(
            tabletSnapshot,
            TColumnFilter(),
            MakeRowBound(startRowIndex),
            MakeRowBound(std::numeric_limits<i64>::max()),
            NullTimestamp,
            TWorkloadDescriptor(EWorkloadCategory::SystemReplication),
            sessionId);

        int timestampCount = 0;
        int rowCount = 0;
        i64 currentRowIndex = startRowIndex;
        i64 dataWeight = 0;

        *rowBuffer = New<TRowBuffer>();
        replicationRows->clear();

        std::vector<TUnversionedRow> readerRows;
        readerRows.reserve(TabletRowsPerRead);

        // This default only makes sence if the batch turns out to be empty.
        auto prevTimestamp = replicaSnapshot->RuntimeData->CurrentReplicationTimestamp.load();

        // Throttling control.
        i64 dataWeightToAcquire = 0;
        auto flushThrottler = [&] {
            Throttler_->Acquire(dataWeightToAcquire);
            dataWeightToAcquire = 0;
        };
        auto acquireThrottler = [&] (i64 dataWeight) {
            dataWeightToAcquire += dataWeight;
            if (dataWeightToAcquire >= 1) {
                flushThrottler();
            }
        };
        auto isThrottlerOverdraft = [&] {
            if (!Throttler_->IsOverdraft()) {
                return false;
            }
            LOG_DEBUG("Bandwidth limit reached; interrupting batch (QueueTotalCount: %v)",
                Throttler_->GetQueueTotalCount());
            return true;
        };

        bool tooMuch = false;
        while (!tooMuch) {
            if (!reader->Read(&readerRows)) {
                break;
            }

            if (readerRows.empty()) {
                LOG_DEBUG("Waiting for replicated rows from tablet reader (StartRowIndex: %v)",
                    currentRowIndex);
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            LOG_DEBUG("Got replicated rows from tablet reader (StartRowIndex: %v, RowCount: %v)",
                currentRowIndex,
                readerRows.size());

            for (auto row : readerRows) {
                TVersionedRow replicationRow;
                i64 rowIndex;
                TTimestamp timestamp;
                ParseLogRow(
                    tabletSnapshot,
                    mountConfig,
                    row,
                    *rowBuffer,
                    &replicationRow,
                    &rowIndex,
                    &timestamp);

                if (timestamp <= replicaSnapshot->StartReplicationTimestamp) {
                    YCHECK(row == readerRows[0]);
                    LOG_INFO("Replication log row violates timestamp bound (StartReplicationTimestamp: %llx, LogRecordTimestamp: %llx)",
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
                acquireThrottler(rowDataWeight);
                dataWeight += rowDataWeight;

                replicationRows->push_back(replicationRow);
                prevTimestamp = timestamp;
            }
        }

        flushThrottler();

        *newReplicationRowIndex = startRowIndex + rowCount;
        *newReplicationTimestamp = prevTimestamp;

        LOG_DEBUG("Finished building replication batch (StartRowIndex: %v, RowCount: %v, DataWeight: %v, "
            "NewReplicationRowIndex: %v, NewReplicationTimestamp: %llx)",
            startRowIndex,
            rowCount,
            dataWeight,
            *newReplicationRowIndex,
            *newReplicationTimestamp);

        return true;
    }


    void DoSoftBackoff(const TError& error)
    {
        LOG_INFO(error, "Doing soft backoff");
        TDelayedExecutor::WaitForDuration(Config_->ReplicatorSoftBackoffTime);
    }

    void DoHardBackoff(const TError& error)
    {
        LOG_INFO(error, "Doing hard backoff");
        TDelayedExecutor::WaitForDuration(Config_->ReplicatorHardBackoffTime);
    }


    void ParseLogRow(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TTableMountConfigPtr& mountConfig,
        TUnversionedRow logRow,
        const TRowBufferPtr& rowBuffer,
        TVersionedRow* replicationRow,
        i64* rowIndex,
        TTimestamp* timestamp)
    {
        Y_ASSERT(logRow[1].Type == EValueType::Int64);
        *rowIndex = logRow[1].Data.Int64;

        Y_ASSERT(logRow[2].Type == EValueType::Uint64);
        *timestamp = logRow[2].Data.Uint64;

        if (!replicationRow) {
            return;
        }

        Y_ASSERT(logRow[3].Type == EValueType::Int64);
        auto changeType = ERowModificationType(logRow[3].Data.Int64);

        int keyColumnCount = tabletSnapshot->TableSchema.GetKeyColumnCount();
        int valueColumnCount = tabletSnapshot->TableSchema.GetValueColumnCount();

        Y_ASSERT(logRow.GetCount() == keyColumnCount + valueColumnCount * 2 + 4);

        switch (changeType) {
            case ERowModificationType::Write: {
                Y_ASSERT(logRow.GetCount() >= keyColumnCount + 4);
                int replicationValueCount = 0;
                for (int logValueIndex = 0; logValueIndex < valueColumnCount; ++logValueIndex) {
                    const auto& value = logRow[logValueIndex * 2 + keyColumnCount + 5];
                    Y_ASSERT(value.Type == EValueType::Uint64);
                    auto flags = static_cast<EReplicationLogDataFlags>(value.Data.Uint64);
                    if (None(flags & EReplicationLogDataFlags::Missing)) {
                        ++replicationValueCount;
                    }
                }
                auto mutableReplicationRow = rowBuffer->AllocateVersioned(
                    keyColumnCount,
                    replicationValueCount,
                    1,  // writeTimestampCount
                    0); // deleteTimestampCount
                for (int keyIndex = 0; keyIndex < keyColumnCount; ++keyIndex) {
                    mutableReplicationRow.BeginKeys()[keyIndex] = rowBuffer->Capture(logRow[keyIndex + 4]);
                }
                int replicationValueIndex = 0;
                for (int logValueIndex = 0; logValueIndex < valueColumnCount; ++logValueIndex) {
                    const auto& flagsValue = logRow[logValueIndex * 2 + keyColumnCount + 5];
                    Y_ASSERT(flagsValue.Type == EValueType::Uint64);
                    auto flags = static_cast<EReplicationLogDataFlags>(flagsValue.Data.Uint64);
                    if (None(flags & EReplicationLogDataFlags::Missing)) {
                        TVersionedValue value;
                        static_cast<TUnversionedValue&>(value) = rowBuffer->Capture(logRow[logValueIndex * 2 + keyColumnCount + 4]);
                        value.Id = logValueIndex + keyColumnCount;
                        value.Aggregate = Any(flags & EReplicationLogDataFlags::Aggregate);
                        value.Timestamp = *timestamp;
                        mutableReplicationRow.BeginValues()[replicationValueIndex++] = value;
                    }
                }
                YCHECK(replicationValueIndex == replicationValueCount);
                mutableReplicationRow.BeginWriteTimestamps()[0] = *timestamp;
                *replicationRow = mutableReplicationRow;
                LOG_DEBUG_IF(mountConfig->EnableReplicationLogging, "Replicating write (Row: %v)", *replicationRow);
                break;
            }

            case ERowModificationType::Delete: {
                auto mutableReplicationRow = rowBuffer->AllocateVersioned(
                    keyColumnCount,
                    0,  // valueCount
                    0,  // writeTimestampCount
                    1); // deleteTimestampCount
                for (int index = 0; index < keyColumnCount; ++index) {
                    mutableReplicationRow.BeginKeys()[index] = rowBuffer->Capture(logRow[index + 4]);
                }
                mutableReplicationRow.BeginDeleteTimestamps()[0] = *timestamp;
                *replicationRow = mutableReplicationRow;
                LOG_DEBUG_IF(mountConfig->EnableReplicationLogging, "Replicating delete (Row: %v)", *replicationRow);
                break;
            }

            default:
                Y_UNREACHABLE();
        }
    }

    static TOwningKey MakeRowBound(i64 rowIndex)
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(-1, 0)); // tablet id, fake
        builder.AddValue(MakeUnversionedInt64Value(rowIndex, 1)); // row index
        return builder.FinishRow();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTableReplicator::TTableReplicator(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    TTableReplicaInfo* replicaInfo,
    INativeConnectionPtr localConnection,
    TTabletSlotPtr slot,
    TSlotManagerPtr slotManager,
    IInvokerPtr workerInvoker)
    : Impl_(New<TImpl>(
        std::move(config),
        tablet,
        replicaInfo,
        std::move(localConnection),
        std::move(slot),
        std::move(slotManager),
        std::move(workerInvoker)))
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

} // namespace NTabletNode
} // namespace NYT
