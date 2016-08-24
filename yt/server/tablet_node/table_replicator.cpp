#include "table_replicator.h"
#include "tablet.h"
#include "slot_manager.h"
#include "tablet_slot.h"
#include "tablet_reader.h"
#include "tablet_manager.h"
#include "config.h"
#include "private.h"

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/name_table.h>

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_transaction.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/transaction_client/action.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/misc/workload.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/delayed_executor.h>

namespace NYT {
namespace NTabletNode {

using namespace NHiveClient;
using namespace NYPath;
using namespace NConcurrency;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const auto MountConfigUpdatePeriod = TDuration::Seconds(3);
static const auto ReplicationTickPeriod = TDuration::MilliSeconds(100);
static const int TabletRowsPerRead = 1024;

////////////////////////////////////////////////////////////////////////////////

class TTableReplicator::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        TTableReplicaInfo* replicaInfo,
        TClusterDirectoryPtr clusterDirectory,
        INativeConnectionPtr localConnection,
        TTabletSlotPtr slot,
        TSlotManagerPtr slotManager,
        IInvokerPtr workerInvoker)
        : Config_(std::move(config))
        , ClusterDirectory_(std::move(clusterDirectory))
        , LocalConnection_(std::move(localConnection))
        , Slot_(std::move(slot))
        , SlotManager_(std::move(slotManager))
        , WorkerInvoker_(std::move(workerInvoker))
        , TabletId_(tablet->GetId())
        , TableSchema_(tablet->TableSchema())
        , ReplicaId_(replicaInfo->GetId())
        , ClusterName_(replicaInfo->GetClusterName())
        , ReplicaPath_(replicaInfo->GetReplicaPath())
        , MountConfigUpdateExecutor_(New<TPeriodicExecutor>(
            Slot_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Read),
            BIND(&TImpl::OnUpdateMountConfig, MakeWeak(this)),
            MountConfigUpdatePeriod))
        , Logger(NLogging::TLogger(TabletNodeLogger)
            .AddTag("TabletId: %v, ReplicaId: %v",
                TabletId_,
                ReplicaId_))
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
    const TClusterDirectoryPtr ClusterDirectory_;
    const INativeConnectionPtr LocalConnection_;
    const TTabletSlotPtr Slot_;
    const TSlotManagerPtr SlotManager_;
    const IInvokerPtr WorkerInvoker_;

    const TTabletId TabletId_;
    const TTableSchema TableSchema_;
    const TTableReplicaId ReplicaId_;
    const Stroka ClusterName_;
    const TYPath ReplicaPath_;

    const TPeriodicExecutorPtr MountConfigUpdateExecutor_;
    const NLogging::TLogger Logger;

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
        auto guard = Guard(MountConfigLock_);
        MountConfig_ = std::move(config);
    }

    void OnUpdateMountConfig()
    {
        auto tabletManager = Slot_->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(TabletId_);
        SetMountConfig(tablet ? tablet->GetConfig() : nullptr);
    }


    void FiberMain()
    {
        while (true) {
            WaitFor(TDelayedExecutor::MakeDelayed(ReplicationTickPeriod));
            FiberIteration();
        }
    }

    void FiberIteration()
    {
        auto mountConfig = GetMountConfig();
        if (!mountConfig) {
            DoSoftBackoff(TError("No mount configuration is available"));
            return;
        }

        auto remoteConnection = ClusterDirectory_->FindConnection(ClusterName_);
        if (!remoteConnection) {
            DoHardBackoff(TError("Replica cluster %Qv is not known", ClusterName_));
            return;
        }

        auto tabletSnapshot = SlotManager_->FindTabletSnapshot(TabletId_);
        if (!tabletSnapshot) {
            DoHardBackoff(TError("No tablet snapshot is available"));
            return;
        }

        auto replicaSnapshot = tabletSnapshot->FindReplicaSnapshot(ReplicaId_);
        if (!replicaSnapshot) {
            DoHardBackoff(TError("No table replica snapshot is available"));
            return;
        }

        const auto& tabletRuntimeData = tabletSnapshot->RuntimeData;
        const auto& replicaRuntimeData = replicaSnapshot->RuntimeData;
        auto startRowIndex = replicaRuntimeData->CurrentReplicationRowIndex.load();
        if (tabletRuntimeData->TotalRowCount <= startRowIndex) {
            return;
        }
        if (replicaRuntimeData->PreparedReplicationRowIndex > startRowIndex) {
            return;
        }

        INativeTransactionPtr localTransaction;
        ITransactionPtr remoteTransaction;
        try {
            LOG_DEBUG("Starting replication transactions");

            // TODO(babenko): use "replicator" user
            auto localClient = LocalConnection_->CreateNativeClient(TClientOptions(NSecurityClient::RootUserName));
            localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet))
                .ValueOrThrow();

            // TODO(babenko): use "replicator" user
            auto remoteClient = remoteConnection->CreateClient(TClientOptions(NSecurityClient::RootUserName));
            remoteTransaction = WaitFor(localTransaction->StartSlaveTransaction(remoteClient))
                .ValueOrThrow();

            YCHECK(localTransaction->GetId() == remoteTransaction->GetId());
            LOG_DEBUG("Replication transactions started (TransactionId: %v)",
                localTransaction->GetId());
        } catch (const std::exception& ex) {
            DoSoftBackoff(ex);
            return;
        }
        
        LOG_DEBUG("Started building replication batch (StartRowIndex: %v)",
            startRowIndex);

        auto reader = CreateSchemafulTabletReader(
            tabletSnapshot,
            TColumnFilter(),
            MakeLowerBound(startRowIndex),
            MakeUpperBound(),
            NullTimestamp,
            TWorkloadDescriptor(EWorkloadCategory::SystemReplication));

        int rowCount = 0;
        i64 currentRowIndex = startRowIndex;
        i64 dataWeight = 0;
        TTimestamp newReplicationTimestamp;

        std::vector<TUnversionedRow> readerRows;
        readerRows.reserve(TabletRowsPerRead);

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TRowModification> modifications;

        bool tooMuch = false;

        while (!tooMuch) {
            try {
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
            } catch (const std::exception& ex) {
                DoSoftBackoff(ex);
                return;
            }
 
            LOG_DEBUG("Got replicated rows from tablet reader (StartRowIndex: %v, RowCount: %v)",
                currentRowIndex,
                readerRows.size());

            for (auto row : readerRows) {
                i64 actualRowIndex;
                ParseLogRow(
                    tabletSnapshot,
                    mountConfig,
                    row,
                    rowBuffer,
                    &modifications,
                    &actualRowIndex,
                    &newReplicationTimestamp);

                if (currentRowIndex != actualRowIndex) {
                    DoHardBackoff(TError("Log row index mismatch: expected %v, got %v",
                        currentRowIndex,
                        actualRowIndex));
                    return;
                }

                ++currentRowIndex;
                ++rowCount;
                dataWeight += GetDataWeight(row);

                if (rowCount >= mountConfig->MaxRowsPerReplicationCommit ||
                    dataWeight >= mountConfig->MaxDataWeightPerReplicationCommit)
                {
                    tooMuch = true;
                    break;
                }
            }
        }

        YCHECK(rowCount > 0);
        auto newReplicationRowIndex = startRowIndex + rowCount;

        LOG_DEBUG("Finished building replication batch (StartRowIndex: %v, RowCount: %v, DataWeight: %v, "
            "NewReplicationRowIndex: %v, NewReplicationTimestamp: %v)",
            currentRowIndex,
            rowCount,
            dataWeight,
            newReplicationRowIndex,
            newReplicationTimestamp);

        try {
            remoteTransaction->ModifyRows(
                ReplicaPath_,
                TNameTable::FromSchema(TableSchema_),
                MakeSharedRange(std::move(modifications), std::move(rowBuffer)));
            
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
                WaitFor(localTransaction->Commit(commitOptions))
                    .ThrowOnError();
            }
            LOG_DEBUG("Finished committing replication transaction");
        } catch (const std::exception& ex) {
            DoSoftBackoff(ex);
            return;
        }
    }


    void DoSoftBackoff(const TError& error)
    {
        LOG_INFO(error, "Doing soft backoff");
        WaitFor(TDelayedExecutor::MakeDelayed(Config_->ReplicatorSoftBackoffTime));
    }

    void DoHardBackoff(const TError& error)
    {
        LOG_INFO(error, "Doing hard backoff");
        WaitFor(TDelayedExecutor::MakeDelayed(Config_->ReplicatorHardBackoffTime));
    }


    void ParseLogRow(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TTableMountConfigPtr& mountConfig,
        TUnversionedRow logRow,
        const TRowBufferPtr& rowBuffer,
        std::vector<TRowModification>* modifications,
        i64* rowIndex,
        TTimestamp* timestamp)
    {
        Y_ASSERT(logRow[1].Type == EValueType::Int64);
        *rowIndex = logRow[1].Data.Int64;

        Y_ASSERT(logRow[2].Type == EValueType::Uint64);
        *timestamp = logRow[2].Data.Uint64;

        Y_ASSERT(logRow[3].Type == EValueType::Int64);
        auto changeType = ERowModificationType(logRow[3].Data.Int64);

        int keyColumnCount = tabletSnapshot->TableSchema.GetKeyColumnCount();
        int valueColumnCount = tabletSnapshot->TableSchema.GetValueColumnCount();

        Y_ASSERT(logRow.GetCount() == keyColumnCount + valueColumnCount* 2 + 4);

        TRowModification modification;
        switch (changeType) {
            case ERowModificationType::Write: {
                Y_ASSERT(logRow.GetCount() >= keyColumnCount + 4);
                int columnCount = keyColumnCount;
                for (int index = 0; index < valueColumnCount; ++index) {
                    const auto& value = logRow[index * 2 + keyColumnCount + 5];
                    Y_ASSERT(value.Type == EValueType::Uint64);
                    auto flags = static_cast<EReplicationLogDataFlags>(value.Data.Uint64);
                    if (None(flags & EReplicationLogDataFlags::Missing)) {
                        ++columnCount;
                    }
                }
                auto row = rowBuffer->Allocate(columnCount);
                int currentIndex = 0;
                for (int index = 0; index < keyColumnCount; ++index) {
                    auto value = rowBuffer->Capture(logRow[index + 4]);
                    value.Id = index;
                    row[currentIndex++] = value; 
                }
                for (int index = 0; index < valueColumnCount; ++index) {
                    const auto& flagsValue  = logRow[index * 2 + keyColumnCount + 5];
                    Y_ASSERT(flagsValue.Type == EValueType::Uint64);
                    auto flags = static_cast<EReplicationLogDataFlags>(flagsValue.Data.Uint64);
                    if (None(flags & EReplicationLogDataFlags::Missing)) {
                        auto dataValue = rowBuffer->Capture(logRow[index * 2 + keyColumnCount + 4]);\
                        dataValue.Id = index + keyColumnCount;
                        row[currentIndex++] = dataValue;
                    }
                }
                modification.Type = ERowModificationType::Write;
                modification.Row = row;
                LOG_DEBUG_IF(MountConfig_->EnableReplicationLogging, "Replicating write (Row: %v)", row);
                break;
            }

            case ERowModificationType::Delete: {
                auto key = rowBuffer->Allocate(keyColumnCount);
                for (int index = 0; index < keyColumnCount; ++index) {
                    auto value = rowBuffer->Capture(logRow[index + 4]);
                    value.Id = index;
                    key[index] = value;
                }
                modification.Type = ERowModificationType::Delete;
                modification.Row = key;
                LOG_DEBUG_IF(MountConfig_->EnableReplicationLogging, "Replicating delete (Key: %v)", key);
                break;
            }

            default:
                Y_UNREACHABLE();
        }
        modifications->push_back(modification);
    }

    static TOwningKey MakeLowerBound(i64 startRowIndex)
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(-1, 0)); // tablet id, fake
        builder.AddValue(MakeUnversionedInt64Value(startRowIndex, 1)); // row index
        return builder.FinishRow();
    }

    static TOwningKey MakeUpperBound()
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(-1, 0)); // tablet id, fake
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max, 1)); // row index
        return builder.FinishRow();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTableReplicator::TTableReplicator(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    TTableReplicaInfo* replicaInfo,
    TClusterDirectoryPtr clusterDirectory,
    INativeConnectionPtr localConnection,
    TTabletSlotPtr slot,
    TSlotManagerPtr slotManager,
    IInvokerPtr workerInvoker)
    : Impl_(New<TImpl>(
        std::move(config),
        tablet,
        replicaInfo,
        std::move(clusterDirectory),
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
