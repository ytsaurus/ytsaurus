#include "table_puller.h"

#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"
#include "private.h"

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NChaosClient;
using namespace NObjectClient;
using namespace NProfiling;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const int TabletRowsPerRead = 1000;
static const auto HardErrorAttribute = TErrorAttribute("hard", true);

////////////////////////////////////////////////////////////////////////////////

class TTablePuller
    : public ITablePuller
{
public:
    TTablePuller(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        NNative::IConnectionPtr localConnection,
        ITabletSlotPtr slot,
        ITabletSnapshotStorePtr tabletSnapshotStore,
        IInvokerPtr workerInvoker,
        IThroughputThrottlerPtr nodeInThrottler)
        : Config_(std::move(config))
        , LocalConnection_(std::move(localConnection))
        , Slot_(std::move(slot))
        , TabletSnapshotStore_(std::move(tabletSnapshotStore))
        , WorkerInvoker_(std::move(workerInvoker))
        , Tablet_(tablet)
        , TabletId_(tablet->GetId())
        , MountRevision_(tablet->GetMountRevision())
        , TableSchema_(tablet->GetTableSchema())
        , NameTable_(TNameTable::FromSchema(*TableSchema_))
        , MountConfig_(tablet->GetSettings().MountConfig)
        , ReplicaId_(tablet->GetUpstreamReplicaId())
        , Logger(TabletNodeLogger.WithTag("%v",
            tablet->GetLoggingTag()))
        , Throttler_(CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            std::move(nodeInThrottler),
            CreateReconfigurableThroughputThrottler(MountConfig_->ReplicationThrottler, Logger)
        }))
    { }

    void Enable() override
    {
        Disable();

        FiberFuture_ = BIND(&TTablePuller::FiberMain, MakeWeak(this))
            .AsyncVia(Slot_->GetHydraManager()->GetAutomatonCancelableContext()->CreateInvoker(WorkerInvoker_))
            .Run();

        YT_LOG_INFO("Puller fiber started");
    }

    void Disable() override
    {
        if (FiberFuture_) {
            FiberFuture_.Cancel(TError("Puller disabled"));
            YT_LOG_INFO("Puller fiber stopped");
        }
        FiberFuture_.Reset();
    }

private:
    const TTabletManagerConfigPtr Config_;
    const NNative::IConnectionPtr LocalConnection_;
    const ITabletSlotPtr Slot_;
    const ITabletSnapshotStorePtr TabletSnapshotStore_;
    const IInvokerPtr WorkerInvoker_;

    const TTablet* Tablet_;
    const TTabletId TabletId_;
    const TRevision MountRevision_;
    const TTableSchemaPtr TableSchema_;
    const TNameTablePtr NameTable_;
    const TTableMountConfigPtr MountConfig_;
    const TReplicaId ReplicaId_;

    const NLogging::TLogger Logger;

    const IThroughputThrottlerPtr NodeInThrottler_;
    const IThroughputThrottlerPtr Throttler_;

    TFuture<void> FiberFuture_;

    void FiberMain()
    {
        while (true) {
            NProfiling::TWallTimer timer;
            FiberIteration();
            TDelayedExecutor::WaitForDuration(MountConfig_->ReplicationTickPeriod - timer.GetElapsedTime());
        }
    }

    void FiberIteration()
    {
        TTabletSnapshotPtr tabletSnapshot;

        try {
            tabletSnapshot = TabletSnapshotStore_->FindTabletSnapshot(TabletId_, MountRevision_);
            if (!tabletSnapshot) {
                THROW_ERROR_EXCEPTION("No tablet snapshot is available")
                    << HardErrorAttribute;
            }

            if (!Tablet_->ReplicationCard()) {
                THROW_ERROR_EXCEPTION("No replication card")
                    << HardErrorAttribute;
            }

            auto counters = tabletSnapshot->TableProfiler->GetTablePullerCounters();
            auto countError = Finally([&] {
                if (std::uncaught_exception()) {
                    counters.ErrorCount.Increment();
                }
            });

            auto replicationCard = Tablet_->ReplicationCard();
            EReplicaMode mode;
            EReplicaState state;
            bool foundSelf = false;
            TString cluster;
            TYPath tablePath;
            TReplicaId upstreamReplicaId;

            YT_LOG_DEBUG("Identifying self replica mode (Replicas: %v)",
                MakeFormattableView(replicationCard->Replicas,
                    [](TStringBuilderBase* builder, const TReplicaInfo& replicaInfo) {
                        builder->AppendFormat("(%v,%v,%v,%v)", replicaInfo.Cluster, replicaInfo.TablePath,
                            replicaInfo.Mode, replicaInfo.ContentType);
                    }));

            for (const auto& replica : replicationCard->Replicas) {
                if (replica.ReplicaId == Tablet_->GetUpstreamReplicaId()) {
                    mode = replica.Mode;
                    state = replica.State;
                    foundSelf = true;
                } else if (replica.ContentType == EReplicaContentType::Queue && replica.Mode == EReplicaMode::Sync) {
                    cluster = replica.Cluster;
                    tablePath = replica.TablePath;
                    upstreamReplicaId = replica.ReplicaId;
                }
            }

            if (!foundSelf) {
                YT_LOG_DEBUG("Will not pull rows since replication card does not contain us");
                return;
            }

            if (mode != EReplicaMode::Async) {
                YT_LOG_DEBUG("Will not pull rows since replica is not async (ReplicaMode: %Qlv)",
                    mode);
                return;
            }

            // TODO(savrus) Check if replica is enabled.
            if (state == EReplicaState::Disabled) {
                YT_LOG_DEBUG("Will not pull rows since replica is disable (ReplicaState: %Qlv)",
                    state);
                return;
            }

            if (!tablePath) {
                YT_LOG_DEBUG("Will not pull rows since no in-sync queue found");
                return;
            }

            struct TPullRowsOutputBufferTag { };
            std::vector<TVersionedRow> resultRows;
            auto outputRowBuffer = New<TRowBuffer>(TPullRowsOutputBufferTag());
            auto replicationRound = Tablet_->GetReplicationRound();
            TReplicationProgress progress;
            std::vector<std::pair<TTabletId, i64>> endReplicationRowIndexes;
            i64 rowCount = 0;
            i64 dataWeight = 0;

            {
                TEventTimerGuard timerGuard(counters.PullRowsTime);

                auto foreignConnection = LocalConnection_->GetClusterDirectory()->FindConnection(cluster);
                auto foreignClient = foreignConnection->CreateClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));

                TPullRowsOptions options;
                options.TabletRowsPerRead = TabletRowsPerRead;
                options.ReplicationProgress = Tablet_->ReplicationProgress();
                options.ReplicationProgress.UpperKey = Tablet_->GetNextPivotKey();
                options.StartReplicationRowIndexes = Tablet_->CurrentReplicationRowIndexes();

                // TODO(savrus): Set timestamp limit if we are trying to catch up.
                //options.TimestampLimit = replicationCard->EraStartTimestamp;

                YT_LOG_DEBUG("Pulling rows (Cluster: %v, Path: %v, ReplicationProgress: %v, ReplicationRowIndexes: %v)",
                    cluster,
                    tablePath,
                    options.ReplicationProgress,
                    options.StartReplicationRowIndexes);


                options.UpstreamReplicaId = upstreamReplicaId;
                auto pullResult = WaitFor(foreignClient->PullRows(tablePath, options))
                    .ValueOrThrow();

                for (auto result : pullResult.ResultPerTablet) {
                    NTableClient::TWireProtocolReader reader(result.Data, outputRowBuffer);
                    const auto schema = Tablet_->GetPhysicalSchema();
                    auto resultSchemaData = TWireProtocolReader::GetSchemaData(*schema, TColumnFilter());

                    while (!reader.IsFinished()) {
                        auto row = reader.ReadVersionedRow(resultSchemaData, true);
                        resultRows.push_back(row);
                    }

                    for (const auto& segment : result.ReplicationProgress.Segments) {
                        progress.Segments.push_back({segment.LowerKey, segment.Timestamp});
                    }

                    endReplicationRowIndexes.emplace_back(result.TabletId, result.EndReplicationRowIndex);

                    rowCount += result.RowCount;
                    dataWeight += result.DataWeight;
                }

                progress.UpperKey = pullResult.ResultPerTablet.back().ReplicationProgress.UpperKey;

                YT_LOG_DEBUG("Pulled rows (RowCount: %v, DataWeight: %v, NewProgress: %v, EndReplictionRowIndexes: %v)",
                    rowCount,
                    dataWeight,
                    progress,
                    endReplicationRowIndexes);

                if (resultRows.empty()) {
                    // TODO(savrus): Always commit transaction to update replication progress (at least if we are trying to catch up).
                    return;
                }
            }

            {
                TEventTimerGuard timerGuard(counters.WriteTime);

                auto localClient = LocalConnection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::ReplicatorUserName));
                auto localTransaction = WaitFor(localClient->StartNativeTransaction(ETransactionType::Tablet))
                    .ValueOrThrow();

                // Set options to avoid nested writes to other replicas.
                TModifyRowsOptions modifyOptions;
                modifyOptions.ReplicationCard = Tablet_->ReplicationCard();
                modifyOptions.UpstreamReplicaId = Tablet_->GetUpstreamReplicaId();
                modifyOptions.TopmostTransaction = false;

                // TODO(savrus) Use wire protocol data directly.
                localTransaction->WriteRows(
                    FromObjectId(Tablet_->GetTableId()),
                    NameTable_,
                    MakeSharedRange(std::move(resultRows), std::move(outputRowBuffer)),
                    modifyOptions);

                {
                    NProto::TReqWritePulledRows req;
                    ToProto(req.mutable_tablet_id(), TabletId_);
                    req.set_replication_round(replicationRound);
                    ToProto(req.mutable_new_replication_progress(), progress);
                    for (const auto [tabletId, endReplicationRowIndex] : endReplicationRowIndexes) {
                        auto protoEndReplicationRowIndex = req.add_new_replication_row_indexes();
                        ToProto(protoEndReplicationRowIndex->mutable_tablet_id(), tabletId);
                        protoEndReplicationRowIndex->set_replication_row_index(endReplicationRowIndex);
                    }
                    localTransaction->AddAction(Slot_->GetCellId(), MakeTransactionActionData(req));
                }

                YT_LOG_DEBUG("Commiting pull rows write transaction (TransactionId: %v)",
                    localTransaction->GetId());

                // NB: 2PC is used here to correctly process transaction signatures (sent by both rows and actions).
                // TODO(savrus) Discard 2PC.
                TTransactionCommitOptions commitOptions;
                commitOptions.CoordinatorCellId = Slot_->GetCellId();
                commitOptions.Force2PC = true;
                WaitFor(localTransaction->Commit(commitOptions))
                    .ThrowOnError();

                YT_LOG_DEBUG("Pull rows write transaction committed (TransactionId: %v)",
                    localTransaction->GetId());
            }

            counters.RowCount.Increment(rowCount);
            counters.DataWeight.Increment(dataWeight);
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Pull);
            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Pull].Store(error);
            YT_LOG_ERROR(error, "Error pulling rows, backing off");

            if (error.Attributes().Get<bool>("hard", false)) {
                DoHardBackoff(error);
            } else {
                DoSoftBackoff(error);
            }
        }
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
};

////////////////////////////////////////////////////////////////////////////////

ITablePullerPtr CreateTablePuller(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    NNative::IConnectionPtr localConnection,
    ITabletSlotPtr slot,
    ITabletSnapshotStorePtr tabletSnapshotStore,
    IInvokerPtr workerInvoker,
    IThroughputThrottlerPtr nodeInThrottler)
{
    return New<TTablePuller>(
        std::move(config),
        tablet,
        std::move(localConnection),
        std::move(slot),
        std::move(tabletSnapshotStore),
        std::move(workerInvoker),
        std::move(nodeInThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
