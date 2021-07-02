#include "master_connector.h"

#include "bootstrap.h"
#include "private.h"
#include "slot_manager.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/cellar_node/master_connector.h>

#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/tablet_node_tracker_client/tablet_node_tracker_service_proxy.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NTabletNode {

using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTabletNodeTrackerClient;
using namespace NTabletNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
{
public:
    TMasterConnector(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->TabletNode->MasterConnector)
        , HeartbeatPeriod_(Config_->HeartbeatPeriod)
        , HeartbeatPeriodSplay_(Config_->HeartbeatPeriodSplay)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }

    virtual void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Bootstrap_->SubscribeMasterConnected(BIND(&TMasterConnector::OnMasterConnected, MakeWeak(this)));

        const auto& cellarNodeMasterConnector = Bootstrap_->GetCellarNodeMasterConnector();
        cellarNodeMasterConnector->SubscribeHeartbeatRequested(BIND(&TMasterConnector::OnCellarNodeHeartbeatRequested, MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));
    }

    virtual TReqHeartbeat GetHeartbeatRequest(TCellTag cellTag) const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(Bootstrap_->IsConnected());

        TReqHeartbeat heartbeatRequest;
        heartbeatRequest.set_node_id(Bootstrap_->GetNodeId());
        AddTabletInfoToHeartbeatRequest(cellTag, &heartbeatRequest);

        return heartbeatRequest;
    }

    virtual void OnHeartbeatResponse(const TRspHeartbeat& /*response*/) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }

private:
    IBootstrap* const Bootstrap_;
    const TMasterConnectorConfigPtr Config_;

    IInvokerPtr HeartbeatInvoker_;
    TDuration HeartbeatPeriod_;
    TDuration HeartbeatPeriodSplay_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void OnMasterConnected(TNodeId /*nodeId*/)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatInvoker_ = Bootstrap_->GetMasterConnectionInvoker();

        if (Bootstrap_->UseNewHeartbeats()) {
            StartHeartbeats();
        }
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatPeriod_ = newNodeConfig->TabletNode->MasterConnector->HeartbeatPeriod.value_or(Config_->HeartbeatPeriod);
        HeartbeatPeriodSplay_ = newNodeConfig->TabletNode->MasterConnector->HeartbeatPeriodSplay.value_or(Config_->HeartbeatPeriodSplay);
    }

    void StartHeartbeats()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting tablet node heartbeats");

        for (auto cellTag : Bootstrap_->GetMasterCellTags()) {
            DoScheduleHeartbeat(cellTag, /* immediately */ true);
        }
    }

    void DoScheduleHeartbeat(TCellTag cellTag, bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto delay = immediately ? TDuration::Zero() : HeartbeatPeriod_ + RandomDuration(HeartbeatPeriodSplay_);
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::ReportHeartbeat, MakeWeak(this), cellTag),
            delay,
            HeartbeatInvoker_);
    }

    void ReportHeartbeat(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TTabletNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.Heartbeat();
        req->SetTimeout(Config_->HeartbeatTimeout);

        static_cast<TReqHeartbeat&>(*req) = GetHeartbeatRequest(cellTag);

        YT_LOG_INFO("Sending tablet node heartbeat to master (CellTag: %v)",
            cellTag);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            OnHeartbeatResponse(*rspOrError.Value());

            YT_LOG_INFO("Successfully reported tablet node heartbeat to master (CellTag: %v)",
                cellTag);

            // Schedule next heartbeat.
            DoScheduleHeartbeat(cellTag, /* immediately */ false);
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting tablet node heartbeat to master (CellTag: %v)",
                cellTag);
            if (IsRetriableError(rspOrError)) {
                DoScheduleHeartbeat(cellTag, /* immediately */ false);
            } else {
                Bootstrap_->ResetAndRegisterAtMaster();
            }
        }
    }

    void AddTabletInfoToHeartbeatRequest(TCellTag cellTag, TReqHeartbeat* heartbeatRequest) const
    {
        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshots = snapshotStore->GetTabletSnapshots();

        for (const auto& tabletSnapshot : tabletSnapshots) {
            if (CellTagFromId(tabletSnapshot->TabletId) == cellTag) {
                auto* protoTabletInfo = heartbeatRequest->add_tablets();
                ToProto(protoTabletInfo->mutable_tablet_id(), tabletSnapshot->TabletId);
                protoTabletInfo->set_mount_revision(tabletSnapshot->MountRevision);

                auto* protoTabletStatistics = protoTabletInfo->mutable_statistics();
                protoTabletStatistics->set_partition_count(tabletSnapshot->PartitionList.size());
                protoTabletStatistics->set_store_count(tabletSnapshot->StoreCount);
                protoTabletStatistics->set_preload_pending_store_count(tabletSnapshot->PreloadPendingStoreCount);
                protoTabletStatistics->set_preload_completed_store_count(tabletSnapshot->PreloadCompletedStoreCount);
                protoTabletStatistics->set_preload_failed_store_count(tabletSnapshot->PreloadFailedStoreCount);
                protoTabletStatistics->set_overlapping_store_count(tabletSnapshot->OverlappingStoreCount);
                protoTabletStatistics->set_last_commit_timestamp(tabletSnapshot->TabletRuntimeData->LastCommitTimestamp);
                protoTabletStatistics->set_last_write_timestamp(tabletSnapshot->TabletRuntimeData->LastWriteTimestamp);
                protoTabletStatistics->set_unflushed_timestamp(tabletSnapshot->TabletRuntimeData->UnflushedTimestamp);
                i64 totalDynamicMemoryUsage = 0;
                for (auto type : TEnumTraits<ETabletDynamicMemoryType>::GetDomainValues()) {
                    totalDynamicMemoryUsage += tabletSnapshot->TabletRuntimeData->DynamicMemoryUsagePerType[type].load();
                }
                protoTabletStatistics->set_dynamic_memory_pool_size(totalDynamicMemoryUsage);
                protoTabletStatistics->set_modification_time(ToProto<ui64>(tabletSnapshot->TabletRuntimeData->ModificationTime));
                protoTabletStatistics->set_access_time(ToProto<ui64>(tabletSnapshot->TabletRuntimeData->AccessTime));

                int tabletErrorCount = 0;

                for (auto key : TEnumTraits<ETabletBackgroundActivity>::GetDomainValues()) {
                    auto error = tabletSnapshot->TabletRuntimeData->Errors[key].Load();
                    if (!error.IsOK()) {
                        ++tabletErrorCount;
                    }
                }

                protoTabletInfo->set_error_count(tabletErrorCount);

                for (const auto& [replicaId, replicaSnapshot] : tabletSnapshot->Replicas) {
                    auto* protoReplicaInfo = protoTabletInfo->add_replicas();
                    ToProto(protoReplicaInfo->mutable_replica_id(), replicaId);
                    replicaSnapshot->RuntimeData->Populate(protoReplicaInfo->mutable_statistics());

                    auto error = replicaSnapshot->RuntimeData->Error.Load();
                    if (!error.IsOK()) {
                        protoReplicaInfo->set_has_error(true);
                    }
                }

                auto* protoPerformanceCounters = protoTabletInfo->mutable_performance_counters();
                auto performanceCounters = tabletSnapshot->PerformanceCounters;
                protoPerformanceCounters->set_dynamic_row_read_count(performanceCounters->DynamicRowReadCount);
                protoPerformanceCounters->set_dynamic_row_read_data_weight_count(performanceCounters->DynamicRowReadDataWeightCount);
                protoPerformanceCounters->set_dynamic_row_lookup_count(performanceCounters->DynamicRowLookupCount);
                protoPerformanceCounters->set_dynamic_row_lookup_data_weight_count(performanceCounters->DynamicRowLookupDataWeightCount);
                protoPerformanceCounters->set_dynamic_row_write_count(performanceCounters->DynamicRowWriteCount);
                protoPerformanceCounters->set_dynamic_row_write_data_weight_count(performanceCounters->DynamicRowWriteDataWeightCount);
                protoPerformanceCounters->set_dynamic_row_delete_count(performanceCounters->DynamicRowDeleteCount);
                protoPerformanceCounters->set_static_chunk_row_read_count(performanceCounters->StaticChunkRowReadCount);
                protoPerformanceCounters->set_static_chunk_row_read_data_weight_count(performanceCounters->StaticChunkRowReadDataWeightCount);
                protoPerformanceCounters->set_static_chunk_row_lookup_count(performanceCounters->StaticChunkRowLookupCount);
                protoPerformanceCounters->set_static_chunk_row_lookup_true_negative_count(performanceCounters->StaticChunkRowLookupTrueNegativeCount);
                protoPerformanceCounters->set_static_chunk_row_lookup_false_positive_count(performanceCounters->StaticChunkRowLookupFalsePositiveCount);
                protoPerformanceCounters->set_static_chunk_row_lookup_data_weight_count(performanceCounters->StaticChunkRowLookupDataWeightCount);
                protoPerformanceCounters->set_unmerged_row_read_count(performanceCounters->UnmergedRowReadCount);
                protoPerformanceCounters->set_merged_row_read_count(performanceCounters->MergedRowReadCount);
                protoPerformanceCounters->set_compaction_data_weight_count(performanceCounters->CompactionDataWeightCount);
                protoPerformanceCounters->set_partitioning_data_weight_count(performanceCounters->PartitioningDataWeightCount);
                protoPerformanceCounters->set_lookup_error_count(performanceCounters->LookupErrorCount);
                protoPerformanceCounters->set_write_error_count(performanceCounters->WriteErrorCount);
            }
        }

    }

    void OnCellarNodeHeartbeatRequested(
        ECellarType cellarType,
        const ICellarPtr& tabletCellar,
        NCellarNodeTrackerClient::NProto::TReqCellarHeartbeat* heartbeatRequest)
    {
        if (cellarType != ECellarType::Tablet) {
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshots = snapshotStore->GetTabletSnapshots();
        for (const auto& tabletSnapshot : tabletSnapshots) {
            if (tabletSnapshot->CellId) {
                if (const auto& occupant = tabletCellar->FindOccupant(tabletSnapshot->CellId)) {
                    auto* protoSlotInfo = heartbeatRequest->mutable_cell_slots(occupant->GetIndex());
                    protoSlotInfo->set_preload_pending_store_count(protoSlotInfo->preload_pending_store_count() +
                        tabletSnapshot->PreloadPendingStoreCount);
                    protoSlotInfo->set_preload_completed_store_count(protoSlotInfo->preload_completed_store_count() +
                        tabletSnapshot->PreloadCompletedStoreCount);
                    protoSlotInfo->set_preload_failed_store_count(protoSlotInfo->preload_failed_store_count() +
                        tabletSnapshot->PreloadFailedStoreCount);
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
