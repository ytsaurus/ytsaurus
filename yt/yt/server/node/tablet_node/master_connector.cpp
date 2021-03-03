#include "master_connector.h"

#include "private.h"
#include "slot_manager.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>
#include <yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/server/node/cluster_node/master_connector.h>

#include <yt/server/lib/cellar_agent/cellar.h>
#include <yt/server/lib/cellar_agent/cellar_manager.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/tablet_node_tracker_client/tablet_node_tracker_service_proxy.h>

#include <yt/library/profiling/solomon/registry.h>

#include <yt/core/concurrency/async_rw_lock.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/rpc/response_keeper.h>

#include <yt/core/utilex/random.h>

namespace NYT::NTabletNode {

using namespace NCellarAgent;
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
    TMasterConnector(TBootstrap* bootstrap)
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

        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeMasterConnector();
        clusterNodeMasterConnector->SubscribeMasterConnected(BIND(&TMasterConnector::OnMasterConnected, MakeWeak(this)));
        clusterNodeMasterConnector->SubscribeMasterDisconnected(BIND(&TMasterConnector::OnMasterDisconnected, MakeWeak(this)));
        clusterNodeMasterConnector->SubscribePopulateAlerts(BIND(&TMasterConnector::PopulateAlerts, MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));
    }

    virtual void ScheduleHeartbeat(TCellTag cellTag, bool immediately) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Bootstrap_->GetControlInvoker()->Invoke(
            BIND(&TMasterConnector::DoScheduleHeartbeat, MakeWeak(this), cellTag, immediately));
    }

    virtual TReqHeartbeat GetHeartbeatRequest(TCellTag cellTag) const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(NodeId_);

        TReqHeartbeat heartbeatRequest;

        heartbeatRequest.set_node_id(*NodeId_);

        const auto& tabletCellar = Bootstrap_->GetCellarManager()->GetCellar(ECellarType::Tablet);
        {
            auto availableTabletSlotCount = tabletCellar->GetAvailableSlotCount();
            auto usedTabletSlotCount = tabletCellar->GetOccupantCount();
            if (Bootstrap_->IsReadOnly()) {
	        availableTabletSlotCount = 0;
	        usedTabletSlotCount = 0;
            }
            heartbeatRequest.mutable_statistics()->set_available_tablet_slots(availableTabletSlotCount);
            heartbeatRequest.mutable_statistics()->set_used_tablet_slots(usedTabletSlotCount);
        }

        const auto& occupants = tabletCellar->Occupants();

        THashMap<TCellId, int> cellIdToSlotIndex;
        for (int slotIndex = 0; slotIndex < occupants.size(); ++slotIndex) {
            const auto& occupant = occupants[slotIndex];
            auto* protoSlotInfo = heartbeatRequest.add_tablet_slots();

            if (occupant) {
                ToProto(protoSlotInfo->mutable_cell_info(), occupant->GetCellDescriptor().ToInfo());
                protoSlotInfo->set_peer_state(ToProto<int>(occupant->GetControlState()));
                protoSlotInfo->set_peer_id(occupant->GetPeerId());
                auto tabletSlot = occupant->GetTypedOccupier<TTabletSlot>();
                if (tabletSlot) {
                    protoSlotInfo->set_dynamic_config_version(tabletSlot->GetDynamicConfigVersion());
                }
                if (auto responseKeeper = occupant->GetResponseKeeper()) {
                    protoSlotInfo->set_is_response_keeper_warming_up(responseKeeper->IsWarmingUp());
                }

                YT_VERIFY(cellIdToSlotIndex.emplace(occupant->GetCellId(), slotIndex).second);
            } else {
                protoSlotInfo->set_peer_state(ToProto<int>(NHydra::EPeerState::None));
            }
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshots = snapshotStore->GetTabletSnapshots();
        for (const auto& tabletSnapshot : tabletSnapshots) {
            if (CellTagFromId(tabletSnapshot->TabletId) == cellTag) {
                auto* protoTabletInfo = heartbeatRequest.add_tablets();
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

                if (tabletSnapshot->CellId) {
                    auto it = cellIdToSlotIndex.find(tabletSnapshot->CellId);
                    if (it != cellIdToSlotIndex.end()) {
                        auto* protoSlotInfo = heartbeatRequest.mutable_tablet_slots(it->second);
                        protoSlotInfo->set_preload_pending_store_count(protoSlotInfo->preload_pending_store_count() +
                            tabletSnapshot->PreloadPendingStoreCount);
                        protoSlotInfo->set_preload_completed_store_count(protoSlotInfo->preload_completed_store_count() +
                            tabletSnapshot->PreloadCompletedStoreCount);
                        protoSlotInfo->set_preload_failed_store_count(protoSlotInfo->preload_failed_store_count() +
                            tabletSnapshot->PreloadFailedStoreCount);
                    }
                }

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

        return heartbeatRequest;
    }

    virtual void OnHeartbeatResponse(const TRspHeartbeat& response) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& tabletCellar = Bootstrap_->GetCellarManager()->GetCellar(ECellarType::Tablet);
        for (const auto& info : response.tablet_slots_to_remove()) {
            auto cellId = FromProto<TCellId>(info.cell_id());
            YT_VERIFY(cellId);
            auto slot = tabletCellar->FindOccupant(cellId);
            if (!slot) {
                YT_LOG_WARNING("Requested to remove a non-existing slot, ignored (CellId: %v)",
                    cellId);
                continue;
            }
	    YT_LOG_WARNING("Requested to remove cell (CellId: %v)",
	        cellId);
            tabletCellar->RemoveOccupant(slot);
        }

        for (const auto& info : response.tablet_slots_to_create()) {
            auto cellId = FromProto<TCellId>(info.cell_id());
            YT_VERIFY(cellId);
            if (tabletCellar->GetAvailableSlotCount() == 0) {
                YT_LOG_WARNING("Requested to start cell when all slots are used, ignored (CellId: %v)",
                    cellId);
                continue;
            }
            if (tabletCellar->FindOccupant(cellId)) {
                YT_LOG_WARNING("Requested to start cell when this cell is already being served by the node, ignored (CellId: %v)",
                    cellId);
                continue;
            }
	    YT_LOG_DEBUG("Requested to start cell (CellId: %v)",
	        cellId);
            tabletCellar->CreateOccupant(info);
        }

        for (const auto& info : response.tablet_slots_to_configure()) {
            auto descriptor = FromProto<TCellDescriptor>(info.cell_descriptor());
            auto slot = tabletCellar->FindOccupant(descriptor.CellId);
            if (!slot) {
                YT_LOG_WARNING("Requested to configure a non-existing slot, ignored (CellId: %v)",
                    descriptor.CellId);
                continue;
            }
            if (!slot->CanConfigure()) {
                YT_LOG_WARNING("Cannot configure slot in non-configurable state, ignored (CellId: %v, State: %v)",
                    descriptor.CellId,
                    slot->GetControlState());
                continue;
            }
	    YT_LOG_DEBUG("Requested to configure cell (CellId: %v)",
	        descriptor.CellId);
            tabletCellar->ConfigureOccupant(slot, info);
        }

        for (const auto& info : response.tablet_slots_to_update()) {
            auto cellId = FromProto<TCellId>(info.cell_id());
            auto slot = tabletCellar->FindOccupant(cellId);
            if (!slot) {
                YT_LOG_WARNING("Requested to update dynamic options for a non-existing slot, ignored (CellId: %v)",
                    cellId);
                continue;
            }
            if (!slot->CanConfigure()) {
                YT_LOG_WARNING("Cannot update slot in non-configurable state, ignored (CellId: %v, State: %v)",
                    cellId,
                    slot->GetControlState());
                continue;
            }
            auto tabletSlot = slot->GetTypedOccupier<TTabletSlot>();
            if (!tabletSlot) {
                YT_LOG_WARNING("Cannot update slot when occupier is absent, ignored (CellId: %v, State: %v)",
                    cellId,
                    slot->GetControlState());
                continue;
            }
            YT_LOG_DEBUG("Requested to update cell (CellId: %v)",
	        cellId);
            tabletSlot->UpdateDynamicConfig(info);
        }

        UpdateSolomonTags();
    }

private:
    const TBootstrap* const Bootstrap_;

    const TMasterConnectorConfigPtr Config_;

    std::optional<TNodeId> NodeId_;

    IInvokerPtr HeartbeatInvoker_;

    TDuration HeartbeatPeriod_;

    TDuration HeartbeatPeriodSplay_;

    THashMap<TCellTag, std::unique_ptr<TAsyncReaderWriterLock>> HeartbeatLocks_;

    THashMap<TCellTag, int> HeartbeatsScheduled_;

    TError SolomonTagAlert_;

    void PopulateAlerts(std::vector<TError>* alerts) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!SolomonTagAlert_.IsOK()) {
            alerts->push_back(SolomonTagAlert_);
        }
    }

    void OnMasterConnected(TNodeId nodeId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(!NodeId_);
        NodeId_ = nodeId;

        HeartbeatInvoker_ = Bootstrap_->GetClusterNodeMasterConnector()->GetMasterConnectionInvoker();

        if (Bootstrap_->GetClusterNodeMasterConnector()->UseNewHeartbeats()) {
            StartHeartbeats();
        }
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatsScheduled_.clear();

        NodeId_ = std::nullopt;
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

        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeMasterConnector();
        for (auto cellTag : clusterNodeMasterConnector->GetMasterCellTags()) {
            DoScheduleHeartbeat(cellTag, /* immediately */ true);
        }
    }

    void DoScheduleHeartbeat(TCellTag cellTag, bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ++HeartbeatsScheduled_[cellTag];

        auto delay = immediately ? TDuration::Zero() : HeartbeatPeriod_ + RandomDuration(HeartbeatPeriodSplay_);
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::ReportHeartbeat, MakeWeak(this), cellTag),
            delay,
            HeartbeatInvoker_);
    }

    void ReportHeartbeat(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TAsyncReaderWriterLock* heartbeatsLock;
        if (auto it = HeartbeatLocks_.find(cellTag); it != HeartbeatLocks_.end()) {
            heartbeatsLock = it->second.get();
        } else {
            auto lock = std::make_unique<TAsyncReaderWriterLock>();
            heartbeatsLock = lock.get();
            YT_VERIFY(HeartbeatLocks_.emplace(cellTag, std::move(lock)).second);
        }

        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(heartbeatsLock))
            .ValueOrThrow();

        --HeartbeatsScheduled_[cellTag];

        auto masterChannel = Bootstrap_->GetClusterNodeMasterConnector()->GetMasterChannel(cellTag);
        TTabletNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.Heartbeat();
        req->SetTimeout(Config_->HeartbeatTimeout);

        static_cast<TReqHeartbeat&>(*req) = GetHeartbeatRequest(cellTag);

        YT_LOG_INFO("Sending tablet node heartbeat to master (CellTag: %v, %v)",
            cellTag,
            req->statistics());

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            OnHeartbeatResponse(*rspOrError.Value());

            YT_LOG_INFO("Successfully reported tablet node heartbeat to master (CellTag: %v)",
                cellTag);

            // Schedule next heartbeat if no more heartbeats are scheduled.
            if (HeartbeatsScheduled_[cellTag] == 0) {
                DoScheduleHeartbeat(cellTag, /* immediately */ false);
            }
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting tablet node heartbeat to master (CellTag: %v)",
                cellTag);
            if (IsRetriableError(rspOrError)) {
                DoScheduleHeartbeat(cellTag, /* immediately */ false);
            } else {
                Bootstrap_->GetClusterNodeMasterConnector()->ResetAndRegisterAtMaster();
            }
        }
    }

    void UpdateSolomonTags()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        THashSet<TString> seenTags;
        const auto& tabletCellar = Bootstrap_->GetCellarManager()->GetCellar(ECellarType::Tablet);
        for (const auto& occupant : tabletCellar->Occupants()) {
            if (!occupant) {
                continue;
            }

            auto slot = occupant->GetTypedOccupier<TTabletSlot>();
            if (!slot) {
                continue;
            }

            std::optional<TString> tag;

            auto dynamicConfig = slot->GetDynamicOptions();
            if (dynamicConfig) {
                tag = dynamicConfig->SolomonTag;
            }

            if (!tag) {
                tag = slot->GetTabletCellBundleName();
            }

            seenTags.insert(*tag);
        }

        if (seenTags.size() == 0) {
            NProfiling::TSolomonRegistry::Get()->SetDynamicTags({});
            SolomonTagAlert_ = TError();
        } else if (seenTags.size() == 1) {
            NProfiling::TSolomonRegistry::Get()->SetDynamicTags({
                {"tablet_cell_bundle", *seenTags.begin()}
            });
            SolomonTagAlert_ = TError();
        } else {
            NProfiling::TSolomonRegistry::Get()->SetDynamicTags({});
            SolomonTagAlert_ = TError("Conflicting Solomon tags")
                << TErrorAttribute("tags", seenTags);
        }
    }

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(TBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
