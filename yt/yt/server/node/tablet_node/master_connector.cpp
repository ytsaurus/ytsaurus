#include "master_connector.h"

#include "bootstrap.h"
#include "private.h"
#include "slot_manager.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/master_heartbeat_reporter_base.h>

#include <yt/yt/server/node/cellar_node/master_connector.h>

#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/performance_counters.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/table_client/performance_counters.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/tablet_node_tracker_client/tablet_node_tracker_service_proxy.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NTabletNode {

using namespace NApi::NNative;
using namespace NCellMasterClient;
using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTabletNodeTrackerClient;
using namespace NTabletNodeTrackerClient::NProto;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
    , public TMasterHeartbeatReporterBase
{
public:
    TMasterConnector(IBootstrap* bootstrap)
        : TMasterHeartbeatReporterBase(
            bootstrap,
            /*reportHeartbeatsToAllSecondaryMasters*/ true,
            TabletNodeLogger().WithTag("HeartbeatType: %v", ENodeHeartbeatType::Tablet))
        , Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->TabletNode->MasterConnector)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);
    }

    void Initialize() override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        TMasterHeartbeatReporterBase::Initialize();

        Bootstrap_->SubscribeMasterConnected(BIND_NO_PROPAGATE(&TMasterConnector::OnMasterConnected, MakeWeak(this)));

        const auto& cellarNodeMasterConnector = Bootstrap_->GetCellarNodeMasterConnector();
        cellarNodeMasterConnector->SubscribeHeartbeatRequested(BIND_NO_PROPAGATE(&TMasterConnector::OnCellarNodeHeartbeatRequested, MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));

        Reconfigure(GetDynamicConfig()->HeartbeatExecutor.value_or(Config_->HeartbeatExecutor));
    }

    TTabletNodeTrackerServiceProxy::TReqHeartbeatPtr BuildHeartbeatRequest(TCellTag cellTag) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(Bootstrap_->IsConnected());

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TTabletNodeTrackerServiceProxy proxy(std::move(masterChannel));

        auto heartbeatRequest = proxy.Heartbeat();
        heartbeatRequest->SetTimeout(GetDynamicConfig()->HeartbeatTimeout);

        heartbeatRequest->set_node_id(ToProto(Bootstrap_->GetNodeId()));
        AddTabletInfoToHeartbeatRequest(cellTag, heartbeatRequest);

        return heartbeatRequest;
    }

protected:
    TFuture<void> DoReportHeartbeat(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (!Bootstrap_->IsConnected()) {
            return MakeFuture(TError("Node disconnected"));
        }

        auto req = BuildHeartbeatRequest(cellTag);
        auto rspFuture = req->Invoke();
        EmplaceOrCrash(CellTagToHeartbeatRspFuture_, cellTag, rspFuture);
        return rspFuture.AsVoid();
    }

    void OnHeartbeatSucceeded(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto rspOrError = GetHeartbeatResponseOrError(cellTag);
        YT_VERIFY(rspOrError.IsOK());
    }

    void OnHeartbeatFailed(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto rspOrError = GetHeartbeatResponseOrError(cellTag);
        YT_VERIFY(!rspOrError.IsOK());
    }

    void ResetState(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        CellTagToHeartbeatRspFuture_.erase(cellTag);
    }

private:
    IBootstrap* const Bootstrap_;
    const TMasterConnectorConfigPtr Config_;
    THashMap<TCellTag, TFuture<TTabletNodeTrackerServiceProxy::TRspHeartbeatPtr>> CellTagToHeartbeatRspFuture_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void OnMasterConnected(TNodeId /*nodeId*/)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        StartNodeHeartbeats();
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        Reconfigure(newNodeConfig->TabletNode->MasterConnector->HeartbeatExecutor.value_or(Config_->HeartbeatExecutor));

        YT_LOG_INFO("Dynamic config changed");
    }

    void AddTabletInfoToHeartbeatRequest(TCellTag cellTag, TTabletNodeTrackerServiceProxy::TReqHeartbeatPtr heartbeatRequest) const
    {
        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshots = snapshotStore->GetTabletSnapshots();

        for (const auto& tabletSnapshot : tabletSnapshots) {
            if (CellTagFromId(tabletSnapshot->TabletId) == cellTag) {
                auto* protoTabletInfo = heartbeatRequest->add_tablets();
                ToProto(protoTabletInfo->mutable_tablet_id(), tabletSnapshot->TabletId);
                protoTabletInfo->set_mount_revision(ToProto(tabletSnapshot->MountRevision));

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
                protoTabletStatistics->set_modification_time(ToProto(tabletSnapshot->TabletRuntimeData->ModificationTime.load()));
                protoTabletStatistics->set_access_time(ToProto(tabletSnapshot->TabletRuntimeData->AccessTime.load()));

                int tabletErrorCount = 0;
                tabletSnapshot->TabletRuntimeData->Errors.ForEachError([&tabletErrorCount] (const TError& error) {
                    if (!error.IsOK()) {
                        ++tabletErrorCount;
                    }
                });
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
                #define XX(name, Name) protoPerformanceCounters->set_##name##_count( \
                    performanceCounters->Name.Counter.load(std::memory_order::relaxed));
                ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
                #undef XX
            }
        }
    }

    static bool IsPeerHealthy(NHydra::EPeerState state)
    {
        return state == EPeerState::Leading ||
            state == EPeerState::Following;
    }

    void OnCellarNodeHeartbeatRequested(
        ECellarType cellarType,
        const ICellarPtr& tabletCellar,
        NCellarNodeTrackerClient::NProto::TReqCellarHeartbeat* heartbeatRequest)
    {
        if (cellarType != ECellarType::Tablet) {
            return;
        }

        THashSet<NHydra::TCellId> notReadyCellIds;

        for (auto occupant : tabletCellar->Occupants()) {
            if (!occupant) {
                continue;
            }

            if (auto tabletSlot = occupant->GetTypedOccupier<ITabletSlot>()) {
                bool snapshotsReady = tabletSlot->IsTabletEpochActive();
                auto* protoSlotInfo = heartbeatRequest->mutable_cell_slots(occupant->GetIndex());

                // Signaling master that store preload is not completed yet
                // (actually it did not even started).
                if (IsPeerHealthy(FromProto<NHydra::EPeerState>(protoSlotInfo->peer_state())) && !snapshotsReady) {
                    notReadyCellIds.insert(occupant->GetCellId());
                    constexpr int PreloadPendingStoreSentinel = 1;
                    protoSlotInfo->set_preload_pending_store_count(PreloadPendingStoreSentinel);
                }
            }
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshots = snapshotStore->GetTabletSnapshots();
        for (const auto& tabletSnapshot : tabletSnapshots) {
            auto cellId = tabletSnapshot->CellId;
            if (!cellId || notReadyCellIds.contains(cellId)) {
                continue;
            }
            if (const auto& occupant = tabletCellar->FindOccupant(cellId)) {
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

    TMasterConnectorDynamicConfigPtr GetDynamicConfig() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetDynamicConfigManager()->GetConfig()->TabletNode->MasterConnector;
    }

    TErrorOr<TTabletNodeTrackerServiceProxy::TRspHeartbeatPtr> GetHeartbeatResponseOrError(TCellTag cellTag)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto futureIt = GetIteratorOrCrash(CellTagToHeartbeatRspFuture_, cellTag);
        auto future = std::move(futureIt->second);
        CellTagToHeartbeatRspFuture_.erase(futureIt);
        YT_VERIFY(future.IsSet());

        return future.Get();
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
