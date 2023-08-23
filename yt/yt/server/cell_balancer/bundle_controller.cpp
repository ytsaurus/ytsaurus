#include "bundle_controller.h"
#include "bundle_scheduler.h"

#include "bootstrap.h"
#include "config.h"
#include "cypress_bindings.h"
#include "orchid_bindings.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NCellBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NProfiling;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BundleControllerLogger;
static const TYPath TabletCellBundlesPath("//sys/tablet_cell_bundles");
static const TYPath TabletCellBundlesDynamicConfigPath("//sys/tablet_cell_bundles/@config");
static const TYPath TabletNodesPath("//sys/tablet_nodes");
static const TYPath TabletCellsPath("//sys/tablet_cells");
static const TYPath RpcProxiesPath("//sys/rpc_proxies");
static const TYPath BundleSystemQuotasPath("//sys/account_tree/bundle_system_quotas");

static const TString SensorTagInstanceSize = "instance_size";

////////////////////////////////////////////////////////////////////////////////

struct TBundleSensors final
{
    TProfiler Profiler;

    TGauge CpuAllocated;
    TGauge CpuAlive;
    TGauge CpuQuota;

    TGauge MemoryAllocated;
    TGauge MemoryAlive;
    TGauge MemoryQuota;

    TGauge WriteThreadPoolSize;
    TGauge TabletDynamicSize;
    TGauge TabletStaticSize;
    TGauge CompressedBlockCacheSize;
    TGauge UncompressedBlockCacheSize;
    TGauge KeyFilterBlockCacheSize;
    TGauge VersionedChunkMetaSize;
    TGauge LookupRowCacheSize;

    THashMap<TString, TGauge> AliveNodesBySize;
    THashMap<TString, TGauge> AliveProxiesBySize;

    THashMap<TString, TGauge> AllocatedNodesBySize;
    THashMap<TString, TGauge> AllocatedProxiesBySize;

    THashMap<TString, TGauge> TargetTabletNodeSize;
    THashMap<TString, TGauge> TargetRpcProxSize;

    TGauge UsingSpareNodeCount;
    TGauge UsingSpareProxyCount;

    TGauge AssigningTabletNodes;
    TGauge AssigningSpareNodes;
    TGauge ReleasingSpareNodes;
};

////////////////////////////////////////////////////////////////////////////////

struct TBundleAlertCounters
{
    THashMap<TString, TCounter> IdToCounter;
};

////////////////////////////////////////////////////////////////////////////////

using TBundleSensorsPtr = TIntrusivePtr<TBundleSensors>;

struct TZoneSensors final
{
    TProfiler Profiler;

    TGauge OfflineNodeCount;
    TGauge OfflineNodeThreshold;

    TGauge OfflineProxyCount;
    TGauge OfflineProxyThreshold;

    TGauge FreeSpareNodeCount;
    TGauge FreeSpareProxyCount;
    TGauge RequiredSpareNodesCount;
};

using TZoneSensorsPtr = TIntrusivePtr<TZoneSensors>;

////////////////////////////////////////////////////////////////////////////////

class TBundleController
    : public IBundleController
{
public:
    TBundleController(IBootstrap* bootstrap, TBundleControllerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , Profiler("/bundle_controller")
        , SuccessfulScanBundleCounter_(Profiler.Counter("/successful_scan_bundles_count"))
        , FailedScanBundleCounter_(Profiler.Counter("/failed_scan_bundles_count"))
        , DynamicConfigUpdateCounter_(Profiler.Counter("/dynamic_config_update_counter"))
        , InstanceAllocationCounter_(Profiler.Counter("/instance_allocation_counter"))
        , InstanceDeallocationCounter_(Profiler.Counter("/instance_deallocation_counter"))
        , CellCreationCounter_(Profiler.Counter("/cell_creation_counter"))
        , CellRemovalCounter_(Profiler.Counter("/cell_removal_counter"))
        , InstanceCypressNodeRemovalCounter_(Profiler.Counter("/instance_cypress_node_removal_counter"))
        , ChangedNodeUserTagCounter_(Profiler.Counter("/changed_node_user_tag_counter"))
        , ChangedDecommissionedFlagCounter_(Profiler.Counter("/changed_decommissioned_flag_counter"))
        , ChangedEnableBundleBalancerFlagCounter_(Profiler.Counter("/changed_enable_bundle_balancer_flag_counter"))
        , ChangedNodeAnnotationCounter_(Profiler.Counter("/changed_node_annotation_counter"))
        , InflightNodeAllocationCount_(Profiler.Gauge("/inflight_node_allocations_count"))
        , InflightNodeDeallocationCount_(Profiler.Gauge("/inflight_node_deallocations_count"))
        , InflightCellRemovalCount_(Profiler.Gauge("/inflight_cell_removal_count"))
        , NodeAllocationRequestAge_(Profiler.TimeGauge("/node_allocation_request_age"))
        , NodeDeallocationRequestAge_(Profiler.TimeGauge("/node_deallocation_request_age"))
        , RemovingCellsAge_(Profiler.TimeGauge("/removing_cells_age"))
        , InflightProxyAllocationCounter_(Profiler.Gauge("/inflight_proxy_allocation_counter"))
        , InflightProxyDeallocationCounter_(Profiler.Gauge("/inflight_proxy_deallocation_counter"))
        , ChangedBundleShortNameCounter_(Profiler.Counter("/changed_bundle_short_name_counter"))
        , ChangedProxyRoleCounter_(Profiler.Counter("/changed_proxy_role_counter"))
        , InitializedNodeTagFiltersCounter_(Profiler.Counter("/initialized_node_tag_filters_counter"))
        , InitializedBundleTargetConfigCounter_(Profiler.Counter("/initialized_bundle_target_config_counter"))
        , ChangedProxyAnnotationCounter_(Profiler.Counter("/changed_proxy_annotation_counter"))
        , ProxyAllocationRequestAge_(Profiler.TimeGauge("/proxy_allocation_request_age"))
        , ProxyDeallocationRequestAge_(Profiler.TimeGauge("/proxy_deallocation_request_age"))
        , ChangedSystemAccountLimitCounter_(Profiler.Counter("/changed_system_account_limit_counter"))
        , ChangedResourceLimitsCounter_(Profiler.Counter("/changed_resource_limits_counter"))
    { }

    void Start() override
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        StartTime_ = TInstant::Now();

        YT_VERIFY(!PeriodicExecutor_);
        PeriodicExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TBundleController::ScanBundles, MakeWeak(this)),
            Config_->BundleScanPeriod);
        PeriodicExecutor_->Start();
    }

private:
    IBootstrap* const Bootstrap_;
    const TBundleControllerConfigPtr Config_;

    TCellTrackerImplPtr CellTrackerImpl_;

    TInstant StartTime_;
    TPeriodicExecutorPtr PeriodicExecutor_;

    const TProfiler Profiler;
    TCounter SuccessfulScanBundleCounter_;
    TCounter FailedScanBundleCounter_;

    TCounter DynamicConfigUpdateCounter_;
    TCounter InstanceAllocationCounter_;
    TCounter InstanceDeallocationCounter_;
    TCounter CellCreationCounter_;
    TCounter CellRemovalCounter_;
    TCounter InstanceCypressNodeRemovalCounter_;

    TCounter ChangedNodeUserTagCounter_;
    TCounter ChangedDecommissionedFlagCounter_;
    TCounter ChangedEnableBundleBalancerFlagCounter_;
    TCounter ChangedNodeAnnotationCounter_;

    TGauge InflightNodeAllocationCount_;
    TGauge InflightNodeDeallocationCount_;
    TGauge InflightCellRemovalCount_;

    TTimeGauge NodeAllocationRequestAge_;
    TTimeGauge NodeDeallocationRequestAge_;
    TTimeGauge RemovingCellsAge_;

    TGauge InflightProxyAllocationCounter_;
    TGauge InflightProxyDeallocationCounter_;

    TCounter ChangedBundleShortNameCounter_;
    TCounter ChangedProxyRoleCounter_;
    TCounter InitializedNodeTagFiltersCounter_;
    TCounter InitializedBundleTargetConfigCounter_;

    TCounter ChangedProxyAnnotationCounter_;
    TTimeGauge ProxyAllocationRequestAge_;
    TTimeGauge ProxyDeallocationRequestAge_;
    TCounter ChangedSystemAccountLimitCounter_;
    TCounter ChangedResourceLimitsCounter_;

    mutable THashMap<TString, TBundleSensorsPtr> BundleSensors_;
    mutable THashMap<TString, TZoneSensorsPtr> ZoneSensors_;

    mutable Orchid::TBundlesInfo OrchidBundlesInfo_;
    mutable Orchid::TZonesRacksInfo OrchidRacksInfo_;

    mutable THashMap<TString, TBundleAlertCounters> BundleAlerts_;


    void ScanBundles() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        if (!IsLeader()) {
            OrchidBundlesInfo_.clear();
            YT_LOG_DEBUG("Bundle Controller is not leading");
            return;
        }

        try {
            YT_PROFILE_TIMING("/bundle_controller/scan_bundles") {
                LinkOrchidService();
                DoScanBundles();
                SuccessfulScanBundleCounter_.Increment();
            }
        } catch (const TErrorException& ex) {
            YT_LOG_ERROR(ex, "Scanning bundles failed");
            FailedScanBundleCounter_.Increment();
        }
    }

    void ClearState() const
    {
        OrchidBundlesInfo_.clear();
        OrchidRacksInfo_.clear();
        BundleAlerts_.clear();
        ZoneSensors_.clear();
        BundleSensors_.clear();
    }

    void LinkOrchidService() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        static const TYPath LeaderOrchidServicePath = "//sys/bundle_controller/orchid";
        static const TString AttributeRemoteAddress = "remote_addresses";

        auto addresses = Bootstrap_->GetLocalAddresses();

        auto client = Bootstrap_->GetClient();
        auto exists = WaitFor(client->NodeExists(Format("%v&", LeaderOrchidServicePath)))
            .ValueOrThrow();

        if (!exists) {
            YT_LOG_INFO("Creating bundle controller orchid node");

            TCreateNodeOptions createOptions;
            createOptions.IgnoreExisting = true;
            createOptions.Recursive = true;
            createOptions.Attributes = CreateEphemeralAttributes();
            createOptions.Attributes->Set(AttributeRemoteAddress, ConvertToYsonString(addresses));

            WaitFor(client->CreateNode(LeaderOrchidServicePath, EObjectType::Orchid, createOptions))
                .ThrowOnError();
        }

        auto remoteAddressPath = Format("%v&/@%v", LeaderOrchidServicePath, AttributeRemoteAddress);
        auto currentRemoteAddress = WaitFor(client->GetNode(remoteAddressPath))
            .ValueOrThrow();

        if (AreNodesEqual(ConvertTo<NYTree::IMapNodePtr>(currentRemoteAddress), ConvertTo<NYTree::IMapNodePtr>(addresses))) {
            return;
        }

        YT_LOG_INFO("Setting new remote address for orchid node (OldValue: %Qv, NewValue: %Qv)",
            ConvertToYsonString(currentRemoteAddress, EYsonFormat::Text),
            ConvertToYsonString(addresses, EYsonFormat::Text));

        WaitFor(client->SetNode(remoteAddressPath, ConvertToYsonString(addresses)))
            .ThrowOnError();
    }

    static std::vector<TString> GetAliveAllocationsId(const TSchedulerInputState& inputState)
    {
        std::vector<TString> result;
        for (const auto& [_, bundleState] : inputState.BundleStates) {
            for (const auto& [allocId, _] : bundleState->NodeAllocations) {
                result.push_back(allocId);
            }
            for (const auto& [allocId, _] : bundleState->ProxyAllocations) {
                result.push_back(allocId);
            }
        }

        return result;
    }

    static std::vector<TString> GetAliveDeallocationsId(const TSchedulerInputState& inputState)
    {
        std::vector<TString> result;
        for (const auto& [_, bundleState] : inputState.BundleStates) {
            for (const auto& [deallocId, _] : bundleState->NodeDeallocations) {
                result.push_back(deallocId);
            }
            for (const auto& [deallocId, _] : bundleState->ProxyDeallocations) {
                result.push_back(deallocId);
            }
        }

        return result;
    }

    void DoScanBundles() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        YT_LOG_DEBUG("Bundles scan started");

        auto transaction = CreateTransaction();
        auto inputState = GetInputState(transaction);
        TSchedulerMutations mutations;
        ScheduleBundles(inputState, &mutations);
        Mutate(transaction, mutations);

        WaitFor(transaction->Commit())
            .ThrowOnError();

        ReportInflightMetrics(inputState, mutations);
        ReportResourceUsage(inputState);

        // Update input state for serving orchid requests.
        OrchidBundlesInfo_ = Orchid::GetBundlesInfo(inputState, mutations);
        OrchidRacksInfo_ = Orchid::GetZonesRacksInfo(inputState);
    }

    inline static const TString  AttributeBundleControllerAnnotations = "bundle_controller_annotations";
    inline static const TString  NodeAttributeUserTags = "user_tags";
    inline static const TString  NodeAttributeDecommissioned = "decommissioned";
    inline static const TString  NodeAttributeEnableBundleBalancer = "enable_bundle_balancer";
    inline static const TString  ProxyAttributeRole = "role";
    inline static const TString  AccountAttributeResourceLimits = "resource_limits";
    inline static const TString  BundleTabletStaticMemoryLimits = "resource_limits/tablet_static_memory";
    inline static const TString  BundleAttributeShortName = "short_name";
    inline static const TString  BundleAttributeNodeTagFilter = "node_tag_filter";
    inline static const TString  BundleAttributeTargetConfig = "bundle_controller_target_config";

    void Mutate(const ITransactionPtr& transaction, const TSchedulerMutations& mutations) const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        CreateHulkRequests<TAllocationRequest>(transaction, Config_->HulkAllocationsPath, mutations.NewAllocations);
        CreateHulkRequests<TDeallocationRequest>(transaction, Config_->HulkDeallocationsPath, mutations.NewDeallocations);
        CypressSet(transaction, GetBundlesStatePath(), mutations.ChangedStates);

        SetNodeAttributes(transaction, AttributeBundleControllerAnnotations, mutations.ChangeNodeAnnotations);
        SetNodeAttributes(transaction, NodeAttributeUserTags, mutations.ChangedNodeUserTags);
        SetNodeAttributes(transaction, NodeAttributeDecommissioned, mutations.ChangedDecommissionedFlag);
        SetNodeAttributes(transaction, NodeAttributeEnableBundleBalancer, mutations.ChangedEnableBundleBalancerFlag);

        SetProxyAttributes(transaction, AttributeBundleControllerAnnotations, mutations.ChangedProxyAnnotations);
        SetProxyAttributes(transaction, ProxyAttributeRole, mutations.ChangedProxyRole);

        // We should not violate RootSystemAccountLimit when changing per-bundle ones, so we apply changes in specific order.
        SetInstanceAttributes(transaction, BundleSystemQuotasPath, AccountAttributeResourceLimits, mutations.LoweredSystemAccountLimit);
        SetRootSystemAccountLimits(transaction, mutations.ChangedRootSystemAccountLimit);
        SetInstanceAttributes(transaction, BundleSystemQuotasPath, AccountAttributeResourceLimits, mutations.LiftedSystemAccountLimit);

        CreateTabletCells(transaction, mutations.CellsToCreate);
        RemoveTabletCells(transaction, mutations.CellsToRemove);

        if (mutations.DynamicConfig) {
            DynamicConfigUpdateCounter_.Increment();
            SetBundlesDynamicConfig(transaction, *mutations.DynamicConfig);
        }

        SetInstanceAttributes(transaction, TabletCellBundlesPath, BundleTabletStaticMemoryLimits, mutations.ChangedTabletStaticMemory);
        SetInstanceAttributes(transaction, TabletCellBundlesPath, BundleAttributeShortName, mutations.ChangedBundleShortName);

        SetInstanceAttributes(transaction, TabletCellBundlesPath, BundleAttributeNodeTagFilter, mutations.InitializedNodeTagFilters);
        SetInstanceAttributes(transaction, TabletCellBundlesPath, BundleAttributeTargetConfig, mutations.InitializedBundleTargetConfig);

        for (const auto& alert : mutations.AlertsToFire) {
            RegisterAlert(alert);
        }

        InstanceAllocationCounter_.Increment(mutations.NewAllocations.size());
        InstanceDeallocationCounter_.Increment(mutations.NewDeallocations.size());
        CellCreationCounter_.Increment(mutations.CellsToCreate.size());
        CellRemovalCounter_.Increment(mutations.CellsToRemove.size());
        ChangedNodeUserTagCounter_.Increment(mutations.ChangedNodeUserTags.size());
        ChangedDecommissionedFlagCounter_.Increment(mutations.ChangedDecommissionedFlag.size());
        ChangedEnableBundleBalancerFlagCounter_.Increment(mutations.ChangedEnableBundleBalancerFlag.size());
        ChangedNodeAnnotationCounter_.Increment(mutations.ChangeNodeAnnotations.size());
        InstanceCypressNodeRemovalCounter_.Increment(mutations.ProxiesToCleanup.size() + mutations.NodesToCleanup.size());

        ChangedProxyRoleCounter_.Increment(mutations.ChangedProxyRole.size());
        ChangedProxyAnnotationCounter_.Increment(mutations.ChangedProxyAnnotations.size());
        ChangedSystemAccountLimitCounter_.Increment(mutations.LoweredSystemAccountLimit.size() + mutations.LiftedSystemAccountLimit.size());
        ChangedResourceLimitsCounter_.Increment(mutations.ChangedTabletStaticMemory.size());

        ChangedBundleShortNameCounter_.Increment(mutations.ChangedBundleShortName.size());
        InitializedNodeTagFiltersCounter_.Increment(mutations.InitializedNodeTagFilters.size());
        InitializedBundleTargetConfigCounter_.Increment(mutations.InitializedBundleTargetConfig.size());

        RemoveInstanceCypressNode(transaction, TabletNodesPath, mutations.NodesToCleanup);
        RemoveInstanceCypressNode(transaction, RpcProxiesPath, mutations.ProxiesToCleanup);
    }

    void ReportInstanceCountBySize(
        const TSchedulerInputState::TInstanceCountBySize& countBySize,
        const TString& sensorName,
        TProfiler& profiler,
        THashMap<TString, TGauge>& sensors) const
    {
        for (auto& [sizeName, count] : countBySize) {
            auto it = sensors.find(sizeName);
            if (it != sensors.end()) {
                it->second.Update(count);
            } else {
                auto& sensor = sensors[sizeName];
                sensor = profiler.WithTag(SensorTagInstanceSize, sizeName).Gauge(sensorName);
                sensor.Update(count);
            }
        }

        for (auto& [sizeName, gauge] : sensors) {
            if (countBySize.find(sizeName) == countBySize.end()) {
                gauge.Update(0);
            }
        }
    }

    void ReportTargetInstanceCount(
        const TString& targetSizeName,
        const TString& sensorName,
        int instanceCount,
        TProfiler& profiler,
        THashMap<TString, TGauge>& sensors) const
    {
        auto it = sensors.find(targetSizeName);
        if (it != sensors.end()) {
            it->second.Update(instanceCount);
        } else {
            auto& sensor = sensors[targetSizeName];
            sensor = profiler.WithTag(SensorTagInstanceSize, targetSizeName).Gauge(sensorName);
            sensor.Update(instanceCount);
        }

        for (auto& [sizeName, gauge] : sensors) {
            if (sizeName != targetSizeName) {
                gauge.Update(0);
            }
        }
    }

    void ReportInflightMetrics(const TSchedulerInputState& input, const TSchedulerMutations& mutations) const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        int nodeAllocationCount = 0;
        int nodeDeallocationCount = 0;
        int proxyAllocationCount = 0;
        int proxyDeallocationCount = 0;
        int removingCellCount = 0;

        auto now = TInstant::Now();
        TDuration nodeAllocationRequestAge;
        TDuration nodeDeallocationRequestAge;
        TDuration removingCellsAge;
        TDuration proxyAllocationRequestAge;
        TDuration proxyDeallocationRequestAge;

        auto mergedBundlesState = MergeBundleStates(input, mutations);
        // TODO(capone212): think about per-bundle sensors.
        for (const auto& [_, state] : mergedBundlesState) {
            nodeAllocationCount += state->NodeAllocations.size();
            nodeDeallocationCount += state->NodeDeallocations.size();
            removingCellCount += state->RemovingCells.size();

            proxyAllocationCount += state->ProxyAllocations.size();
            proxyDeallocationCount += state->ProxyDeallocations.size();

            for (const auto& [_, allocation] : state->NodeAllocations) {
                nodeAllocationRequestAge = std::max(nodeAllocationRequestAge, now - allocation->CreationTime);
            }

            for (const auto& [_, deallocation] : state->NodeDeallocations) {
                nodeDeallocationRequestAge = std::max(nodeDeallocationRequestAge, now - deallocation->CreationTime);
            }

            for (const auto& [_, removingCell] : state->RemovingCells) {
                removingCellsAge = std::max(removingCellsAge, now - removingCell->RemovedTime);
            }

            for (const auto& [_, allocation] : state->ProxyAllocations) {
                proxyAllocationRequestAge = std::max(proxyAllocationRequestAge, now - allocation->CreationTime);
            }

            for (const auto& [_, deallocation] : state->ProxyDeallocations) {
                proxyDeallocationRequestAge = std::max(proxyDeallocationRequestAge, now - deallocation->CreationTime);
            }
        }

        InflightNodeAllocationCount_.Update(nodeAllocationCount);
        InflightNodeDeallocationCount_.Update(nodeDeallocationCount);
        InflightCellRemovalCount_.Update(removingCellCount);
        InflightProxyAllocationCounter_.Update(proxyAllocationCount);
        InflightProxyDeallocationCounter_.Update(proxyDeallocationCount);

        NodeAllocationRequestAge_.Update(nodeAllocationRequestAge);
        NodeDeallocationRequestAge_.Update(nodeDeallocationRequestAge);
        RemovingCellsAge_.Update(removingCellsAge);
        ProxyAllocationRequestAge_.Update(proxyAllocationRequestAge);
        ProxyDeallocationRequestAge_.Update(proxyAllocationRequestAge);
    }

    void ReportResourceUsage(TSchedulerInputState& input) const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        for (const auto& [bundleName, allocated] : input.BundleResourceAllocated) {
            auto sensors = GetBundleSensors(bundleName);
            sensors->CpuAllocated.Update(allocated->Vcpu);
            sensors->MemoryAllocated.Update(allocated->Memory);
        }

        for (const auto& [bundleName, alive] : input.BundleResourceAlive) {
            auto sensors = GetBundleSensors(bundleName);
            sensors->CpuAlive.Update(alive->Vcpu);
            sensors->MemoryAlive.Update(alive->Memory);
        }

        for (const auto& [bundleName, bundleInfo] : input.Bundles) {
            const auto& quota = bundleInfo->ResourceQuota;
            if (!bundleInfo->EnableBundleController || !quota) {
                continue;
            }
            auto sensors = GetBundleSensors(bundleName);
            sensors->CpuQuota.Update(quota->Vcpu());
            sensors->MemoryQuota.Update(quota->Memory);
        }

        for (const auto& [bundleName, bundleInfo] : input.Bundles) {
            if (!bundleInfo->EnableBundleController || !bundleInfo->TargetConfig) {
                continue;
            }

            const auto& targetConfig = bundleInfo->TargetConfig;
            auto sensors = GetBundleSensors(bundleName);
            sensors->WriteThreadPoolSize.Update(targetConfig->CpuLimits->WriteThreadPoolSize);

            const auto& memoryLimits = targetConfig->MemoryLimits;
            sensors->TabletStaticSize.Update(memoryLimits->TabletStatic.value_or(0));
            sensors->TabletDynamicSize.Update(memoryLimits->TabletDynamic.value_or(0));
            sensors->CompressedBlockCacheSize.Update(memoryLimits->CompressedBlockCache.value_or(0));
            sensors->UncompressedBlockCacheSize.Update(memoryLimits->UncompressedBlockCache.value_or(0));
            sensors->KeyFilterBlockCacheSize.Update(memoryLimits->KeyFilterBlockCache.value_or(0));
            sensors->VersionedChunkMetaSize.Update(memoryLimits->VersionedChunkMeta.value_or(0));
            sensors->LookupRowCacheSize.Update(memoryLimits->LookupRowCache.value_or(0));
        }

        for (const auto& [bundleName, bundleInfo] : input.Bundles) {
            if (!bundleInfo->EnableBundleController) {
                continue;
            }

            auto sensors = GetBundleSensors(bundleName);

            ReportInstanceCountBySize(
                input.AllocatedNodesBySize[bundleName],
                "/allocated_tablet_node_count",
                sensors->Profiler,
                sensors->AllocatedNodesBySize);

            ReportInstanceCountBySize(
                input.AliveNodesBySize[bundleName],
                "/alive_tablet_node_count",
                sensors->Profiler,
                sensors->AliveNodesBySize);

            ReportInstanceCountBySize(
                input.AllocatedProxiesBySize[bundleName],
                "/allocated_rpc_proxy_count",
                sensors->Profiler,
                sensors->AllocatedProxiesBySize);

            ReportInstanceCountBySize(
                input.AliveProxiesBySize[bundleName],
                "/alive_rpc_proxy_count",
                sensors->Profiler,
                sensors->AliveProxiesBySize);
        }

        for (const auto& [bundleName, bundleInfo] : input.Bundles) {
            if (!bundleInfo->EnableBundleController || !bundleInfo->TargetConfig) {
                continue;
            }

            const auto& targetConfig = bundleInfo->TargetConfig;
            auto sensors = GetBundleSensors(bundleName);

            ReportTargetInstanceCount(
                targetConfig->TabletNodeResourceGuarantee->Type,
                "/target_tablet_node_count",
                targetConfig->TabletNodeCount,
                sensors->Profiler,
                sensors->TargetTabletNodeSize);

            ReportTargetInstanceCount(
                targetConfig->RpcProxyResourceGuarantee->Type,
                "/target_rpc_proxy_count",
                targetConfig->RpcProxyCount,
                sensors->Profiler,
                sensors->TargetRpcProxSize);
        }

        for (const auto& [bundleName, bundleState] : input.BundleStates) {
            auto sensors = GetBundleSensors(bundleName);

            sensors->AssigningTabletNodes.Update(std::ssize(bundleState->BundleNodeAssignments));
            sensors->AssigningSpareNodes.Update(std::ssize(bundleState->SpareNodeAssignments));
            sensors->ReleasingSpareNodes.Update(std::ssize(bundleState->SpareNodeReleasements));
        }

        for (const auto& [zoneName, zoneDisrupted] : input.ZonesDisrupted) {
            auto sensor = GetZoneSensors(zoneName);
            sensor->OfflineNodeCount.Update(zoneDisrupted.OfflineNodeCount);
            sensor->OfflineNodeThreshold.Update(zoneDisrupted.OfflineNodeThreshold);
            sensor->OfflineProxyCount.Update(zoneDisrupted.OfflineProxyCount);
            sensor->OfflineProxyThreshold.Update(zoneDisrupted.OfflineProxyThreshold);
        }

        for (const auto& [zoneName, spareInfo] : input.ZoneToSpareNodes) {
            auto sensor = GetZoneSensors(zoneName);
            sensor->FreeSpareNodeCount.Update(std::ssize(spareInfo.FreeNodes));
        }

        for (const auto& [zoneName, spareInfo] : input.ZoneToSpareProxies) {
            auto sensor = GetZoneSensors(zoneName);
            sensor->FreeSpareProxyCount.Update(std::ssize(spareInfo.FreeProxies));
        }

        for (const auto& [zoneName, rackInfo] : input.ZoneToRacks) {
            auto zoneSensor = GetZoneSensors(zoneName);
            zoneSensor->RequiredSpareNodesCount.Update(rackInfo.RequiredSpareNodesCount);
        }
    }

    void RegisterAlert(const TAlert& alert) const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        auto bundleName = alert.BundleName.value_or(TString());

        auto& bundle = BundleAlerts_[bundleName];
        auto it = bundle.IdToCounter.find(alert.Id);

        if (it == bundle.IdToCounter.end()) {
            auto counter = Profiler
                .WithTag("bundle", bundleName)
                .WithTag("alarm_id", alert.Id)
                .Counter("/scan_bundles_alarms_count");
            it = bundle.IdToCounter.insert({bundleName, std::move(counter)}).first;
        }

        it->second.Increment(1);
    }

    TBundleSensorsPtr GetBundleSensors(const TString& bundleName) const
    {
        auto it = BundleSensors_.find(bundleName);
        if (it != BundleSensors_.end()) {
            return it->second;
        }

        auto sensors = New<TBundleSensors>();
        sensors->Profiler = Profiler.WithPrefix("/resource").WithTag("bundle", bundleName);
        auto& bundleProfiler = sensors->Profiler;

        sensors->CpuAllocated = bundleProfiler.Gauge("/cpu_allocated");
        sensors->CpuAlive = bundleProfiler.Gauge("/cpu_alive");
        sensors->CpuQuota = bundleProfiler.Gauge("/cpu_quota");

        sensors->MemoryAllocated = bundleProfiler.Gauge("/memory_allocated");
        sensors->MemoryAlive = bundleProfiler.Gauge("/memory_alive");
        sensors->MemoryQuota = bundleProfiler.Gauge("/memory_quota");

        sensors->WriteThreadPoolSize = bundleProfiler.Gauge("/write_thread_pool_size");
        sensors->CompressedBlockCacheSize = bundleProfiler.Gauge("/compressed_block_cache_size");
        sensors->UncompressedBlockCacheSize = bundleProfiler.Gauge("/uncompressed_block_cache_size");
        sensors->KeyFilterBlockCacheSize = bundleProfiler.Gauge("/key_filter_block_cache_size");
        sensors->LookupRowCacheSize = bundleProfiler.Gauge("/lookup_row_cache_size");
        sensors->TabletDynamicSize = bundleProfiler.Gauge("/tablet_dynamic_size");
        sensors->TabletStaticSize = bundleProfiler.Gauge("/tablet_static_size");

        sensors->UsingSpareNodeCount = bundleProfiler.Gauge("/using_spare_node_count");
        sensors->UsingSpareProxyCount = bundleProfiler.Gauge("/using_spare_proxy_count");

        sensors->AssigningTabletNodes = bundleProfiler.Gauge("/assigning_tablet_nodes");
        sensors->AssigningSpareNodes = bundleProfiler.Gauge("/assigning_spare_nodes");
        sensors->ReleasingSpareNodes = bundleProfiler.Gauge("/releasing_spare_nodes");

        BundleSensors_[bundleName] = sensors;
        return sensors;
    }

    TZoneSensorsPtr GetZoneSensors(const TString& zoneName) const
    {
        auto it = ZoneSensors_.find(zoneName);
        if (it != ZoneSensors_.end()) {
            return it->second;
        }

        auto sensors = New<TZoneSensors>();
        sensors->Profiler = Profiler.WithPrefix("/resource").WithTag("zone", zoneName);
        auto& zoneProfiler = sensors->Profiler;

        sensors->OfflineNodeCount = zoneProfiler.Gauge("/offline_node_count");
        sensors->OfflineNodeThreshold = zoneProfiler.Gauge("/offline_node_threshold");

        sensors->OfflineProxyCount = zoneProfiler.Gauge("/offline_proxy_count");
        sensors->OfflineProxyThreshold = zoneProfiler.Gauge("/offline_proxy_threshold");

        sensors->FreeSpareNodeCount = zoneProfiler.Gauge("/free_spare_node_count");
        sensors->FreeSpareProxyCount = zoneProfiler.Gauge("/free_spare_proxy_count");
        sensors->RequiredSpareNodesCount = zoneProfiler.Gauge("/required_spare_nodes_count");

        ZoneSensors_[zoneName] = sensors;
        return sensors;
    }

    ITransactionPtr CreateTransaction() const
    {
        TTransactionStartOptions transactionOptions;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", "Bundle Controller bundles scan");
        transactionOptions.Attributes = std::move(attributes);
        transactionOptions.Timeout = Config_->BundleScanTransactionTimeout;

        return WaitFor(Bootstrap_
            ->GetClient()
            ->StartTransaction(ETransactionType::Master, transactionOptions))
            .ValueOrThrow();
    }

    TSchedulerInputState GetInputState(const ITransactionPtr& transaction) const
    {
        TSchedulerInputState inputState{
            .Config = Config_,
        };

        YT_PROFILE_TIMING("/bundle_controller/load_timings/zones") {
            inputState.Zones = CypressList<TZoneInfo>(transaction, GetZonesPath());
        }

        YT_PROFILE_TIMING("/bundle_controller/load_timings/tablet_cell_bundles") {
            inputState.Bundles = CypressList<TBundleInfo>(transaction, TabletCellBundlesPath);
        }

        YT_PROFILE_TIMING("/bundle_controller/load_timings/tablet_cell_bundles_state") {
            inputState.BundleStates = CypressList<TBundleControllerState>(transaction, GetBundlesStatePath());
        }

        YT_PROFILE_TIMING("/bundle_controller/load_timings/tablet_nodes") {
            inputState.TabletNodes = CypressList<TTabletNodeInfo>(transaction, TabletNodesPath);
        }

        YT_PROFILE_TIMING("/bundle_controller/load_timings/tablet_cells") {
            inputState.TabletCells = CypressList<TTabletCellInfo>(transaction, TabletCellsPath);
        }

        YT_PROFILE_TIMING("/bundle_controller/load_timings/rpc_proxies") {
            inputState.RpcProxies = CypressGet<TRpcProxyInfo>(transaction, RpcProxiesPath);
        }

        YT_PROFILE_TIMING("/bundle_controller/load_timings/bundle_system_quotas") {
            inputState.SystemAccounts = CypressList<TSystemAccount>(transaction, BundleSystemQuotasPath);
        }


        YT_PROFILE_TIMING("/bundle_controller/load_timings/system_quotas_parent_account") {
            inputState.RootSystemAccount = GetRootSystemAccount(transaction);
        }

        inputState.AllocationRequests = LoadHulkRequests<TAllocationRequest>(
            transaction,
            {Config_->HulkAllocationsPath, Config_->HulkAllocationsHistoryPath},
            GetAliveAllocationsId(inputState));

        inputState.DeallocationRequests = LoadHulkRequests<TDeallocationRequest>(
            transaction,
            {Config_->HulkDeallocationsPath, Config_->HulkDeallocationsHistoryPath},
            GetAliveDeallocationsId(inputState));

        inputState.DynamicConfig = GetBundlesDynamicConfig(transaction);

        YT_LOG_DEBUG("Bundle Controller input state loaded "
            "(ZoneCount: %v, BundleCount: %v, BundleStateCount: %v, TabletNodeCount %v, TabletCellCount: %v, "
            "NodeAllocationRequestCount: %v, NodeDeallocationRequestCount: %v, RpcProxyCount: %v, SystemAccounts: %v)",
            inputState.Zones.size(),
            inputState.Bundles.size(),
            inputState.BundleStates.size(),
            inputState.TabletNodes.size(),
            inputState.TabletCells.size(),
            inputState.AllocationRequests.size(),
            inputState.DeallocationRequests.size(),
            inputState.RpcProxies.size(),
            inputState.SystemAccounts.size());

        return inputState;
    }

    template <typename TEntryInfo>
    static TIndexedEntries<TEntryInfo> CypressList(const ITransactionPtr& transaction, const TYPath& path)
    {
        TListNodeOptions options;
        options.Attributes = TEntryInfo::GetAttributes();

        auto yson = WaitFor(transaction->ListNode(path, options))
            .ValueOrThrow();
        auto entryList = ConvertTo<IListNodePtr>(yson);

        TIndexedEntries<TEntryInfo> result;
        for (const auto& entry : entryList->GetChildren()) {
            if (entry->GetType() != ENodeType::String) {
                THROW_ERROR_EXCEPTION("Unexpected entry type")
                    << TErrorAttribute("parent_path", path)
                    << TErrorAttribute("expected_type", ENodeType::String)
                    << TErrorAttribute("actual_type", entry->GetType());
            }
            const auto& name = entry->AsString()->GetValue();
            result[name] = ConvertTo<TIntrusivePtr<TEntryInfo>>(&entry->Attributes());
        }

        return result;
    }

    template <typename TEntryInfo>
    static TIndexedEntries<TEntryInfo> CypressGet(const ITransactionPtr& transaction, const TYPath& path)
    {
        TGetNodeOptions options;
        options.Attributes = TEntryInfo::GetAttributes();

        auto yson = WaitFor(transaction->GetNode(path, options))
            .ValueOrThrow();
        auto entryMap = ConvertTo<IMapNodePtr>(yson);

        TIndexedEntries<TEntryInfo> result;
        for (const auto& [name, entry] : entryMap->GetChildren()) {
            // Merging cypress node attributes with node value.
            auto entryInfo = ConvertTo<TIntrusivePtr<TEntryInfo>>(&entry->Attributes());
            result[name] = UpdateYsonStruct(entryInfo, entry);
        }

        return result;
    }

    template <typename TEntryInfo>
    static void CypressSet(
        const ITransactionPtr& transaction,
        const TYPath& basePath,
        const TIndexedEntries<TEntryInfo>& entries)
    {
        for (const auto& [name, entry] : entries) {
            CypressSet(transaction, basePath, name, entry);
        }
    }

    template <typename TEntryInfoPtr>
    static void CypressSet(
        const ITransactionPtr& transaction,
        const TYPath& basePath,
        const TString& name,
        const TEntryInfoPtr& entry)
    {
        auto path = Format("%v/%v", basePath, NYPath::ToYPathLiteral(name));

        TCreateNodeOptions createOptions;
        createOptions.IgnoreExisting = true;
        createOptions.Recursive = true;
        WaitFor(transaction->CreateNode(path, EObjectType::MapNode, createOptions))
            .ThrowOnError();

        TMultisetAttributesNodeOptions options;
        WaitFor(transaction->MultisetAttributesNode(
            path + "/@",
            ConvertTo<IMapNodePtr>(entry),
            options))
            .ThrowOnError();
    }

    template <typename TEntryInfo>
    static void CreateHulkRequests(
        const ITransactionPtr& transaction,
        const TYPath& basePath,
        const TIndexedEntries<TEntryInfo>& requests)
    {
        for (const auto& [requestId, requestBody] : requests) {
            auto path = Format("%v/%v", basePath, NYPath::ToYPathLiteral(requestId));

            TCreateNodeOptions createOptions;
            createOptions.Attributes = CreateEphemeralAttributes();
            createOptions.Attributes->Set("value", ConvertToYsonString(requestBody));
            createOptions.Recursive = true;
            WaitFor(transaction->CreateNode(path, EObjectType::Document, createOptions))
                .ThrowOnError();
        }
    }

    template <typename TAttribute>
    static void SetInstanceAttributes(
        const ITransactionPtr& transaction,
        const TYPath& instanceBasePath,
        const TString& attributeName,
        const THashMap<TString, TAttribute>& attributes)
    {
        for (const auto& [instanceName, attribute] : attributes) {
            auto path = Format("%v/%v/@%v",
                instanceBasePath,
                NYPath::ToYPathLiteral(instanceName),
                attributeName);

            TSetNodeOptions setOptions;
            WaitFor(transaction->SetNode(path, ConvertToYsonString(attribute), setOptions))
                .ThrowOnError();
        }
    }

    template <typename TAttribute>
    static void SetNodeAttributes(
        const ITransactionPtr& transaction,
        const TString& attributeName,
        const THashMap<TString, TAttribute>& attributes)
    {
        SetInstanceAttributes(transaction, TabletNodesPath, attributeName, attributes);
    }

    template <typename TAttribute>
    static void SetProxyAttributes(
        const ITransactionPtr& transaction,
        const TString& attributeName,
        const THashMap<TString, TAttribute>& attributes)
    {
        SetInstanceAttributes(transaction, RpcProxiesPath, attributeName, attributes);
    }

    template <typename TEntryInfo>
    static TIndexedEntries<TEntryInfo> LoadHulkRequests(
        const ITransactionPtr& transaction,
        const std::vector<TYPath>& basePaths,
        const std::vector<TString>& requests)
    {
        TIndexedEntries<TEntryInfo> results;
        using TEntryInfoPtr = TIntrusivePtr<TEntryInfo>;

        for (const auto& requestId : requests) {
            auto request = LoadHulkRequest<TEntryInfoPtr>(transaction, basePaths, requestId);
            if (request) {
                results[requestId] = request;
            }
        }
        return results;
    }

    template <typename TEntryInfoPtr>
    static TEntryInfoPtr LoadHulkRequest(
        const ITransactionPtr& transaction,
        const std::vector<TYPath>& basePaths,
        const TString& id)
    {
        for (const auto& basePath: basePaths) {
            auto path = Format("%v/%v", basePath,  NYPath::ToYPathLiteral(id));

            if (!WaitFor(transaction->NodeExists(path)).ValueOrThrow()) {
                continue;
            }

            auto yson = WaitFor(transaction->GetNode(path))
                .ValueOrThrow();

            return ConvertTo<TEntryInfoPtr>(yson);
        }

        return {};
    }

    static TSystemAccountPtr GetRootSystemAccount(const ITransactionPtr& transaction)
    {
        auto path = Format("%v/@", BundleSystemQuotasPath);
        auto yson = WaitFor(transaction->GetNode(path))
            .ValueOrThrow();
        return ConvertTo<TSystemAccountPtr>(yson);
    }

    static void SetRootSystemAccountLimits(const ITransactionPtr& transaction, const TAccountResourcesPtr& limits)
    {
        if (!limits) {
            return;
        }

        auto path = Format("%v/@%v", BundleSystemQuotasPath, AccountAttributeResourceLimits);
        TSetNodeOptions setOptions;
        WaitFor(transaction->SetNode(path, ConvertToYsonString(limits), setOptions))
            .ThrowOnError();
    }

    static void CreateTabletCells(const ITransactionPtr& transaction, const THashMap<TString, int>& cellsToCreate)
    {
        for (const auto& [bundleName, cellCount] : cellsToCreate) {
            TCreateObjectOptions createOptions;
            createOptions.Attributes = CreateEphemeralAttributes();
            createOptions.Attributes->Set("tablet_cell_bundle", bundleName);

            for (int index = 0; index < cellCount; ++index) {
                WaitFor(transaction->CreateObject(EObjectType::TabletCell, createOptions))
                    .ThrowOnError();
            }
        }
    }

    static void RemoveTabletCells(const ITransactionPtr& transaction, const std::vector<TString>& cellsToRemove)
    {
        for (const auto& cellId : cellsToRemove) {
            WaitFor(transaction->RemoveNode(Format("%v/%v", TabletCellsPath, cellId)))
                .ThrowOnError();
        }
    }

    static void RemoveInstanceCypressNode(const ITransactionPtr& transaction, const TYPath& basePath, const THashSet<TString>& instanciesToRemove)
    {
        for (const auto& instanceName : instanciesToRemove) {
            WaitFor(transaction->RemoveNode(Format("%v/%v", basePath, instanceName)))
                .ThrowOnError();
        }
    }

    static void SetBundlesDynamicConfig(
        const ITransactionPtr& transaction,
        const TBundlesDynamicConfig& config)
    {
        TSetNodeOptions setOptions;
        WaitFor(transaction->SetNode(TabletCellBundlesDynamicConfigPath, ConvertToYsonString(config), setOptions))
            .ThrowOnError();
    }

    static TBundlesDynamicConfig GetBundlesDynamicConfig(const ITransactionPtr& transaction)
    {
        bool exists = WaitFor(transaction->NodeExists(TabletCellBundlesDynamicConfigPath))
            .ValueOrThrow();

        if (!exists) {
            return {};
        }

        auto yson = WaitFor(transaction->GetNode(TabletCellBundlesDynamicConfigPath))
            .ValueOrThrow();

        return ConvertTo<TBundlesDynamicConfig>(yson);
    }

    bool IsLeader() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetElectionManager()->IsLeader();
    }

    TYPath GetZonesPath() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Format("%v/zones", Config_->RootPath);
    }

    TYPath GetBundlesStatePath() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Format("%v/bundles_state", Config_->RootPath);
    }

    NYTree::IYPathServicePtr CreateOrchidService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND(&TBundleController::BuildOrchid, MakeStrong(this)))
            ->Via(Bootstrap_->GetControlInvoker());
    }

    void BuildOrchid(IYsonConsumer* consumer)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("service").BeginMap()
                    .Item("leader").Value(IsLeader())
                .EndMap()
                .Item("config").Value(Config_)
                .Item("state").BeginMap()
                    .Item("bundles").Value(OrchidBundlesInfo_)
                    .Item("racks").Value(OrchidRacksInfo_)
                .EndMap()
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

IBundleControllerPtr CreateBundleController(IBootstrap* bootstrap, TBundleControllerConfigPtr config)
{
    return New<TBundleController>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
