#include "bundle_controller.h"
#include "bundle_scheduler.h"
#include "chaos_scheduler.h"

#include "bootstrap.h"
#include "config.h"
#include "cypress_bindings.h"
#include "orchid_bindings.h"
#include "bundle_sensors.h"
#include "cell_downtime_tracker.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

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

static constexpr auto& Logger = BundleControllerLogger;
static const TYPath TabletCellBundlesPath("//sys/tablet_cell_bundles");
static const TYPath ChaosCellBundlesPath("//sys/chaos_cell_bundles");

static const TYPath TabletCellBundlesDynamicConfigPath("//sys/tablet_cell_bundles/@config");
static const TYPath TabletNodesPath("//sys/tablet_nodes");
static const TYPath TabletCellsPath("//sys/tablet_cells");
static const TYPath RpcProxiesPath("//sys/rpc_proxies");
static const TYPath BundleSystemQuotasPath("//sys/account_tree/bundle_system_quotas");
static const TYPath GlobalCellRegistryPath("//sys/global_cell_registry");

static const TYPath BundleAttributeClockClusterTag("options/clock_cluster_tag");
static const TYPath BundleAttributeMetadataCellIds("metadata_cell_ids");

static const TYPath BundleAttributeMuteTabletCellSnapshotCheck("mute_tablet_cell_snapshots_check");
static const TYPath BundleAttributeMuteTabletCellsCheck("mute_tablet_cells_check");

static const TString SensorTagInstanceSize = "instance_size";

static constexpr int DefaultMaxSize = 1'000'000;

////////////////////////////////////////////////////////////////////////////////

struct TBundleAlertCounters
{
    THashMap<TString, TCounter> IdToCounter;
};

////////////////////////////////////////////////////////////////////////////////

struct TZoneSensors final
{
    TProfiler Profiler;

    TGauge OfflineNodeCount;
    TGauge OfflineNodeThreshold;

    TGauge OfflineProxyCount;
    TGauge OfflineProxyThreshold;

    TGauge FreeSpareNodeCount;
    TGauge ExternallyDecommissionedNodeCount;
    TGauge FreeSpareProxyCount;
    TGauge RequiredSpareNodeCount;
};

using TZoneSensorsPtr = TIntrusivePtr<TZoneSensors>;

////////////////////////////////////////////////////////////////////////////////

class TForeignClientProvider
{
public:
    explicit TForeignClientProvider(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    IClientPtr Get(const TString& clusterName)
    {
        if (const auto& client = Clients_[clusterName]; client) {
            return client;
        }

        auto localConnection = Bootstrap_->GetClient()->GetNativeConnection();
        auto foreignConnection = localConnection->GetClusterDirectory()->GetConnectionOrThrow(clusterName);
        // TODO(capone212): Use separate user name NSecurityClient::BundleControllerUserName
        auto client = foreignConnection->CreateClient(TClientOptions::FromUser(NSecurityClient::RootUserName));
        Clients_[clusterName] = client;

        return client;
    }

private:
    IBootstrap* const Bootstrap_;
    THashMap<TString, IClientPtr> Clients_;
};

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
        , ChangedBundleShortNameCounter_(Profiler.Counter("/changed_bundle_short_name_counter"))
        , ChangedProxyRoleCounter_(Profiler.Counter("/changed_proxy_role_counter"))
        , InitializedNodeTagFilterCounter_(Profiler.Counter("/initialized_node_tag_filters_counter"))
        , InitializedBundleTargetConfigCounter_(Profiler.Counter("/initialized_bundle_target_config_counter"))
        , ChangedProxyAnnotationCounter_(Profiler.Counter("/changed_proxy_annotation_counter"))
        , ChangedSystemAccountLimitCounter_(Profiler.Counter("/changed_system_account_limit_counter"))
        , ChangedResourceLimitCounter_(Profiler.Counter("/changed_resource_limits_counter"))
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

    TCounter ChangedBundleShortNameCounter_;
    TCounter ChangedProxyRoleCounter_;
    TCounter InitializedNodeTagFilterCounter_;
    TCounter InitializedBundleTargetConfigCounter_;

    TCounter ChangedProxyAnnotationCounter_;
    TCounter ChangedSystemAccountLimitCounter_;
    TCounter ChangedResourceLimitCounter_;

    THashMap<TString, TBundleSensorsPtr> BundleSensors_;
    THashMap<TString, TZoneSensorsPtr> ZoneSensors_;

    Orchid::TBundlesInfo OrchidBundlesInfo_;
    Orchid::TZonesRacksInfo OrchidRacksInfo_;

    THashMap<TString, TBundleAlertCounters> BundleAlerts_;
    ICellDowntimeTrackerPtr CellDowntimeTracker_;

    void ScanBundles()
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        if (!IsLeader()) {
            ClearState();

            YT_LOG_DEBUG("Bundle Controller is not leading");
            return;
        }

        ScanTabletCellBundles();
        ScanChaosCellBundles();
    }

    void ScanTabletCellBundles()
    {
        try {
            YT_PROFILE_TIMING("/bundle_controller/scan_bundles") {
                LinkOrchidService();
                LinkBundleControllerService();
                DoScanTabletBundles();
                SuccessfulScanBundleCounter_.Increment();
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Scanning tablet cell bundles failed");
            FailedScanBundleCounter_.Increment();
        }
    }

    void ScanChaosCellBundles()
    {
        try {
            DoScanChaosBundles();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Scanning chaos cell bundles failed");
            FailedScanBundleCounter_.Increment();
        }
    }

    void ClearState()
    {
        OrchidBundlesInfo_.clear();
        OrchidRacksInfo_.clear();
        BundleAlerts_.clear();
        ZoneSensors_.clear();
        BundleSensors_.clear();
        CellDowntimeTracker_.Reset();
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

        YT_LOG_INFO("Setting new remote address for orchid node (OldValue: %v, NewValue: %v)",
            ConvertToYsonString(currentRemoteAddress, EYsonFormat::Text),
            ConvertToYsonString(addresses, EYsonFormat::Text));

        WaitFor(client->SetNode(remoteAddressPath, ConvertToYsonString(addresses)))
            .ThrowOnError();
    }

    void LinkBundleControllerService() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        static const TYPath LeaderBundleControllerPath = "//sys/bundle_controller";
        static const TString AttributeAddress = "addresses";

        auto client = Bootstrap_->GetClient();

        auto addressesPath = Format("%v&/@%v", LeaderBundleControllerPath, AttributeAddress);

        auto exists = WaitFor(client->NodeExists(addressesPath))
            .ValueOrThrow();

        auto addresses = Bootstrap_->GetLocalAddresses();

        if (exists) {
            auto currentAddresses = WaitFor(client->GetNode(addressesPath))
                .ValueOrThrow();
            if (AreNodesEqual(ConvertTo<NYTree::IMapNodePtr>(currentAddresses), ConvertTo<NYTree::IMapNodePtr>(addresses))) {
                return;
            }
        }

        YT_LOG_INFO("Setting new address for BundleController (NewValue: %v)",
            ConvertToYsonString(addresses, EYsonFormat::Text));

        WaitFor(client->SetNode(addressesPath, ConvertToYsonString(addresses)))
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

    void DoScanTabletBundles()
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        YT_LOG_DEBUG("Bundles scan started");

        auto transaction = CreateTransaction();
        auto inputState = GetInputState(transaction);
        TSchedulerMutations mutations;
        ScheduleBundles(inputState, &mutations);

        if (!inputState.SysConfig || !inputState.SysConfig->DisableBundleController ) {
            Mutate(transaction, mutations);
        } else {
            YT_LOG_WARNING("Bundle controller is disabled");

            RegisterAlert({
                .Id = "bundle_controller_is_disabled",
                .Description = "BundleController is explicitly disabled",
            });
        }

        WaitFor(transaction->Commit())
            .ThrowOnError();

        ReportInflightMetrics(inputState, mutations);
        ReportResourceUsage(inputState);

        // Update input state for serving orchid requests.
        OrchidBundlesInfo_ = Orchid::GetBundlesInfo(inputState, mutations);
        OrchidRacksInfo_ = Orchid::GetZonesRacksInfo(inputState);

        if (!CellDowntimeTracker_) {
            CellDowntimeTracker_ = CreateCellDowntimeTracker();
        }

        CellDowntimeTracker_->HandleState(inputState, [this] (const TString& bundleName) {
            return GetBundleSensors(bundleName);
        });
    }

    void DoScanChaosBundles()
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        if (!Config_->EnableChaosBundleManagement) {
            return;
        }

        YT_LOG_DEBUG("Chaos bundles scan started");

        TForeignClientProvider clientProvider(Bootstrap_);

        auto transaction = CreateTransaction();
        auto inputState = GetChaosInputState(transaction, &clientProvider);
        TChaosSchedulerMutations mutations;
        ScheduleChaosBundles(inputState, &mutations);

        if (!inputState.SysConfig || !inputState.SysConfig->DisableBundleController ) {
            Mutate(mutations, transaction, &clientProvider);
            WaitFor(transaction->Commit())
                .ThrowOnError();
        } else {
            YT_LOG_WARNING("Bundle controller is disabled");

            RegisterAlert({
                .Id = "bundle_controller_is_disabled",
                .Description = "BundleController is explicitly disabled",
            });
        }
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

    void Mutate(const ITransactionPtr& transaction, const TSchedulerMutations& mutations)
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
        RemoveInstanceAttributes(transaction, RpcProxiesPath, ProxyAttributeRole, mutations.RemovedProxyRole);

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

        SetInstanceAttributes(transaction, TabletCellBundlesPath, BundleAttributeNodeTagFilter, mutations.ChangedNodeTagFilters);
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
        ChangedProxyRoleCounter_.Increment(mutations.RemovedProxyRole.size());
        ChangedProxyAnnotationCounter_.Increment(mutations.ChangedProxyAnnotations.size());
        ChangedSystemAccountLimitCounter_.Increment(mutations.LoweredSystemAccountLimit.size() + mutations.LiftedSystemAccountLimit.size());
        ChangedResourceLimitCounter_.Increment(mutations.ChangedTabletStaticMemory.size());

        ChangedBundleShortNameCounter_.Increment(mutations.ChangedBundleShortName.size());
        InitializedNodeTagFilterCounter_.Increment(mutations.ChangedNodeTagFilters.size());
        InitializedBundleTargetConfigCounter_.Increment(mutations.InitializedBundleTargetConfig.size());

        RemoveInstanceCypressNode(transaction, TabletNodesPath, mutations.NodesToCleanup);
        RemoveInstanceCypressNode(transaction, RpcProxiesPath, mutations.ProxiesToCleanup);

        SetInstanceAttributes(
            transaction,
            TabletCellBundlesPath,
            BundleAttributeMuteTabletCellSnapshotCheck,
            mutations.ChangedMuteTabletCellSnapshotsCheck);

        SetInstanceAttributes(
            transaction,
            TabletCellBundlesPath,
            BundleAttributeMuteTabletCellsCheck,
            mutations.ChangedMuteTabletCellsCheck);
    }

    void Mutate(
        const TChaosSchedulerMutations& mutations,
        const ITransactionPtr& transaction,
        TForeignClientProvider* clientProvider)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        for (const auto& [cellTag, cellInfo] : mutations.CellTagsToRegister) {
            auto path = Format("%v/cell_tags/%v", GlobalCellRegistryPath, cellTag);
            CypressSetSingleNode(transaction, path, cellInfo);
        }

        for (const auto& [cellTag, cellInfo] : mutations.AdditionalCellTagsToRegister) {
            auto path = Format("%v/additional_cell_tags/%v", GlobalCellRegistryPath, cellTag);
            CypressSetSingleNode(transaction, path, cellInfo);
        }

        if (mutations.ChangedChaosCellTagLast) {
            auto path = Format("%v/cell_tag_last", GlobalCellRegistryPath);
            CypressSetSingleNode(transaction, path, mutations.ChangedChaosCellTagLast.value());
        }

        for (const auto& [cluster, accounts] : mutations.ForeignSystemAccountsToCreate) {
            auto client = clientProvider->Get(cluster);

            for (const auto& account : accounts) {
                YT_LOG_INFO("Creating system account (Cluster: %v, Account: %v)",
                    cluster,
                    account);

                CreateSystemAccount(client, account);
            }
        }

        for (const auto& [cluster, bundlesInfo] : mutations.ForeignTabletCellBundlesToCreate) {
            auto client = clientProvider->Get(cluster);

            for (const auto& [bundleName, attributes] : bundlesInfo) {
                YT_LOG_INFO("Creating tablet cell bundle (Cluster: %v, BundleName: %v)",
                    cluster,
                    bundleName);

                CreateObject(EObjectType::TabletCellBundle, client, attributes);
            }
        }

        for (const auto& [cluster, bundlesCellTag] : mutations.ForeignBundleCellTagsToSet) {
            auto client = clientProvider->Get(cluster);
            SetInstanceAttributes(
                client,
                TabletCellBundlesPath,
                BundleAttributeClockClusterTag,
                bundlesCellTag);
        }

        for (const auto& [cluster, chaosBundlesInfo] : mutations.ForeignChaosCellBundlesToCreate) {
            auto client = clientProvider->Get(cluster);

            for (const auto& [bundleName, attributes] : chaosBundlesInfo) {
                YT_LOG_INFO("Creating chaos cell bundle (Cluster: %v, BundleName: %v)",
                    cluster,
                    bundleName);

                CreateObject(EObjectType::ChaosCellBundle, client, attributes);
            }
        }

        for (const auto& [cluster, chaosAreasInfo] : mutations.ForeignChaosAreasToCreate) {
            auto client = clientProvider->Get(cluster);

            for (const auto& [bundleName, attributes] : chaosAreasInfo) {
                YT_LOG_INFO("Creating chaos area (Cluster: %v, BundleName: %v)",
                    cluster,
                    bundleName);

                CreateObject(EObjectType::Area, client, attributes);
            }
        }

        for (const auto& [cluster, chaosCellsInfo] : mutations.ForeignChaosCellsToCreate) {
            auto client = clientProvider->Get(cluster);

            for (const auto& [cellId, attributes] : chaosCellsInfo) {
                YT_LOG_INFO("Creating chaos cell (Cluster: %v, CellId: %v)",
                    cluster,
                    cellId);

                CreateObject(EObjectType::ChaosCell, client, attributes);
            }
        }

        for (const auto& [cluster, bundlesMetadataCellIds] : mutations.ForeignMetadataCellIdsToSet) {
            auto client = clientProvider->Get(cluster);
            SetInstanceAttributes(
                client,
                ChaosCellBundlesPath,
                BundleAttributeMetadataCellIds,
                bundlesMetadataCellIds);
        }
    }

    void ReportInstanceCountBySize(
        const TSchedulerInputState::TInstanceCountBySize& countBySize,
        const TString& sensorName,
        TProfiler& profiler,
        THashMap<TString, TGauge>& sensors)
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
        THashMap<TString, TGauge>& sensors)
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

    void ReportInflightMetrics(const TSchedulerInputState& input, const TSchedulerMutations& mutations)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        auto now = TInstant::Now();
        auto mergedBundlesState = MergeBundleStates(input, mutations);

        for (const auto& [bundleName, state] : mergedBundlesState) {
            auto sensors = GetBundleSensors(bundleName);

            sensors->InflightNodeAllocationCount.Update(state->NodeAllocations.size());
            sensors->InflightNodeDeallocationCount.Update(state->NodeDeallocations.size());
            sensors->InflightCellRemovalCount.Update(state->RemovingCells.size());
            sensors->InflightProxyAllocationCounter.Update(state->ProxyAllocations.size());
            sensors->InflightProxyDeallocationCounter.Update(state->ProxyDeallocations.size());

            TDuration nodeAllocationRequestAge;
            TDuration nodeDeallocationRequestAge;
            TDuration removingCellsAge;
            TDuration proxyAllocationRequestAge;
            TDuration proxyDeallocationRequestAge;

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

            sensors->NodeAllocationRequestAge.Update(nodeAllocationRequestAge);
            sensors->NodeDeallocationRequestAge.Update(nodeDeallocationRequestAge);
            sensors->RemovingCellsAge.Update(removingCellsAge);
            sensors->ProxyAllocationRequestAge.Update(proxyAllocationRequestAge);
            sensors->ProxyDeallocationRequestAge.Update(proxyAllocationRequestAge);
        }
    }

    void ReportResourceUsage(TSchedulerInputState& input)
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
            sensors->WriteThreadPoolSize.Update(targetConfig->CpuLimits->WriteThreadPoolSize.value_or(0));

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
                GetInstanceSize(targetConfig->TabletNodeResourceGuarantee),
                "/target_tablet_node_count",
                targetConfig->TabletNodeCount,
                sensors->Profiler,
                sensors->TargetTabletNodeSize);

            ReportTargetInstanceCount(
                GetInstanceSize(targetConfig->RpcProxyResourceGuarantee),
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

        for (const auto& [bundleName, dataCenterNodes] : input.BundleNodes) {
            auto sensors = GetBundleSensors(bundleName);

            THashMap<TString, int> assignedNodesByDC;
            for (const auto& [dataCenter, _] : sensors->AssignedBundleNodesPerDC) {
                assignedNodesByDC[dataCenter] = 0;
            }

            int offlineNodeCount = 0;
            int decommissionedNodeCount = 0;
            int maintenanceRequestedNodeCount = 0;

            for (const auto& [dataCenter, nodes] : dataCenterNodes) {
                for (const auto& nodeName : nodes) {
                    const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

                    if (nodeInfo->State != InstanceStateOnline) {
                        ++offlineNodeCount;
                        continue;
                    }

                    if (!nodeInfo->CmsMaintenanceRequests.empty()) {
                        ++maintenanceRequestedNodeCount;
                        continue;
                    }

                    if (nodeInfo->Decommissioned) {
                        ++decommissionedNodeCount;
                        continue;
                    }

                    if (!nodeInfo->UserTags.empty()) {
                        ++assignedNodesByDC[dataCenter];
                    }
                }
            }
            sensors->OfflineNodeCount.Update(offlineNodeCount);
            sensors->MaintenanceRequestedNodeCount.Update(maintenanceRequestedNodeCount);
            sensors->DecommissionedNodeCount.Update(decommissionedNodeCount);

            for (const auto& [dataCenter, nodeCount] : assignedNodesByDC) {
                auto it = sensors->AssignedBundleNodesPerDC.find(dataCenter);
                if (it == sensors->AssignedBundleNodesPerDC.end()) {
                    auto gauge = sensors->Profiler.WithTag("data_center", dataCenter).Gauge("/assigned_bundle_nodes");
                    it = sensors->AssignedBundleNodesPerDC.insert(std::pair(dataCenter, gauge)).first;
                }
                it->second.Update(nodeCount);
            }
        }

        for (const auto& [bundleName, dataCenterProxies] : input.BundleProxies) {
            int offlineProxyCount = 0;

            for (const auto& [_, proxies] : dataCenterProxies) {
                for (const auto& proxyName : proxies) {
                    const auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
                    if (!proxyInfo->Alive) {
                        ++offlineProxyCount;
                    }
                }
            }

            auto sensors = GetBundleSensors(bundleName);
            sensors->OfflineProxyCount.Update(offlineProxyCount);
        }

        for (const auto& [dcPair, zoneDisrupted] : input.DatacenterDisrupted) {
            auto sensor = GetZoneSensors(dcPair.first, dcPair.second);
            sensor->OfflineNodeCount.Update(zoneDisrupted.OfflineNodeCount);
            sensor->OfflineNodeThreshold.Update(zoneDisrupted.OfflineNodeThreshold);
            sensor->OfflineProxyCount.Update(zoneDisrupted.OfflineProxyCount);
            sensor->OfflineProxyThreshold.Update(zoneDisrupted.OfflineProxyThreshold);
        }

        for (const auto& [zoneName, perDCSpareInfo] : input.ZoneToSpareNodes) {
            for (const auto& [dataCenter, spareInfo] : perDCSpareInfo) {
                auto zoneSensor = GetZoneSensors(zoneName, dataCenter);
                zoneSensor->FreeSpareNodeCount.Update(std::ssize(spareInfo.FreeNodes));
                zoneSensor->ExternallyDecommissionedNodeCount.Update(std::ssize(spareInfo.ExternallyDecommissioned));

                for (const auto& [bundleName, instancies] : spareInfo.UsedByBundle) {
                    auto bundleSensors = GetBundleSensors(bundleName);
                    bundleSensors->UsingSpareNodeCount.Update(std::ssize(instancies));
                }
            }
        }

        for (const auto& [zoneName, spareInfo] : input.ZoneToSpareProxies) {
            auto zoneSensor = GetZoneSensors(zoneName, {});
            zoneSensor->FreeSpareProxyCount.Update(std::ssize(spareInfo.FreeProxies));

            for (const auto& [bundleName, instancies] : spareInfo.UsedByBundle) {
                auto bundleSensors = GetBundleSensors(bundleName);
                bundleSensors->UsingSpareProxyCount.Update(std::ssize(instancies));
            }
        }

        for (const auto& [zoneName, rackInfo] : input.ZoneToRacks) {
            for (const auto& [dataCenter, dataCenterRacks] : rackInfo) {
                auto zoneSensor = GetZoneSensors(zoneName, dataCenter);
                zoneSensor->RequiredSpareNodeCount.Update(dataCenterRacks.RequiredSpareNodeCount);
            }
        }
    }

    void RegisterAlert(const TAlert& alert)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        static const TString DefaultBundleName = "all";

        auto bundleName = alert.BundleName.value_or(DefaultBundleName);

        auto& bundle = BundleAlerts_[bundleName];
        auto it = bundle.IdToCounter.find(alert.Id);

        if (it == bundle.IdToCounter.end()) {
            auto counter = Profiler
                .WithTag("tablet_cell_bundle", bundleName)
                .WithTag("alarm_id", alert.Id)
                .Counter("/scan_bundles_alarms_count");
            it = bundle.IdToCounter.insert({bundleName, std::move(counter)}).first;
        }

        it->second.Increment(1);
    }

    TBundleSensorsPtr GetBundleSensors(const TString& bundleName)
    {
        auto it = BundleSensors_.find(bundleName);
        if (it != BundleSensors_.end()) {
            return it->second;
        }

        auto sensors = New<TBundleSensors>();
        sensors->Profiler = Profiler.WithPrefix("/resource").WithTag("tablet_cell_bundle", bundleName);
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

        sensors->InflightNodeAllocationCount = bundleProfiler.Gauge("/inflight_node_allocations_count");
        sensors->InflightNodeDeallocationCount = bundleProfiler.Gauge("/inflight_node_deallocations_count");
        sensors->InflightCellRemovalCount = bundleProfiler.Gauge("/inflight_cell_removal_count");

        sensors->InflightProxyAllocationCounter = bundleProfiler.Gauge("/inflight_proxy_allocation_counter");
        sensors->InflightProxyDeallocationCounter = bundleProfiler.Gauge("/inflight_proxy_deallocation_counter");

        sensors->OfflineNodeCount = bundleProfiler.Gauge("/offline_node_count");
        sensors->OfflineProxyCount = bundleProfiler.Gauge("/offline_proxy_count");
        sensors->DecommissionedNodeCount = bundleProfiler.Gauge("/decommissioned_node_count");
        sensors->MaintenanceRequestedNodeCount = bundleProfiler.Gauge("/maintenance_requested_node_count");

        sensors->NodeAllocationRequestAge = bundleProfiler.TimeGauge("/node_allocation_request_age");
        sensors->NodeDeallocationRequestAge = bundleProfiler.TimeGauge("/node_deallocation_request_age");
        sensors->RemovingCellsAge = bundleProfiler.TimeGauge("/removing_cells_age");

        sensors->ProxyAllocationRequestAge = bundleProfiler.TimeGauge("/proxy_allocation_request_age");
        sensors->ProxyDeallocationRequestAge = bundleProfiler.TimeGauge("/proxy_deallocation_request_age");
        sensors->BundleCellsDowntime = bundleProfiler.TimeGauge("/bundle_cells_downtime");

        BundleSensors_[bundleName] = sensors;
        return sensors;
    }

    TZoneSensorsPtr GetZoneSensors(const TString& zoneName, const TString& dataCenter)
    {
        auto it = ZoneSensors_.find(zoneName);
        if (it != ZoneSensors_.end()) {
            return it->second;
        }

        auto sensors = New<TZoneSensors>();
        sensors->Profiler = Profiler.WithPrefix("/resource")
            .WithTag("zone", zoneName)
            .WithTag("data_center", dataCenter);
        auto& zoneProfiler = sensors->Profiler;

        sensors->OfflineNodeCount = zoneProfiler.Gauge("/offline_node_count");
        sensors->OfflineNodeThreshold = zoneProfiler.Gauge("/offline_node_threshold");

        sensors->OfflineProxyCount = zoneProfiler.Gauge("/offline_proxy_count");
        sensors->OfflineProxyThreshold = zoneProfiler.Gauge("/offline_proxy_threshold");

        sensors->FreeSpareNodeCount = zoneProfiler.Gauge("/free_spare_node_count");
        sensors->ExternallyDecommissionedNodeCount = zoneProfiler.Gauge("/externally_decommissioned_node_count");
        sensors->FreeSpareProxyCount = zoneProfiler.Gauge("/free_spare_proxy_count");
        sensors->RequiredSpareNodeCount = zoneProfiler.Gauge("/required_spare_nodes_count");

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

        inputState.SysConfig = GetSystemConfig(transaction);

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

    TChaosSchedulerInputState GetChaosInputState(ITransactionPtr& transaction, TForeignClientProvider* clientProvider) const
    {
        TChaosSchedulerInputState inputState{
            .Config = Config_,
        };

        inputState.TabletCellBundles = CypressList<TBundleInfo>(transaction, TabletCellBundlesPath);
        inputState.SysConfig = GetSystemConfig(transaction);

        const auto& chaosConfig = Config_->ChaosConfig;

        for (const auto& cluster : chaosConfig->TabletCellClusters) {
            YT_LOG_DEBUG("Loading tablet cell bundles for foreign cluster (Cluster: %v)",
                cluster);

            auto foreignClient = clientProvider->Get(cluster);
            inputState.ForeignTabletCellBundles[cluster] = CypressList<TBundleInfo>(foreignClient, TabletCellBundlesPath);
        }

        for (const auto& cluster : chaosConfig->ChaosCellClusters) {
            YT_LOG_DEBUG("Loading chaos cell bundles for foreign cluster (Cluster: %v)",
                cluster);

            auto foreignClient = clientProvider->Get(cluster);
            inputState.ForeignChaosCellBundles[cluster] = CypressList<TChaosBundleInfo>(foreignClient, ChaosCellBundlesPath);
        }

        inputState.GlobalRegistry = CypressGetSingleNode<TGlobalCellRegistryPtr>(transaction, GlobalCellRegistryPath);

        return inputState;
    }

    template <typename TEntryInfo>
    static TIndexedEntries<TEntryInfo> CypressList(const IClientBasePtr& transaction, const TYPath& path)
    {
        TListNodeOptions options;
        options.MaxSize = DefaultMaxSize;
        options.Attributes = TEntryInfo::GetAttributes();

        auto yson = WaitFor(transaction->ListNode(path, options))
            .ValueOrThrow();
        auto entryList = ConvertTo<IListNodePtr>(yson);

        if (entryList->Attributes().Get("incomplete", false)) {
            THROW_ERROR_EXCEPTION("Cypress list received incomplete results")
                << TErrorAttribute("path", path);
        }

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
        options.MaxSize = DefaultMaxSize;
        options.Attributes = TEntryInfo::GetAttributes();

        auto yson = WaitFor(transaction->GetNode(path, options))
            .ValueOrThrow();
        auto entryMap = ConvertTo<IMapNodePtr>(yson);

        if (entryMap->Attributes().Get("incomplete", false)) {
            THROW_ERROR_EXCEPTION("Cypress get received incomplete results")
                << TErrorAttribute("path", path);
        }

        TIndexedEntries<TEntryInfo> result;
        for (const auto& [name, entry] : entryMap->GetChildren()) {
            // Merging Cypress node attributes with node value.
            auto entryInfo = ConvertTo<TIntrusivePtr<TEntryInfo>>(&entry->Attributes());
            result[name] = UpdateYsonStruct(entryInfo, entry);
        }

        return result;
    }

    template <typename TEntryInfoPtr>
    static TEntryInfoPtr CypressGetSingleNode(const ITransactionPtr& transaction, const TYPath& path)
    {
        auto yson = WaitFor(transaction->GetNode(path))
            .ValueOrThrow();
        return ConvertTo<TEntryInfoPtr>(yson);
    }

    template <typename TEntryValue>
    static void CypressSetSingleNode(
        const ITransactionPtr& transaction,
        const TYPath& path,
        TEntryValue value)
    {
        WaitFor(transaction->SetNode(path, ConvertToYsonString(value)))
            .ThrowOnError();
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

    static void CreateSystemAccount(
        const IClientBasePtr& client,
        const TString& accountName)
    {
        TCreateObjectOptions createOptions;
        createOptions.Attributes = CreateEphemeralAttributes();
        createOptions.Attributes->Set("parent_name", "bundle_system_quotas");
        createOptions.Attributes->Set("name", accountName);

        WaitFor(client->CreateObject(EObjectType::Account, createOptions))
            .ThrowOnError();
    }

    static void CreateObject(
        EObjectType objectType,
        const IClientBasePtr& client,
        const NYTree::IAttributeDictionaryPtr& attributes)
    {
        TCreateObjectOptions createOptions;
        createOptions.Attributes = attributes;

        WaitFor(client->CreateObject(objectType, createOptions))
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
        const IClientBasePtr& client,
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
            WaitFor(client->SetNode(path, ConvertToYsonString(attribute), setOptions))
                .ThrowOnError();
        }
    }

    static void RemoveInstanceAttributes(
        const ITransactionPtr& transaction,
        const TYPath& instanceBasePath,
        const TString& attributeName,
        const THashSet<TString>& instancies)
    {
        for (const auto& instanceName : instancies) {
            auto path = Format("%v/%v/@%v",
                instanceBasePath,
                NYPath::ToYPathLiteral(instanceName),
                attributeName);

            YT_LOG_DEBUG("Removing attribute (Path: %v)",
                path);

            WaitFor(transaction->RemoveNode(path))
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

    static TSysConfigPtr GetSystemConfig(const IClientBasePtr& client)
    {
        auto yson = WaitFor(client->GetNode("//sys/@"))
            .ValueOrThrow();
        return ConvertTo<TSysConfigPtr>(yson);
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
            TString path = Format("%v/%v", TabletCellsPath, cellId);

            YT_LOG_INFO("Removing tablet cell (CellId: %v, Path: %v)",
                cellId,
                path);

            WaitFor(transaction->RemoveNode(path))
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
