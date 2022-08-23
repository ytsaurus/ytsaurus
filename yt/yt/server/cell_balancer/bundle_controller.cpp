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
        , AlarmCounter_(Profiler.Counter("/scan_bundles_alarms_count"))
        , DynamicConfigUpdateCounter_(Profiler.Counter("/dynamic_config_update_counter"))
        , InstanceAllocationCounter_(Profiler.Counter("/instance_allocation_counter"))
        , InstanceDeallocationCounter_(Profiler.Counter("/instance_deallocation_counter"))
        , CellCreationCounter_(Profiler.Counter("/cell_creation_counter"))
        , CellRemovalCounter_(Profiler.Counter("/cell_removal_counter"))
        , ChangedNodeUserTagCounter_(Profiler.Counter("/changed_node_user_tag_counter"))
        , ChangedDecommissionedFlagCounter_(Profiler.Counter("/changed_decommissioned_flag_counter"))
        , ChangedNodeAnnotationCounter_(Profiler.Counter("/changed_node_annotation_counter"))
        , InflightNodeAllocationCount_(Profiler.Gauge("/inflight_node_allocations_count"))
        , InflightNodeDeallocationCount_(Profiler.Gauge("/inflight_node_deallocations_count"))
        , InflightCellRemovalCount_(Profiler.Gauge("/inflight_cell_removal_count"))
        , NodeAllocationRequestAge_(Profiler.TimeGauge("/node_allocation_request_age"))
        , NodeDeallocationRequestAge_(Profiler.TimeGauge("/node_deallocation_request_age"))
        , RemovingCellsAge_(Profiler.TimeGauge("/removing_cells_age"))
        , InflightProxyAllocationCounter_(Profiler.Gauge("/inflight_proxy_allocation_counter"))
        , InflightProxyDeallocationCounter_(Profiler.Gauge("/inflight_proxy_deallocation_counter"))
        , ChangedProxyRoleCounter_(Profiler.Counter("/changed_proxy_role_counter"))
        , ChangedProxyAnnotationCounter_(Profiler.Counter("/changed_proxy_annotation_counter"))
        , ProxyAllocationRequestAge_(Profiler.TimeGauge("/proxy_allocation_request_age"))
        , ProxyDeallocationRequestAge_(Profiler.TimeGauge("/proxy_deallocation_request_age"))
        , ChangedSystemAccountLimitCounter_(Profiler.Counter("/changed_system_account_limit_counter"))
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
    TCounter AlarmCounter_;

    TCounter DynamicConfigUpdateCounter_;
    TCounter InstanceAllocationCounter_;
    TCounter InstanceDeallocationCounter_;
    TCounter CellCreationCounter_;
    TCounter CellRemovalCounter_;

    TCounter ChangedNodeUserTagCounter_;
    TCounter ChangedDecommissionedFlagCounter_;
    TCounter ChangedNodeAnnotationCounter_;

    TGauge InflightNodeAllocationCount_;
    TGauge InflightNodeDeallocationCount_;
    TGauge InflightCellRemovalCount_;

    TTimeGauge NodeAllocationRequestAge_;
    TTimeGauge NodeDeallocationRequestAge_;
    TTimeGauge RemovingCellsAge_;

    TGauge InflightProxyAllocationCounter_;
    TGauge InflightProxyDeallocationCounter_;
    TCounter ChangedProxyRoleCounter_;
    TCounter ChangedProxyAnnotationCounter_;
    TTimeGauge ProxyAllocationRequestAge_;
    TTimeGauge ProxyDeallocationRequestAge_;
    TCounter ChangedSystemAccountLimitCounter_;

    mutable Orchid::TBundlesInfo OrchidBundlesInfo_;

    void ScanBundles() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        if (!IsLeader()) {
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

    void LinkOrchidService() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        static const TYPath LeaderOrchidServicePath = "//sys/bundle_controller/orchid";
        static const TString AttributeRemoteAddress = "remote_addresses";

        auto addresses = Bootstrap_->GetLocalAddresses();

        auto client = Bootstrap_->GetClient();
        auto exists = WaitFor(client->NodeExists(LeaderOrchidServicePath))
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

        // Update input state for serving orchid requests.
        OrchidBundlesInfo_ = Orchid::GetBundlesInfo(inputState);
    }

    inline static const TString  AttributeBundleControllerAnnotations = "bundle_controller_annotations";
    inline static const TString  NodeAttributeUserTags = "user_tags";
    inline static const TString  NodeAttributeDecommissioned = "decommissioned";
    inline static const TString  ProxyAttributeRole = "role";
    inline static const TString  AccountAttributeResourceLimits = "resource_limits";

    void Mutate(const ITransactionPtr& transaction, const TSchedulerMutations& mutations) const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        CreateHulkRequests<TAllocationRequest>(transaction, Config_->HulkAllocationsPath, mutations.NewAllocations);
        CreateHulkRequests<TDeallocationRequest>(transaction, Config_->HulkDeallocationsPath, mutations.NewDeallocations);
        CypressSet(transaction, GetBundlesStatePath(), mutations.ChangedStates);

        SetNodeAttributes(transaction, AttributeBundleControllerAnnotations, mutations.ChangeNodeAnnotations);
        SetNodeAttributes(transaction, NodeAttributeUserTags, mutations.ChangedNodeUserTags);
        SetNodeAttributes(transaction, NodeAttributeDecommissioned, mutations.ChangedDecommissionedFlag);

        SetProxyAttributes(transaction, AttributeBundleControllerAnnotations, mutations.ChangedProxyAnnotations);
        SetProxyAttributes(transaction, ProxyAttributeRole, mutations.ChangedProxyRole);

        SetInstanceAttributes(transaction, BundleSystemQuotasPath, AccountAttributeResourceLimits, mutations.ChangedSystemAccountLimit);
        SetRootSystemAccountLimits(transaction, mutations.ChangedRootSystemAccountLimit);

        CreateTabletCells(transaction, mutations.CellsToCreate);
        RemoveTabletCells(transaction, mutations.CellsToRemove);

        if (mutations.DynamicConfig) {
            DynamicConfigUpdateCounter_.Increment();
            SetBundlesDynamicConfig(transaction, *mutations.DynamicConfig);
        }

        // TODO(capone212): Fire alarms.

        AlarmCounter_.Increment(mutations.AlertsToFire.size());
        InstanceAllocationCounter_.Increment(mutations.NewAllocations.size());
        InstanceDeallocationCounter_.Increment(mutations.NewDeallocations.size());
        CellCreationCounter_.Increment(mutations.CellsToCreate.size());
        CellRemovalCounter_.Increment(mutations.CellsToRemove.size());
        ChangedNodeUserTagCounter_.Increment(mutations.ChangedNodeUserTags.size());
        ChangedDecommissionedFlagCounter_.Increment(mutations.ChangedDecommissionedFlag.size());
        ChangedNodeAnnotationCounter_.Increment(mutations.ChangeNodeAnnotations.size());

        ChangedProxyRoleCounter_.Increment(mutations.ChangedProxyRole.size());
        ChangedProxyAnnotationCounter_.Increment(mutations.ChangedProxyAnnotations.size());
        ChangedSystemAccountLimitCounter_.Increment(mutations.ChangedSystemAccountLimit.size());

        int nodeAllocationCount = 0;
        int nodeDeallocationCount = 0;
        int proxyAllocationCount = 0;
        int proxyDeallocationCount = 0;
        int removingCellCount = 0;
        auto now = TInstant::Now();

        // TODO(capone212): think about per-bundle sensors.
        for (const auto& [_, state] : mutations.ChangedStates) {
            nodeAllocationCount += state->NodeAllocations.size();
            nodeDeallocationCount += state->NodeDeallocations.size();
            removingCellCount += state->RemovingCells.size();

            proxyAllocationCount += state->ProxyAllocations.size();
            proxyDeallocationCount += state->ProxyDeallocations.size();

            for (const auto& [_, allocation] : state->NodeAllocations) {
                NodeAllocationRequestAge_.Update(now - allocation->CreationTime);
            }

            for (const auto& [_, deallocation] : state->NodeDeallocations) {
                NodeDeallocationRequestAge_.Update(now - deallocation->CreationTime);
            }

            for (const auto& [_, removingCell] : state->RemovingCells) {
                RemovingCellsAge_.Update(now - removingCell->RemovedTime);
            }

            for (const auto& [_, allocation] : state->ProxyAllocations) {
                ProxyAllocationRequestAge_.Update(now - allocation->CreationTime);
            }

            for (const auto& [_, deallocation] : state->ProxyDeallocations) {
                ProxyDeallocationRequestAge_.Update(now - deallocation->CreationTime);
            }
        }

        InflightNodeAllocationCount_.Update(nodeAllocationCount);
        InflightNodeDeallocationCount_.Update(nodeDeallocationCount);
        InflightCellRemovalCount_.Update(removingCellCount);
        InflightProxyAllocationCounter_.Update(proxyAllocationCount);
        InflightProxyDeallocationCounter_.Update(proxyDeallocationCount);

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

        inputState.Zones = CypressList<TZoneInfo>(transaction, GetZonesPath());
        inputState.Bundles = CypressList<TBundleInfo>(transaction, TabletCellBundlesPath);
        inputState.BundleStates = CypressList<TBundleControllerState>(transaction, GetBundlesStatePath());
        inputState.TabletNodes = CypressList<TTabletNodeInfo>(transaction, TabletNodesPath);
        inputState.TabletCells = CypressList<TTabletCellInfo>(transaction, TabletCellsPath);
        inputState.RpcProxies = CypressGet<TRpcProxyInfo>(transaction, RpcProxiesPath);
        inputState.SystemAccounts = CypressList<TSystemAccount>(transaction, BundleSystemQuotasPath);
        inputState.RootSystemAccount = GetRootSystemAccount(transaction);
        inputState.RpcProxies = CypressGet<TRpcProxyInfo>(transaction, RpcProxiesPath);

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
                NYPath::ToYPathLiteral(attributeName));

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
