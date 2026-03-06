#include "bundle_scheduler.h"

#include "allocator_adapter.h"
#include "config.h"
#include "cypress_bindings.h"
#include "helpers.h"
#include "input_state.h"
#include "instance_manager.h"
#include "mutations.h"
#include "pod_id_helpers.h"
#include "system_accounts.h"

#include <library/cpp/yt/yson_string/public.h>

#include <util/string/subst.h>

namespace NYT::NCellBalancer {

using namespace NBundleControllerClient;
using namespace NLogging;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

std::string GenerateShortNameForBundle(
    const std::string& bundleName,
    const THashMap<std::string, std::string>& shortNameToBundle,
    int maxLength)
{
    auto shortName = bundleName;

    // pod id can not contain '_'
    SubstGlobal(shortName, '_', '-');
    if (std::ssize(shortName) <= maxLength && shortNameToBundle.count(shortName) == 0) {
        return shortName;
    }

    shortName.resize(maxLength - 1);

    for (int index = 1; index < 100; ++index) {
        if (index == 10) {
            shortName.resize(maxLength - 2);
        }

        auto proposed = Format("%v%v", shortName, index);

        if (shortNameToBundle.count(proposed) == 0) {
            return proposed;
        }
    }

    THROW_ERROR_EXCEPTION("Cannot generate short name for bundle")
        << TErrorAttribute("bundle_name", bundleName);
}

THashMap<std::string, std::string> MapBundlesToShortNames(const TSchedulerInputState& input)
{
    THashMap<std::string, std::string> bundleToShortName;
    THashMap<std::string, std::string> shortNameToBundle;

    // Instance pod-id looks like sas3-4993-venus212-001-rpc-hume.
    // Max pod id length is 35, so cluster short name and bundle short name
    // should be under 16 characters.
    constexpr int MaxBundlePlusClusterNamesLength = 16;
    constexpr int MinBundleShortName = 4;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (bundleInfo->ShortName) {
            bundleToShortName[bundleName] = *bundleInfo->ShortName;
            shortNameToBundle[*bundleInfo->ShortName] = bundleName;
        }
    }

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (auto it = bundleToShortName.find(bundleName); it != bundleToShortName.end()) {
            continue;
        }

        auto it = input.Zones.find(bundleInfo->Zone);
        if (it == input.Zones.end()) {
            continue;
        }

        auto clusterName = it->second->ShortName.value_or(input.Config->Cluster);
        int maxShortNameLength = MaxBundlePlusClusterNamesLength - clusterName.size();

        THROW_ERROR_EXCEPTION_UNLESS(
            maxShortNameLength >= MinBundleShortName,
            "Please set cluster short name, cluster name it too long");

        auto shortName = GenerateShortNameForBundle(bundleName, shortNameToBundle, maxShortNameLength);
        YT_VERIFY(std::ssize(shortName) <= maxShortNameLength);

        bundleToShortName[bundleName] = shortName;
        shortNameToBundle[shortName] = bundleName;
    }

    return bundleToShortName;
}

std::string GetInstanceSize(const TInstanceResourcesPtr& resource)
{
    auto cpuCores = resource->Vcpu / 1000;
    auto memoryGB = resource->Memory / 1_GB;

    return Format("%vCPUx%vGB", cpuCores, memoryGB);
}

void CalculateResourceUsage(TSchedulerInputState& input)
{
    THashMap<std::string, TInstanceResourcesPtr> aliveResources;
    THashMap<std::string, TInstanceResourcesPtr> allocatedResources;
    THashMap<std::string, TInstanceResourcesPtr> targetResources;

    auto calculateResources = [] (
        const auto& aliveNames,
        const auto& instancesInfo,
        TInstanceResourcesPtr& target,
        auto& countBySize)
    {
        for (const auto& instanceName : aliveNames) {
            const auto& instanceInfo = GetOrCrash(instancesInfo, instanceName);
            const auto& resource = instanceInfo->BundleControllerAnnotations->Resource;
            target->Vcpu += resource->Vcpu;
            target->Memory += resource->Memory;
            ++countBySize[GetInstanceSize(resource)];
        }
    };

    input.AliveNodesBySize.clear();
    input.AllocatedNodesBySize.clear();

    input.AliveProxiesBySize.clear();
    input.AllocatedProxiesBySize.clear();

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->TargetConfig) {
            continue;
        }

        {
            auto aliveResourceUsage = New<TInstanceResources>();
            // Default values are non-zero, so we need to clear them.
            aliveResourceUsage->Clear();

            TBundleControllerStatePtr bundleState;
            if (auto it = input.BundleStates.find(bundleName); it != input.BundleStates.end()) {
                bundleState = NYTree::CloneYsonStruct(it->second);
            }

            auto perDCaliveNodes = GetAliveNodes(
                bundleName,
                input.BundleNodes[bundleName],
                input,
                bundleState,
                EGracePeriodBehaviour::Wait);

            auto aliveNodes = FlattenAliveInstances(perDCaliveNodes);
            calculateResources(aliveNodes, input.TabletNodes, aliveResourceUsage, input.AliveNodesBySize[bundleName]);

            auto aliveProxies = FlattenAliveInstances(GetAliveProxies(input.BundleProxies[bundleName], input, EGracePeriodBehaviour::Wait));
            calculateResources(aliveProxies, input.RpcProxies, aliveResourceUsage, input.AliveProxiesBySize[bundleName]);

            aliveResources[bundleName] = aliveResourceUsage;
        }

        {
            auto allocated = New<TInstanceResources>();
            // Default values are non-zero, so we need to clear them.
            allocated->Clear();

            calculateResources(FlattenBundleInstances(input.BundleNodes[bundleName]), input.TabletNodes, allocated, input.AllocatedNodesBySize[bundleName]);
            calculateResources(FlattenBundleInstances(input.BundleProxies[bundleName]), input.RpcProxies, allocated, input.AllocatedProxiesBySize[bundleName]);

            allocatedResources[bundleName] = allocated;
        }

        {
            const auto& targetConfig = bundleInfo->TargetConfig;
            const auto& nodeGuarantee = targetConfig->TabletNodeResourceGuarantee;
            const auto& proxyGuarantee = targetConfig->RpcProxyResourceGuarantee;

            auto targetResource = New<TInstanceResources>();
            targetResource->Vcpu = nodeGuarantee->Vcpu * targetConfig->TabletNodeCount + proxyGuarantee->Vcpu * targetConfig->RpcProxyCount;
            targetResource->Memory = nodeGuarantee->Memory * targetConfig->TabletNodeCount + proxyGuarantee->Memory * targetConfig->RpcProxyCount;
            targetResource->NetBytes = nodeGuarantee->NetBytes.value_or(0) * targetConfig->TabletNodeCount + proxyGuarantee->NetBytes.value_or(0) * targetConfig->RpcProxyCount;

            targetResources[bundleName] = targetResource;
        }
    }

    input.BundleResourceAlive = aliveResources;
    input.BundleResourceAllocated = allocatedResources;
    input.BundleResourceTarget = targetResources;
}

THashMap<std::string, THashSet<std::string>> GetAliveNodes(
    const std::string& bundleName,
    const TDataCenterToInstanceMap& bundleNodes,
    const TSchedulerInputState& input,
    const TBundleControllerStatePtr& bundleState,
    EGracePeriodBehaviour gracePeriodBehaviour)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    THashMap<std::string, THashSet<std::string>> aliveNodes;

    auto now = TInstant::Now();

    for (const auto& [dataCenterName, dataCenterNodes] : bundleNodes) {
        for (const auto& nodeName : dataCenterNodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            if (!nodeInfo->BundleControllerAnnotations->Allocated || nodeInfo->Banned) {
                continue;
            }

            bool internallyDecommissioned = bundleState &&
                (bundleState->BundleNodeAssignments.contains(nodeName) ||
                bundleState->BundleNodeReleasements.contains(nodeName));
            // May be null in tests.
            if (bundleState) {
                for (const auto& [_, deallocation] : bundleState->NodeDeallocations) {
                    if (deallocation->InstanceName == nodeName) {
                        internallyDecommissioned = true;
                    }
                }
            }

            if (nodeInfo->DisableTabletCells) {
                YT_LOG_DEBUG("Tablet cells are disabled for the node (BundleName: %v, Node: %v)",
                    bundleName,
                    nodeName);
                continue;
            }

            if (!bundleInfo->NodeTagFilter.empty() && nodeInfo->Decommissioned && !internallyDecommissioned) {
                YT_LOG_DEBUG("Node is externally decommissioned (BundleName: %v, Node: %v)",
                    bundleName,
                    nodeName);
                continue;
            }

            if (nodeInfo->State != InstanceStateOnline) {
                if (gracePeriodBehaviour == EGracePeriodBehaviour::Immediately ||
                    now - nodeInfo->LastSeenTime > input.Config->OfflineInstanceGracePeriod)
                {
                    continue;
                }
            }

            aliveNodes[dataCenterName].insert(nodeName);
        }
    }

    return aliveNodes;
}

THashMap<std::string, THashSet<std::string>> GetAliveProxies(
    const TDataCenterToInstanceMap& bundleProxies,
    const TSchedulerInputState& input,
    EGracePeriodBehaviour gracePeriodBehaviour)
{
    THashMap<std::string, THashSet<std::string>> aliveProxies;
    auto now = TInstant::Now();

    for (const auto& [dataCenterName, dataCenterProxies] : bundleProxies) {
        for (const auto& proxyName : dataCenterProxies) {
            const auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
            if (!proxyInfo->BundleControllerAnnotations->Allocated || proxyInfo->Banned) {
                continue;
            }

            if (!proxyInfo->Alive) {
                if (gracePeriodBehaviour == EGracePeriodBehaviour::Immediately ||
                    now - proxyInfo->ModificationTime > input.Config->OfflineInstanceGracePeriod)
                {
                    continue;
                }
            }

            aliveProxies[dataCenterName].insert(proxyName);
        }
    }

    return aliveProxies;
}

bool EnsureNodeDecommissioned(
    const std::string& nodeName,
    const TTabletNodeInfoPtr& nodeInfo,
    TSchedulerMutations* mutations)
{
    if (!nodeInfo->Decommissioned) {
        mutations->ChangedDecommissionedFlag[nodeName] = mutations->WrapMutation(true);
        return false;
    }
    // Wait tablet cells to migrate.
    return GetUsedSlotCount(nodeInfo) == 0;
}

struct TTabletCellRemoveOrder
{
    std::string Id;
    std::string HostNode;
    bool Disrupted = false;
    // No tablet host and non zero tablet nodes

    auto AsTuple() const
    {
        return std::tie(Disrupted, HostNode, Id);
    }

    bool operator<(const TTabletCellRemoveOrder& other) const
    {
        return AsTuple() < other.AsTuple();
    }
};

std::string GetHostNodeForCell(const TTabletCellInfoPtr& cellInfo, const THashSet<std::string>& bundleNodes)
{
    std::string nodeName;

    for (const auto& peer : cellInfo->Peers) {
        if (bundleNodes.count(peer->Address) == 0) {
            continue;
        }

        if (nodeName.empty() || peer->State == PeerStateLeading) {
            nodeName = peer->Address;
        }
    }

    return nodeName;
}

std::vector<std::string> PickTabletCellsToRemove(
    int cellCountToRemove,
    const std::vector<std::string>& bundleCellIds)
{
    YT_VERIFY(std::ssize(bundleCellIds) >= cellCountToRemove);

    std::vector<std::string> result;
    result.reserve(bundleCellIds.size());

    for (auto& cell : bundleCellIds) {
        result.push_back(cell);
    }

    // add some determinism
    std::sort(result.begin(), result.end());
    result.resize(cellCountToRemove);
    return result;
}

void ProcessRemovingCells(
    const std::string& bundleName,
    const TDataCenterToInstanceMap& /*bundleNodes*/,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto& state = mutations->ChangedStates[bundleName];
    std::vector<std::string> removeCompleted;

    for (const auto& [cellId, removingStateInfo] : state->RemovingCells) {
        auto it = input.TabletCells.find(cellId);
        if (it == input.TabletCells.end()) {
            YT_LOG_INFO("Tablet cell removal finished (BundleName: %v, TabletCellId: %v)",
                bundleName,
                cellId);
            removeCompleted.push_back(cellId);
            continue;
        }

        auto removingTime = TInstant::Now() - removingStateInfo->RemovedTime;

        if (removingTime > input.Config->CellRemovalTimeout) {
            YT_LOG_WARNING("Tablet cell removal is stuck (BundleName: %v, TabletCellId: %v, RemovingTime: %v, Threshold: %v)",
                bundleName,
                cellId,
                removingTime,
                input.Config->CellRemovalTimeout);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "stuck_tablet_cell_removal",
                .BundleName = bundleName,
                .Description = Format("Found stuck tablet cell %v removal "
                    " with removing time %v, which is more than threshold %v.",
                    cellId,
                    removingTime,
                    input.Config->CellRemovalTimeout),
            });
        }

        YT_LOG_DEBUG("Tablet cell removal in progress"
            " (BundleName: %v, TabletCellId: %v)",
            bundleName,
            cellId);
    }

    for (const auto& cellId : removeCompleted) {
        state->RemovingCells.erase(cellId);
    }
}

void CreateRemoveTabletCells(
    const std::string& bundleName,
    const TDataCenterToInstanceMap& bundleNodes,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& bundleState = GetOrCrash(mutations->ChangedStates, bundleName);

    if (!bundleInfo->EnableTabletCellManagement) {
        return;
    }

    const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);

    auto aliveNodes = FlattenAliveInstances(GetAliveNodes(
        bundleName,
        bundleNodes,
        input,
        mutations->ChangedStates[bundleName],
        EGracePeriodBehaviour::Wait));

    if (std::ssize(aliveNodes) < bundleInfo->TargetConfig->TabletNodeCount ||
        !bundleState->NodeAllocations.empty() ||
        !bundleState->NodeDeallocations.empty())
    {
        // It is better not to mix node allocations with tablet cell management.
        return;
    }

    if (!bundleState->RemovingCells.empty()) {
        // Do not do anything with cells while tablet cell removal is in progress.
        return;
    }

    int targetCellCount = GetTargetCellCount(bundleInfo, zoneInfo);
    int cellCountDiff = targetCellCount - std::ssize(bundleInfo->TabletCellIds);

    YT_LOG_DEBUG("Managing tablet cells (BundleName: %v, TargetCellCount: %v, ExistingCount: %v)",
        bundleName,
        targetCellCount,
        std::ssize(bundleInfo->TabletCellIds));

    if (cellCountDiff < 0) {
        auto cellsToRemove = PickTabletCellsToRemove(std::abs(cellCountDiff), bundleInfo->TabletCellIds);

        YT_LOG_INFO("Removing tablet cells (BundleName: %v, CellIds: %v)",
            bundleName,
            cellsToRemove);

        mutations->CellsToRemove.reserve(cellsToRemove.size());
        for (const auto& cellId : cellsToRemove) {
            mutations->CellsToRemove.push_back(mutations->WrapMutation(cellId));
        }

        for (auto& cellId : cellsToRemove) {
            auto removingCellState = New<TRemovingTabletCellState>();
            removingCellState->RemovedTime = TInstant::Now();
            bundleState->RemovingCells[cellId] = removingCellState;
        }
    } else if (cellCountDiff > 0) {
        YT_LOG_INFO("Creating tablet cells (BundleName: %v, CellCount: %v)",
            bundleName,
            cellCountDiff);

        mutations->CellsToCreate[bundleName] = cellCountDiff;
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageResourceLimits(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController ||
            !bundleInfo->EnableTabletCellManagement ||
            !bundleInfo->EnableResourceLimitsManagement)
        {
            continue;
        }

        auto zoneIt = input.Zones.find(bundleInfo->Zone);
        if (zoneIt == input.Zones.end()) {
            continue;
        }

        const auto& targetConfig = bundleInfo->TargetConfig;
        if (!targetConfig->MemoryLimits->TabletStatic) {
            continue;
        }

        auto availableTabletStatic = *targetConfig->MemoryLimits->TabletStatic * targetConfig->TabletNodeCount;

        if (availableTabletStatic != bundleInfo->ResourceLimits->TabletStaticMemory) {
            YT_LOG_INFO("Adjusting tablet static memory limit (BundleName: %v, NewValue: %v, OldValue: %v)",
                bundleName,
                availableTabletStatic,
                bundleInfo->ResourceLimits->TabletStaticMemory);

            mutations->ChangedTabletStaticMemory[bundleName] = availableTabletStatic;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string GetSpareBundleName(const TZoneInfoPtr& zoneInfo)
{
    return zoneInfo->SpareBundleName;
}

THashMap<TSchedulerInputState::TQualifiedDCName, TDataCenterDisruptedState> GetDataCenterDisruptedState(const TSchedulerInputState& input)
{
    using TQualifiedDCName = TSchedulerInputState::TQualifiedDCName;

    THashMap<TQualifiedDCName, int> zoneOfflineNodeCount;

    for (const auto& [zoneName, zoneNodes] : input.ZoneNodes) {
        for (const auto& [dataCenterName, dataCenterNodes] : zoneNodes.PerDataCenter) {
            for (const auto& nodeName : dataCenterNodes) {
                const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
                if (nodeInfo->State == InstanceStateOnline) {
                    continue;
                }

                YT_LOG_DEBUG("Node is offline (NodeName: %v, NannyService: %v, Banned: %v, LastSeen: %v)",
                    nodeName,
                    nodeInfo->BundleControllerAnnotations->NannyService,
                    nodeInfo->Banned,
                    nodeInfo->LastSeenTime);

                ++zoneOfflineNodeCount[std::pair(zoneName, dataCenterName)];
            }
        }
    }

    THashMap<TQualifiedDCName, int> zoneOfflineProxyCount;
    for (const auto& [zoneName, zoneProxies] : input.ZoneProxies) {
        for (const auto& [dataCenterName, dataCenterProxies] : zoneProxies.PerDataCenter) {
            for (const auto& proxyName : dataCenterProxies) {
                const auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
                if (proxyInfo->Alive) {
                    continue;
                }

                YT_LOG_DEBUG("Proxy is offline (ProxyName: %v, NannyService: %v)",
                    proxyName,
                    proxyInfo->BundleControllerAnnotations->NannyService);

                ++zoneOfflineProxyCount[std::pair(zoneName, dataCenterName)];
            }
        }
    }

    THashMap<TQualifiedDCName, TDataCenterDisruptedState> result;
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        for (const auto& [dataCenterName, dataCenterInfo] : zoneInfo->DataCenters) {
            auto& dataCenterDisrupted = result[std::pair(zoneName, dataCenterName)];

            dataCenterDisrupted.OfflineNodeCount = zoneOfflineNodeCount[std::pair(zoneName, dataCenterName)];
            dataCenterDisrupted.OfflineNodeThreshold = zoneInfo->SpareTargetConfig->TabletNodeCount * zoneInfo->DisruptedThresholdFactor / std::ssize(zoneInfo->DataCenters);

            YT_LOG_WARNING_IF(dataCenterDisrupted.IsNodesDisrupted(), "Zone data center is in disrupted state"
                " (ZoneName: %v, DataCenter: %v, NannyService: %v, DisruptedThreshold: %v, OfflineNodeCount: %v)",
                zoneName,
                dataCenterName,
                dataCenterInfo->TabletNodeNannyService,
                dataCenterDisrupted.OfflineNodeThreshold,
                dataCenterDisrupted.OfflineNodeCount);

            dataCenterDisrupted.OfflineProxyThreshold = zoneInfo->SpareTargetConfig->RpcProxyCount * zoneInfo->DisruptedThresholdFactor / std::ssize(zoneInfo->DataCenters);
            dataCenterDisrupted.OfflineProxyCount = zoneOfflineProxyCount[std::pair(zoneName, dataCenterName)];

            YT_LOG_WARNING_IF(dataCenterDisrupted.IsProxiesDisrupted(), "Zone data center is in disrupted state"
                " (ZoneName: %v, DataCenter: %v, NannyService: %v, DisruptedThreshold: %v, OfflineProxyCount: %v)",
                zoneName,
                dataCenterName,
                dataCenterInfo->RpcProxyNannyService,
                dataCenterDisrupted.OfflineProxyThreshold,
                dataCenterDisrupted.OfflineProxyCount);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool IsOnline(const TTabletNodeInfoPtr& node)
{
    return node->State == InstanceStateOnline;
}

bool IsOnline(const TRpcProxyInfoPtr& proxy)
{
    return !!proxy->Alive;
}

template <class TInstanceMap>
THashSet<std::string> ScanForObsoleteCypressNodes(const TSchedulerInputState& input, const TInstanceMap& instanceMap)
{
    THashSet<std::string> result;
    auto obsoleteThreshold = input.Config->RemoveInstanceCypressNodeAfter;
    auto now = TInstant::Now();

    for (const auto& [instanceName, instanceInfo] : instanceMap) {
        auto bundleControllerAnnotations = instanceInfo->BundleControllerAnnotations;
        if (bundleControllerAnnotations->Allocated ||  !bundleControllerAnnotations->DeallocatedAt) {
            continue;
        }
        if (bundleControllerAnnotations->DeallocationStrategy != DeallocationStrategyHulkRequest) {
            continue;
        }

        if (now - *bundleControllerAnnotations->DeallocatedAt < obsoleteThreshold) {
            continue;
        }

        if (IsOnline(instanceInfo)) {
            YT_LOG_WARNING("Skipping obsolete Cypress node in online state (InstanceName: %v)", instanceName);
            continue;
        }

        result.insert(instanceName);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void InitializeBundleChangedStates(const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        const auto& zoneName = bundleInfo->Zone;

        if (auto zoneIt = input.Zones.find(zoneName); zoneIt == input.Zones.end()) {
            continue;
        }

        auto& bundleState = mutations->ChangedStates[bundleName];
        if (auto it = input.BundleStates.find(bundleName); it != input.BundleStates.end()) {
            bundleState = NYTree::CloneYsonStruct(it->second);
        } else {
            bundleState = New<TBundleControllerState>();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageInstances(
    TSchedulerInputState& input,
    ISpareInstanceAllocatorPtr spareNodesAllocator,
    ISpareInstanceAllocatorPtr spareProxiesAllocator,
    TSchedulerMutations* mutations)
{
    TInstanceManager nodeAllocator;
    TInstanceManager proxyAllocator;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        const auto& zoneName = bundleInfo->Zone;

        if (auto zoneIt = input.Zones.find(zoneName); zoneIt == input.Zones.end()) {
            continue;
        }

        auto bundleState = GetOrCrash(mutations->ChangedStates, bundleName);

        if (!bundleInfo->EnableInstanceAllocation) {
            continue;
        }

        const auto& bundleNodes = input.BundleNodes[bundleName];
        auto aliveNodes = GetAliveNodes(
            bundleName,
            bundleNodes,
            input,
            bundleState,
            EGracePeriodBehaviour::Wait);
        auto nodeAdapter = CreateTabletNodeAllocatorAdapter(bundleState, bundleNodes, aliveNodes);
        nodeAllocator.ManageInstances(bundleName, nodeAdapter.Get(), spareNodesAllocator, input, mutations);

        const auto& bundleProxies = input.BundleProxies[bundleName];
        auto aliveProxies = GetAliveProxies(bundleProxies, input, EGracePeriodBehaviour::Wait);
        auto proxyAdapter = CreateRpcProxyAllocatorAdapter(bundleState, bundleProxies, aliveProxies);
        proxyAllocator.ManageInstances(bundleName, proxyAdapter.Get(), spareProxiesAllocator, input, mutations);
    }

    if (input.Config->HasInstanceAllocatorService) {
        mutations->NodesToCleanup = ScanForObsoleteCypressNodes(input, input.TabletNodes);
        mutations->ProxiesToCleanup = ScanForObsoleteCypressNodes(input, input.RpcProxies);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageCells(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        if (auto zoneIt = input.Zones.find(bundleInfo->Zone); zoneIt == input.Zones.end()) {
            continue;
        }

        const auto& bundleNodes = input.BundleNodes[bundleName];
        CreateRemoveTabletCells(bundleName, bundleNodes, input, mutations);
        ProcessRemovingCells(bundleName, bundleNodes, input, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

TCpuLimitsPtr GetBundleEffectiveCpuLimits(
    const std::string& bundleName,
    const TBundleInfoPtr& bundleInfo,
    const TSchedulerInputState& input)
{
    auto currentCpuLimits = NYTree::CloneYsonStruct(bundleInfo->TargetConfig->CpuLimits);

    if (bundleInfo->NodeTagFilter.empty()) {
        return currentCpuLimits;
    }

    auto previousConfigIt = input.DynamicConfig.find(bundleInfo->NodeTagFilter);
    if (previousConfigIt == input.DynamicConfig.end()) {
        return currentCpuLimits;
    }

    const auto& previousConfig = previousConfigIt->second;

    auto previousCpuLimits = previousConfig->CpuLimits;

    if (currentCpuLimits->WriteThreadPoolSize >= previousCpuLimits->WriteThreadPoolSize) {
        return currentCpuLimits;
    }

    const auto& zoneName = bundleInfo->Zone;
    const auto& zoneInfo = GetOrCrash(input.Zones, zoneName);

    auto targetCellCount = GetTargetCellCount(bundleInfo, zoneInfo);
    auto currentCellCount = std::ssize(bundleInfo->TabletCellIds);

    int removingCellCount = 0;

    auto bundleStateIt = input.BundleStates.find(bundleName);
    if (bundleStateIt != input.BundleStates.end()) {
        const auto& bundleState = bundleStateIt->second;
        removingCellCount = std::ssize(bundleState->RemovingCells);
    }

    if (currentCellCount > targetCellCount || removingCellCount > 0) {
        YT_LOG_DEBUG("Will not set new bundle dynamic config with reduced \"write_thread_pool_size\" since not all cells are removed "
            "(BundleName: %v, CurrentCellCount: %v, TargetCellCount: %v, RemovingCellCount: %v, "
            "OldCpuLimits: %v, NewCpuLimits: %v)",
            bundleName,
            currentCellCount,
            targetCellCount,
            removingCellCount,
            ConvertToYsonString(previousCpuLimits, EYsonFormat::Text),
            ConvertToYsonString(currentCpuLimits, EYsonFormat::Text));
        return previousCpuLimits;
    }

    return currentCpuLimits;
}

////////////////////////////////////////////////////////////////////////////////

void ManageBundlesDynamicConfig(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    TBundlesDynamicConfig freshConfig;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController || !bundleInfo->EnableTabletNodeDynamicConfig) {
            continue;
        }

        if (bundleInfo->NodeTagFilter.empty()) {
            YT_LOG_WARNING("Bundle has empty node tag filter (BundleName: %v)", bundleName);
            continue;
        }

        auto bundleConfig = New<TBundleDynamicConfig>();
        bundleConfig->CpuLimits = GetBundleEffectiveCpuLimits(bundleName, bundleInfo, input);
        bundleConfig->MemoryLimits = NYTree::CloneYsonStruct(bundleInfo->TargetConfig->MemoryLimits);
        bundleConfig->MediumThroughputLimits = NYTree::CloneYsonStructs(bundleInfo->TargetConfig->MediumThroughputLimits);
        freshConfig[bundleInfo->NodeTagFilter] = bundleConfig;
    }

    if (AreNodesEqual(ConvertTo<NYTree::IMapNodePtr>(freshConfig), ConvertTo<NYTree::IMapNodePtr>(input.DynamicConfig))) {
        return;
    }

    YT_LOG_INFO("Bundles dynamic config has changed (Config: %v)",
        ConvertToYsonString(freshConfig, EYsonFormat::Text));

    mutations->DynamicConfig = freshConfig;
}

////////////////////////////////////////////////////////////////////////////////

TIndexedEntries<TBundleControllerState> GetActuallyChangedStates(
    const TSchedulerInputState& input,
    const TSchedulerMutations& mutations)
{
    const auto inputStates = input.BundleStates;
    std::vector<std::string> unchangedBundleStates;

    for (auto [bundleName, possiblyChangedState] : mutations.ChangedStates) {
        auto it = inputStates.find(bundleName);
        if (it == inputStates.end()) {
            continue;
        }

        if (AreNodesEqual(ConvertTo<NYTree::INodePtr>(it->second), ConvertTo<NYTree::INodePtr>(possiblyChangedState))) {
            unchangedBundleStates.push_back(bundleName);
        } else {
            YT_LOG_DEBUG("Bundle state changed (Bundle: %v, OldState: %v, NewState: %v)",
                bundleName,
                ConvertToYsonString(it->second, EYsonFormat::Text),
                ConvertToYsonString(possiblyChangedState, EYsonFormat::Text));
        }
    }

    auto result = mutations.ChangedStates;
    for (const auto& unchanged : unchangedBundleStates) {
        result.erase(unchanged);
    }

    return result;
}

void ManageBundleShortName(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, shortName] : input.BundleToShortName) {
        const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        if (bundleName == shortName || (bundleInfo->ShortName && *bundleInfo->ShortName == shortName)) {
            continue;
        }

        YT_LOG_INFO("Assigning short name for bundle (Bundle: %v, ShortName: %v)",
            bundleName,
            shortName);

        mutations->ChangedBundleShortName[bundleName] = shortName;
    }
}

void InitializeNodeTagFilters(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController || bundleInfo->Zone.empty()) {
            continue;
        }

        if (!bundleInfo->EnableNodeTagFilterManagement) {
            continue;
        }

        if (bundleInfo->Areas.empty()) {
            YT_LOG_WARNING("Bundle does not have any tablet cell area (BundleName: %v)",
                bundleName);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "no_areas_found",
                .BundleName = bundleName,
                .Description = Format("Bundle does not have any tablet cell area"),
            });

            bundleInfo->EnableBundleController = false;

            continue;
        }

        if (bundleInfo->NodeTagFilter.empty()) {
            auto nodeTagFilter = Format("%v/%v", bundleInfo->Zone, bundleName);
            bundleInfo->NodeTagFilter = nodeTagFilter;
            mutations->ChangedNodeTagFilters[bundleName] = mutations->WrapMutation(static_cast<std::string>(nodeTagFilter));

            YT_LOG_INFO("Initializing node tag filter for bundle (Bundle: %v, NodeTagFilter: %v)",
                bundleName,
                nodeTagFilter);
        }
    }
}

std::string GetDrillsNodeTagFilter(const TBundleInfoPtr& bundleInfo, const std::string& bundleName)
{
    return Format("%v/%v_drills_mode_on", bundleInfo->Zone, bundleName);
}

std::string GetNodeTagFilter(const TBundleInfoPtr& bundleInfo, const std::string& bundleName)
{
    return Format("%v/%v", bundleInfo->Zone, bundleName);
}

void ProcessEnableDrillsMode(const std::string& bundleName, const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    auto& bundleState = GetOrCrash(mutations->ChangedStates, bundleName);
    const auto& drillsMode = bundleState->DrillsMode;
    if (!drillsMode->TurningOn) {
        return;
    }

    YT_LOG_DEBUG("Processing turning on drills mode (BundleName: %v)",
        bundleName);

    auto operationAge = TInstant::Now() - drillsMode->TurningOn->CreationTime;
    if (operationAge > input.Config->NodeAssignmentTimeout) {
        YT_LOG_WARNING("Turning on drills mode is stuck (BundleName: %v, OperationAge: %v, Threshold: %v)",
            bundleName,
            operationAge,
            input.Config->NodeAssignmentTimeout);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "stuck_drills_mode",
            .BundleName = bundleName,
            .Description = Format("Turning on drills mode is stuck for bundle %v. OperationAge: %v, Threshold: %v",
                bundleName,
                operationAge,
                input.Config->NodeAssignmentTimeout),
        });
    }

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    if (!bundleInfo->MuteTabletCellsCheck || !bundleInfo->MuteTabletCellSnapshotsCheck) {
        mutations->ChangedMuteTabletCellsCheck[bundleName] = mutations->WrapMutation(true);
        mutations->ChangedMuteTabletCellSnapshotsCheck[bundleName] = mutations->WrapMutation(true);

        YT_LOG_DEBUG("Disabling tablet cell checks for bundle (BundleName: %v)",
            bundleName);

        return;
    }

    auto drillsNodeTagFilter = GetDrillsNodeTagFilter(bundleInfo, bundleName);
    if (bundleInfo->NodeTagFilter != drillsNodeTagFilter) {
        YT_LOG_DEBUG("Setting drills node tag filter for bundle (BundleName: %v, NodeTagFilter: %v)",
            bundleName,
            drillsNodeTagFilter);

        mutations->ChangedNodeTagFilters[bundleName] = mutations->WrapMutation(drillsNodeTagFilter);
        return;
    }

    YT_LOG_DEBUG("Finished turning on drills mode (BundleName: %v)",
        bundleName);

    drillsMode->TurningOn.Reset();
}

void ProcessDisableDrillsMode(const std::string& bundleName, const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    auto& bundleState = GetOrCrash(mutations->ChangedStates, bundleName);
    const auto& drillsMode = bundleState->DrillsMode;

    if (!drillsMode->TurningOff) {
        return;
    }

    YT_LOG_DEBUG("Processing turning off drills mode (BundleName: %v)",
        bundleName);

    auto operationAge = TInstant::Now() - drillsMode->TurningOff->CreationTime;
    auto disableTimeout = input.Config->NodeAssignmentTimeout + input.Config->MuteTabletCellsCheckGracePeriod;

    if (operationAge > disableTimeout) {
        YT_LOG_WARNING("Turning off drills mode is stuck (BundleName: %v, OperationAge: %v, Threshold: %v)",
            bundleName,
            operationAge,
            disableTimeout);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "stuck_disable_drills_mode",
            .BundleName = bundleName,
            .Description = Format("Turning off drills mode is stuck for bundle %v. OperationAge: %v, Threshold: %v",
                bundleName,
                operationAge,
                disableTimeout),
        });
    }

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    auto nodeTagFilter = GetNodeTagFilter(bundleInfo, bundleName);
    if (bundleInfo->NodeTagFilter != nodeTagFilter) {
        mutations->ChangedNodeTagFilters[bundleName] = mutations->WrapMutation(nodeTagFilter);

        YT_LOG_DEBUG("Setting node tag filter for bundle (BundleName: %v, NodeTagFilter: %v)",
            bundleName,
            nodeTagFilter);

        return;
    }

    if (operationAge < input.Config->MuteTabletCellsCheckGracePeriod) {
        YT_LOG_DEBUG("Waiting grace period before enabling tablet cell checks (BundleName: %v, OperationAge: %v, Threshold: %v)",
            bundleName,
            operationAge,
            input.Config->MuteTabletCellsCheckGracePeriod);

        return;
    }

    if (bundleInfo->MuteTabletCellsCheck || bundleInfo->MuteTabletCellSnapshotsCheck) {
        YT_LOG_DEBUG("Enabling tablet cell checks for bundle (BundleName: %v)",
            bundleName);

        mutations->ChangedMuteTabletCellsCheck[bundleName] = mutations->WrapMutation(false);
        mutations->ChangedMuteTabletCellSnapshotsCheck[bundleName] = mutations->WrapMutation(false);
        return;
    }

    YT_LOG_DEBUG("Finished turning off drills mode (BundleName: %v)",
        bundleName);

    drillsMode->TurningOff.Reset();
}

void ToggleDrillsMode(const std::string& bundleName, const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    auto& bundleState = GetOrCrash(mutations->ChangedStates, bundleName);
    const auto& drillsMode = bundleState->DrillsMode;

    YT_VERIFY(!drillsMode->TurningOn || !drillsMode->TurningOff);

    auto newState = New<TDrillsModeOperationState>();
    newState->CreationTime = TInstant::Now();

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    auto enableDrillsMode = bundleInfo->TargetConfig->EnableDrillsMode;
    auto drillsNodeTagFilter = GetDrillsNodeTagFilter(bundleInfo, bundleName);

    // Transition from TurningOffDrillsMode --> TurningOnDrillsMode.
    if (enableDrillsMode && drillsMode->TurningOff) {
        drillsMode->TurningOff.Reset();
        drillsMode->TurningOn = newState;
    }

    if (drillsMode->TurningOn || drillsMode->TurningOff) {
        return;
    }

    if (enableDrillsMode && bundleInfo->NodeTagFilter != drillsNodeTagFilter) {
        drillsMode->TurningOn = newState;
    } else if (!enableDrillsMode && bundleInfo->NodeTagFilter == drillsNodeTagFilter) {
        drillsMode->TurningOff = newState;
    }
}

void ManageDrillsMode(const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController || bundleInfo->Zone.empty()) {
            continue;
        }

        if (!bundleInfo->EnableNodeTagFilterManagement) {
            continue;
        }

        ProcessDisableDrillsMode(bundleName, input, mutations);
        ProcessEnableDrillsMode(bundleName, input, mutations);
        ToggleDrillsMode(bundleName, input, mutations);
    }
}

void TrimNetworkInfo(TSchedulerInputState* input)
{
    if (input->Config->EnableNetworkLimits) {
        return;
    }

    // Networking is disabled. We have to trim all networking info.

    for (auto& [_, bundleInfo] : input->Bundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->TargetConfig) {
            continue;
        }

        const auto& targetConfig = bundleInfo->TargetConfig;
        targetConfig->TabletNodeResourceGuarantee->ResetNet();
        targetConfig->RpcProxyResourceGuarantee->ResetNet();
    }

    for (const auto& [_, nodeInfo] : input->TabletNodes) {
        if (nodeInfo->BundleControllerAnnotations && nodeInfo->BundleControllerAnnotations->Resource) {
            nodeInfo->BundleControllerAnnotations->Resource->ResetNet();
        }
    }

    for (const auto& [_, proxyInfo] : input->RpcProxies) {
        if (proxyInfo->BundleControllerAnnotations && proxyInfo->BundleControllerAnnotations->Resource) {
            proxyInfo->BundleControllerAnnotations->Resource->ResetNet();
        }
    }

    for (auto& [_, zoneInfo] : input->Zones) {
        if (!zoneInfo->SpareTargetConfig) {
            continue;
        }

        const auto& spareTargetConfig = zoneInfo->SpareTargetConfig;
        spareTargetConfig->TabletNodeResourceGuarantee->ResetNet();
        spareTargetConfig->RpcProxyResourceGuarantee->ResetNet();
    }
}

void InitializeBundleTargetConfig(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController || bundleInfo->TargetConfig) {
            continue;
        }

        auto targetConfig = New<TBundleConfig>();
        bundleInfo->TargetConfig = targetConfig;
        mutations->InitializedBundleTargetConfig[bundleName] = targetConfig;

        auto zoneIt = input.Zones.find(bundleInfo->Zone);
        if (zoneIt == input.Zones.end()) {
            continue;
        }

        const auto& zoneInfo = zoneIt->second;
        if (!zoneInfo->TabletNodeSizes.empty()) {
            auto& front = *zoneInfo->TabletNodeSizes.begin();
            targetConfig->TabletNodeResourceGuarantee = NYTree::CloneYsonStruct(front.second->ResourceGuarantee);
            targetConfig->TabletNodeResourceGuarantee->Type = front.first;
            targetConfig->CpuLimits = front.second->DefaultConfig->CpuLimits;
            targetConfig->MemoryLimits = front.second->DefaultConfig->MemoryLimits;
        }

        if (!zoneInfo->RpcProxySizes.empty()) {
            auto& front = *zoneInfo->RpcProxySizes.begin();
            targetConfig->RpcProxyResourceGuarantee = NYTree::CloneYsonStruct(front.second->ResourceGuarantee);
            targetConfig->RpcProxyResourceGuarantee->Type = front.first;
        }
    }

    for (const auto& [bundleName, targetConfig] : mutations->InitializedBundleTargetConfig) {
        YT_LOG_INFO("Initializing target config for bundle (Bundle: %v, TargetConfig: %v)",
            bundleName,
            ConvertToYsonString(targetConfig, EYsonFormat::Text));
    }
}

void MiscBundleChecks(const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        if (bundleInfo->BundleHotfix) {
            YT_LOG_WARNING("Hotfix mode is enabled for bundle (BundleName: %v)",
                bundleName);

            mutations->AlertsToFire.push_back({
                .Id = "hotfix_mode_is_enabled",
                .BundleName = bundleName,
                .Description = "Hotfix mode is enabled for the bundle",
            });
        }
    }

    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        for (const auto& [dataCenterName, dataCenter] : zoneInfo->DataCenters) {
            if (dataCenter->Forbidden) {
                YT_LOG_WARNING("Data center is forbidden (Zone: %v, DataCenter: %v)",
                    zoneName,
                    dataCenterName);

                mutations->AlertsToFire.push_back({
                    .Id = "dc_is_forbidden",
                    .DataCenter = dataCenterName,
                    .Description = Format("Data center %Qv is forbidden",
                        dataCenterName),
                });
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ScheduleBundles(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    InitializeRelations(&input, mutations->MakeOnAlertCallback());

    TrimNetworkInfo(&input);
    InitializeNodeTagFilters(input, mutations);
    InitializeBundleTargetConfig(input, mutations);

    CalculateResourceUsage(input);
    input.DatacenterDisrupted = GetDataCenterDisruptedState(input);
    input.BundleToShortName = MapBundlesToShortNames(input);

    InitializeBundleChangedStates(input, mutations);

    InitializeZoneToSpareNodes(input, mutations);
    InitializeZoneToSpareProxies(input, mutations);

    auto spareNodesState = New<TSpareInstanceAllocator<TSpareNodesInfo>>(input.ZoneToSpareNodes);
    auto spareProxiesState = New<TSpareInstanceAllocator<TSpareProxiesInfo>>(input.ZoneToSpareProxies);

    ManageBundlesDynamicConfig(input, mutations);
    ManageInstances(input, spareNodesState, spareProxiesState, mutations);
    ManageCells(input, mutations);
    ManageSystemAccountLimit(input, mutations);
    ManageResourceLimits(input, mutations);

    ManageNodeTagFilters(input, *spareNodesState, mutations);
    ManageRpcProxyRoles(input, *spareProxiesState, mutations);
    ManageBundleShortName(input, mutations);
    ManageDrillsMode(input, mutations);
    MiscBundleChecks(input, mutations);

    mutations->ChangedStates = GetActuallyChangedStates(input, *mutations);

    YT_LOG_DEBUG("Logging scheduled mutations");
    mutations->Log(Logger());
}

////////////////////////////////////////////////////////////////////////////////

TIndexedEntries<TBundleControllerState> MergeBundleStates(
    const TSchedulerInputState& schedulerState,
    const TSchedulerMutations& mutations)
{
    TIndexedEntries<TBundleControllerState> results = schedulerState.BundleStates;

    for (const auto& [bundleName, state] : mutations.ChangedStates) {
        results[bundleName] = NYTree::CloneYsonStruct(state);
    }

    return results;
}

////////////////////////////////////////////////////////////////////////////////

THashSet<std::string> FlattenAliveInstances(const THashMap<std::string, THashSet<std::string>>& instances)
{
    THashSet<std::string> result;

    for (const auto& [_, dataCenterInstances] : instances) {
        for (const auto& instance : dataCenterInstances) {
            result.insert(instance);
        }
    }

    return result;
}

std::vector<std::string> FlattenBundleInstances(const THashMap<std::string, std::vector<std::string>>& instances)
{
    std::vector<std::string> result;

    for (const auto& [_, dataCenterInstances] : instances) {
        for (const auto& instance : dataCenterInstances) {
            result.push_back(instance);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
