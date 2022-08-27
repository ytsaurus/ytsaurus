#include "bundle_scheduler.h"
#include "config.h"

#include <library/cpp/yt/yson_string/public.h>

#include <compare>

namespace NYT::NCellBalancer {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

bool IsAllocationFailed(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "FAILED";
}

bool IsAllocationCompleted(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "COMPLETED";
}

TString GetPodIdForInstance(const TString& name)
{
    // TODO(capone212): Get pod_id from node cypress annotations.

    // For now we get PodId in a bit hacky way:
    // we expect PodId to be prefix of fqdn before the first dot.
    auto endPos = name.find(".");
    YT_VERIFY(endPos != TString::npos);

    auto podId = name.substr(0, endPos);
    YT_VERIFY(!podId.empty());
    return podId;
}

////////////////////////////////////////////////////////////////////////////////

TString GetInstancePodIdTemplate(
    const TString& cluster,
    const TString& bundleName,
    const TString& instanceType,
    int index)
{
    return Format("<short-hostname>-%v-%03x-%v-%v", bundleName, index, instanceType, cluster);
}

std::optional<int> GetIndexFromPodId(
    const TString& podId,
    const TString& cluster,
    const TString& instanceType)
{
    TStringBuf buffer = podId;
    auto suffix = Format("-%v-%v", instanceType, cluster);
    if (!buffer.ChopSuffix(suffix)) {
        return {};
    }

    constexpr char Delimiter = '-';
    auto indexString = buffer.RNextTok(Delimiter);

    int result = 0;
    if (TryIntFromString<16>(indexString, result)) {
        return result;
    }

    return {};
}

int FindNextInstanceId(
    const std::vector<TString>& instanceNames,
    const TString& cluster,
    const TString& instanceType)
{
    std::vector<int> existingIds;
    existingIds.reserve(instanceNames.size());

    for (const auto& instanceName : instanceNames) {
        auto index = GetIndexFromPodId(instanceName, cluster, instanceType);
        if (index && *index > 0) {
            existingIds.push_back(*index);
        }
    }

    // Sort and make unique.
    std::sort(existingIds.begin(), existingIds.end());
    auto last = std::unique(existingIds.begin(), existingIds.end());
    existingIds.resize(std::distance(existingIds.begin(), last));

    if (existingIds.empty()) {
        return 1;
    }

    for (int index = 0; index < std::ssize(existingIds); ++index) {
        if (existingIds[index] != index + 1) {
            return index + 1;
        }
    }

    return existingIds.back() + 1;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TInstanceTypeAdapter>
class TInstanceManager
{
public:
    explicit TInstanceManager(NLogging::TLogger logger)
        : Logger(std::move(logger))
    { }

    void ManageInstancies(
        const TString& bundleName,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        ProcessExistingAllocations(bundleName, adapter, input, mutations);
        InitNewAllocations(bundleName, adapter, input, mutations);
        ProcessExistingDeallocations(bundleName, adapter, input, mutations);
        InitNewDeallocations(bundleName, adapter, input, mutations);
    }

private:
    NLogging::TLogger Logger;

    void InitNewAllocations(
        const TString& bundleName,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        // TODO(capone212): think about allocation/deallocation budget.
        const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
        auto& allocationsState = adapter->AllocationsState();

        YT_VERIFY(bundleInfo->EnableBundleController);

        int aliveInstanceCount = std::ssize(adapter->GetAliveInstancies());
        int instanceCountToAllocate = adapter->GetTargetInstanceCount(bundleInfo) -
            aliveInstanceCount -
            std::ssize(allocationsState);

        YT_LOG_DEBUG("Scheduling allocations (BundleName: %v, InstanceType: %v, InstanceCount: %v, "
            "AliveInstanceCount: %v, RequestCount: %v, ExistingAllocations: %v)",
            bundleName,
            adapter->GetInstanceType(),
            adapter->GetTargetInstanceCount(bundleInfo),
            aliveInstanceCount,
            instanceCountToAllocate,
            std::ssize(allocationsState));

        auto zoneIt = input.Zones.find(bundleInfo->Zone);

        if (zoneIt == input.Zones.end()) {
            YT_LOG_WARNING("Cannot locate zone for bundle (Bundle: %v, Zone: %v)",
                bundleName,
                bundleInfo->Zone);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "can_not_find_zone_for_bundle",
                .Description = Format("Cannot locate zone %v for bundle %v.", bundleInfo->Zone, bundleName)
            });
            return;
        }

        const auto& zoneInfo = zoneIt->second;
        if (instanceCountToAllocate > 0 && adapter->IsInstanceCountLimitReached(bundleInfo->Zone, zoneInfo, input)) {
            mutations->AlertsToFire.push_back(TAlert{
                .Id = "zone_instance_limit_reached",
                .Description = Format("Cannot allocate new %v at zone %v for bundle %v.",
                    adapter->GetInstanceType(), bundleInfo->Zone, bundleName)
            });
            return;
        }

        for (int i = 0; i < instanceCountToAllocate; ++i) {
            TString allocationId = ToString(TGuid::Create());

            YT_LOG_INFO("Init allocation for bundle (BundleName: %v, InstanceType %v, AllocationId: %v)",
                bundleName,
                adapter->GetInstanceType(),
                allocationId);

            auto spec = New<TAllocationRequestSpec>();
            spec->YPCluster = zoneInfo->YPCluster;
            spec->NannyService = adapter->GetNannyService(zoneInfo);
            *spec->ResourceRequest = *adapter->GetResourceGuarantee(bundleInfo);
            spec->PodIdTemplate = GetPodIdTemplate(bundleName, adapter, input, mutations);
            spec->InstanceRole = adapter->GetInstanceRole();

            auto request = New<TAllocationRequest>();
            request->Spec = spec;
            mutations->NewAllocations[allocationId] = request;
            auto allocationState = New<TAllocationRequestState>();
            allocationState->CreationTime = TInstant::Now();
            allocationState->PodIdTemplate = spec->PodIdTemplate;
            allocationsState[allocationId] = allocationState;
        }
    }

    TString GetPodIdTemplate(
        const TString& bundleName,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        std::vector<TString> knownPodIds;
        for (const auto& instanceName : adapter->GetInstancies()) {
            knownPodIds.push_back(GetPodIdForInstance(instanceName));
        }

        for (const auto& [allocationId, state] : adapter->AllocationsState()) {
            if (state->PodIdTemplate.Empty()) {
                YT_LOG_WARNING("Empty PodIdTemplate found in allocation state "
                    "(AllocationId: %v, InstanceType: %v, BundleName: %v)",
                    allocationId,
                    adapter->GetInstanceType(),
                    bundleName);

                mutations->AlertsToFire.push_back(TAlert{
                        .Id = "unexpected_pod_id",
                        .Description = Format("Allocation request %v for bundle %v has empty pod_id",
                            allocationId, bundleName),
                    });
            }

            knownPodIds.push_back(state->PodIdTemplate);
        }

        for (const auto& [_, state] : adapter->DeallocationsState()) {
            knownPodIds.push_back(GetPodIdForInstance(state->InstanceName));
        }

        auto instanceIndex = FindNextInstanceId(
            knownPodIds,
            input.Config->Cluster,
            adapter->GetInstanceType());

        return GetInstancePodIdTemplate(
            input.Config->Cluster,
            bundleName,
            adapter->GetInstanceType(),
            instanceIndex);
    }

    void ProcessExistingAllocations(
        const TString& bundleName,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        auto& allocationsState = adapter->AllocationsState();

        TIndexedEntries<TAllocationRequestState> aliveAllocations;
        for (const auto& [allocationId, allocationState] :allocationsState) {
            auto it = input.AllocationRequests.find(allocationId);
            if (it == input.AllocationRequests.end()) {
                YT_LOG_WARNING("Cannot find allocation (AllocationId: %v, InstanceType: %v, BundleName: %v)",
                    allocationId,
                    adapter->GetInstanceType(),
                    bundleName);

                mutations->AlertsToFire.push_back(TAlert{
                    .Id = "can_not_find_allocation_request",
                    .Description = Format("Allocation request %v "
                        "found in bundle state, but is absent in hulk allocations.",
                        allocationId),
                });
                // It is better to keep this allocation, otherwise there is a chance to
                // create create unbounded amount of new instancies.
                aliveAllocations[allocationId] = allocationState;
                continue;
            }

            const auto& allocationInfo = it->second;

            if (IsAllocationFailed(allocationInfo)) {
                YT_LOG_WARNING("Allocation Failed (AllocationId: %v, InstanceType: %v, BundleName: %v)",
                    allocationId,
                    adapter->GetInstanceType(),
                    bundleName);

                mutations->AlertsToFire.push_back(TAlert{
                    .Id = "instance_allocation_failed",
                    .Description = Format("Allocation request %v has failed.",
                        allocationId),
                });
                continue;
            }

            auto instanceName = LocateAllocatedInstance(allocationInfo, input);
            if (!instanceName.empty() && adapter->EnsureAllocatedInstanceTagsSet(instanceName, bundleName, allocationInfo, input, mutations)) {
                YT_LOG_INFO("Instance allocation completed (Name: %v, AllocationId: %v, BundleName: %v)",
                    instanceName,
                    allocationId,
                    bundleName);
                continue;
            }

            auto allocationAge = TInstant::Now() - allocationState->CreationTime;
            if (allocationAge > input.Config->HulkRequestTimeout) {
                YT_LOG_WARNING("Allocation Request is stuck (AllocationId: %v, AllocationAge: %v, Threshold: %v)",
                    allocationId,
                    allocationAge,
                    input.Config->HulkRequestTimeout);

                mutations->AlertsToFire.push_back(TAlert{
                    .Id = "stuck_instance_allocation",
                    .Description = Format("Found stuck allocation %v and age %v which is more than threshold %v.",
                        allocationId,
                        allocationAge,
                        input.Config->HulkRequestTimeout),
                });
            }

            YT_LOG_DEBUG("Tracking existing allocation (AllocationId: %v, Bundle: %v,  InstanceName: %v)",
                allocationId,
                bundleName,
                instanceName);

            aliveAllocations[allocationId] = allocationState;
        }

        allocationsState.swap(aliveAllocations);
    }

    // Returns false if current deallocation should not be tracked any more.
    bool ProcessDeallocation(
        TInstanceTypeAdapter* adapter,
        const TString& deallocationId,
        const TDeallocationRequestStatePtr& deallocationState,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        auto deallocationAge = TInstant::Now() - deallocationState->CreationTime;
        if (deallocationAge > input.Config->HulkRequestTimeout) {
            YT_LOG_WARNING("Deallocation Request is stuck (DeallocationId: %v, DeallocationAge: %v, Threshold: %v)",
                deallocationId,
                deallocationAge,
                input.Config->HulkRequestTimeout);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "stuck_instance_deallocation",
                .Description = Format("Found stuck deallocation %v and age %v which is more than threshold %v.",
                    deallocationId,
                    deallocationAge,
                    input.Config->HulkRequestTimeout),
            });
        }

        const auto& instanceName = deallocationState->InstanceName;
        if (!adapter->IsInstanceReadyToBeDeallocated(instanceName, deallocationId, input, mutations)) {
            return true;
        }

        if (!deallocationState->HulkRequestCreated) {
            CreateHulkDeallocationRequest(deallocationId, instanceName, adapter, input, mutations);
            deallocationState->HulkRequestCreated = true;
            return true;
        }

        auto it = input.DeallocationRequests.find(deallocationId);
        if (it == input.DeallocationRequests.end()) {
            YT_LOG_WARNING("Cannot find deallocation (DeallocationId: %v)",
                deallocationId);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "can_not_find_deallocation_request",
                .Description = Format("Deallocation request %v "
                    "found in bundle state, but is absent in hulk deallocations.",
                    deallocationId),
            });
            // Keep deallocation request and wait for an in duty person.
            return true;
        }

        if (IsAllocationFailed(it->second)) {
            YT_LOG_WARNING("Deallocation Failed (AllocationId: %v)",
                deallocationId);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "instance_deallocation_failed",
                .Description = Format("Deallocation request %v has failed.",
                    deallocationId),
            });
            return false;
        }

        if (IsAllocationCompleted(it->second) && adapter->EnsureDeallocatedInstanceTagsSet(instanceName, input, mutations)) {
            YT_LOG_INFO("Instance deallocation completed (InstanceName: %v, DeallocationId: %v)",
                instanceName,
                deallocationId);
            return false;
        }

        YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, InstanceName: %v)",
            deallocationId,
            instanceName);
        return true;
    }

    void ProcessExistingDeallocations(
        const TString& /*bundleName*/,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        auto& deallocations = adapter->DeallocationsState();
        TIndexedEntries<TDeallocationRequestState> aliveDeallocations;

        for (const auto& [deallocationId, deallocationState] : deallocations) {
            if (ProcessDeallocation(adapter, deallocationId, deallocationState, input, mutations)) {
                aliveDeallocations[deallocationId] = deallocationState;
            }
        }

        deallocations.swap(aliveDeallocations);
    }

    void CreateHulkDeallocationRequest(
        const TString& deallocationId,
        const TString& instanceName,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
        const auto& instanceAnnotations = instanceInfo->Annotations;
        auto request = New<TDeallocationRequest>();
        auto& spec = request->Spec;
        spec->YPCluster = instanceAnnotations->YPCluster;
        spec->PodId = GetPodIdForInstance(instanceName);
        spec->InstanceRole = adapter->GetInstanceRole();
        mutations->NewDeallocations[deallocationId] = request;
    }

    void InitNewDeallocations(
        const TString& bundleName,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* /*mutations*/)
    {
        // TODO(capone212): think about allocation deallocation budget.
        const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
        YT_VERIFY(bundleInfo->EnableBundleController);

        if (!adapter->IsNewDeallocationAllowed(bundleInfo)) {
            return;
        }

        auto aliveInstancies = adapter->GetAliveInstancies();
        int instanceCountToDeallocate = std::ssize(aliveInstancies) - adapter->GetTargetInstanceCount(bundleInfo);
        auto& deallocationsState = adapter->DeallocationsState();

        YT_LOG_DEBUG("Scheduling deallocations (BundleName: %v, InstanceCount: %v, AliveInstances: %v, "
            "RequestCount: %v, ExistingDeallocations: %v)",
            bundleName,
            adapter->GetTargetInstanceCount(bundleInfo),
            std::ssize(aliveInstancies),
            instanceCountToDeallocate,
            std::ssize(deallocationsState));

        if (instanceCountToDeallocate <= 0) {
            return;
        }

        const auto instanciesToRemove = adapter->PeekInstanciesToDeallocate(instanceCountToDeallocate, input);

        for (const auto& instanceName : instanciesToRemove) {
            TString deallocationId = ToString(TGuid::Create());
            auto deallocationState = New<TDeallocationRequestState>();
            deallocationState->CreationTime = TInstant::Now();
            deallocationState->InstanceName = instanceName;
            deallocationsState[deallocationId] = deallocationState;

            YT_LOG_INFO("Init instance deallocation (BundleName: %v, InstanceName: %v, DeallocationId: %v)",
                bundleName,
                instanceName,
                deallocationId);
        }
    }

    TString LocateAllocatedInstance(
        const TAllocationRequestPtr& requestInfo,
        const TSchedulerInputState& input) const
    {
        if (!IsAllocationCompleted(requestInfo)) {
            return {};
        }

        const auto& podId = requestInfo->Status->PodId;
        auto it = input.PodIdToInstanceName.find(podId);
        if (it != input.PodIdToInstanceName.end()) {
            return it->second;
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TInstanceInfoPtr>
TString GetBundleNameFor(const TString& /*name*/, const TInstanceInfoPtr& info)
{
    return info->Annotations->AllocatedForBundle;
}

template <typename TCollection>
TSchedulerInputState::TBundleToInstanceMapping MapBundlesToInstancies(const TCollection& collection)
{
    TSchedulerInputState::TBundleToInstanceMapping result;

    for (const auto& [instanceName, instanceInfo] : collection) {
        auto bundleName = GetBundleNameFor(instanceName, instanceInfo);
        if (!bundleName.empty()) {
            result[bundleName].push_back(instanceName);
        }
    }

    return result;
}

template <typename TCollection>
TSchedulerInputState::TZoneToInstanceMap MapZonesToInstancies(
    const TSchedulerInputState& input,
    const TCollection& collection)
{
    THashMap<TString, TString> nannyServiceToZone;
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        if (!zoneInfo->TabletNodeNannyService.empty()) {
            nannyServiceToZone[zoneInfo->TabletNodeNannyService] = zoneName;
        }

        if (!zoneInfo->RpcProxyNannyService.empty()) {
            nannyServiceToZone[zoneInfo->RpcProxyNannyService] = zoneName;
        }
    }

    TSchedulerInputState::TZoneToInstanceMap result;
    for (const auto& [instanceName, instanceInfo] : collection) {
        if (!instanceInfo->Annotations->Allocated) {
            continue;
        }
        auto it = nannyServiceToZone.find(instanceInfo->Annotations->NannyService);
        if (it == nannyServiceToZone.end()) {
            continue;
        }
        const auto& zoneName = it->second;
        result[zoneName].push_back(instanceName);
    }

    return result;
}

THashMap<TString, TString> MapPodIdToInstanceName(const TSchedulerInputState& input)
{
    THashMap<TString, TString> result;

    for (const auto& [nodeName, _] : input.TabletNodes) {
        auto podId = GetPodIdForInstance(nodeName);
        result[podId] = nodeName;
    }

    for (const auto& [proxyName, _] : input.RpcProxies) {
        auto podId = GetPodIdForInstance(proxyName);
        result[podId] = proxyName;
    }

    return result;
}

THashSet<TString> GetAliveNodes(
    const TString& bundleName,
    const std::vector<TString>& bundleNodes,
    const TSchedulerInputState& input)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    THashSet<TString> aliveNodes;

    for (const auto& nodeName : bundleNodes) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        if (!nodeInfo->Annotations->Allocated || nodeInfo->State != InstanceStateOnline) {
            continue;
        }

        if (!bundleInfo->NodeTagFilter.empty() && nodeInfo->Decommissioned) {
            continue;
        }

        aliveNodes.insert(nodeName);
    }

    return aliveNodes;
}

THashSet<TString> GetAliveProxies(
    const std::vector<TString>& bundleProxies,
    const TSchedulerInputState& input)
{
    THashSet<TString> aliveProxies;

    for (const auto& proxyName : bundleProxies) {
        const auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        if (!proxyInfo->Annotations->Allocated || proxyInfo->Banned || !proxyInfo->Alive) {
            continue;
        }

        aliveProxies.insert(proxyName);
    }

    return aliveProxies;
}

int GetUsedSlotCount(const TTabletNodeInfoPtr& nodeInfo)
{
    int usedSlotCount = 0;

    for (const auto& slotInfo : nodeInfo->TabletSlots) {
        if (slotInfo->State != TabletSlotStateEmpty) {
            ++usedSlotCount;
        }
    }

    return usedSlotCount;
}

struct TNodeRemoveOrder
{
    int UsedSlotCount = 0;
    TString NodeName;

    bool operator<(const TNodeRemoveOrder& other) const
    {
        return std::tie(UsedSlotCount, NodeName)
            < std::tie(other.UsedSlotCount, other.NodeName);
    }
};

int GetTargetCellCount(const TBundleInfoPtr& bundleInfo)
{
    const auto& targetConfig = bundleInfo->TargetConfig;
    return targetConfig->TabletNodeCount * targetConfig->CpuLimits->WriteThreadPoolSize;
}

bool EnsureNodeDecommissioned(
    const TString& nodeName,
    const TTabletNodeInfoPtr& nodeInfo,
    TSchedulerMutations* mutations)
{
    if (!nodeInfo->Decommissioned) {
        mutations->ChangedDecommissionedFlag[nodeName] = true;
        return false;
    }
    // Wait tablet cells to migrate.
    return GetUsedSlotCount(nodeInfo) == 0;
}

struct TTabletCellRemoveOrder
{
    TString Id;
    TString HostNode;
    int HostNodeTabletCount = 0;
    int TabletCount = 0;
    bool Disrupted = false;
    // No tablet host and non zero tablet nodes

    auto AsTuple() const
    {
        return std::tie(Disrupted, TabletCount, HostNodeTabletCount, HostNode, Id);
    }

    bool operator<(const TTabletCellRemoveOrder& other) const
    {
        return AsTuple() < other.AsTuple();
    }
};

TString GetHostNodeForCell(const TTabletCellInfoPtr& cellInfo, const THashSet<TString>& bundleNodes)
{
    TString nodeName;

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

std::vector<TString> PeekTabletCellsToRemove(
    const TString& bundleName,
    int cellCountToRemove,
    const std::vector<TString>& bundleCellIds,
    const THashSet<TString>& bundleNodes,
    const TSchedulerInputState& input)
{
    YT_VERIFY(std::ssize(bundleCellIds) >= cellCountToRemove);

    // Using the following heuristics:
    // - peek cells that are not running or running at foreign nodes.
    // - In order to deallocate tablet nodes after instance count shrinkage
    //      -- try to peek colocated tablet cells on single host node
    //      -- prefer nodes with less tablets to make less disruption

    THashMap<TString, int> NodeToTabletCount;

    std::vector<TTabletCellRemoveOrder> cellsOrder;
    cellsOrder.reserve(bundleCellIds.size());

    for(const auto& cellId : bundleCellIds) {
        auto it = input.TabletCells.find(cellId);
        if (it == input.TabletCells.end()) {
            YT_LOG_WARNING("Cannot locate cell info (BundleName: %v, TableCellId: %v)",
                    bundleName,
                    cellId);
            continue;
        }
        const auto& cellInfo = it->second;
        auto nodeName = GetHostNodeForCell(cellInfo, bundleNodes);

        cellsOrder.push_back(TTabletCellRemoveOrder{
            .Id = cellId,
            .HostNode = nodeName,
            .TabletCount = cellInfo->TabletCount,
            .Disrupted = nodeName.empty() && cellInfo->TabletCount != 0,
        });

        if (!nodeName.empty()) {
            NodeToTabletCount[nodeName] += cellInfo->TabletCount;
        }
    }

    for (auto& cell : cellsOrder) {
        auto it = NodeToTabletCount.find(cell.HostNode);
        if (it != NodeToTabletCount.end()) {
            cell.HostNodeTabletCount = it->second;
        }
    }

    auto endIt = cellsOrder.end();
    if (std::ssize(cellsOrder) > cellCountToRemove) {
        endIt = cellsOrder.begin() + cellCountToRemove;
        std::nth_element(cellsOrder.begin(), endIt, cellsOrder.end());
    }

    std::vector<TString> result;
    result.reserve(std::distance(cellsOrder.begin(), endIt));
    for (auto it = cellsOrder.begin(); it != endIt; ++it) {
        result.push_back(it->Id);
    }

    return result;
}

void ProcessRemovingCells(
    const TString& bundleName,
    const std::vector<TString>& /*bundleNodes*/,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto& state = mutations->ChangedStates[bundleName];
    std::vector<TString> removeCompleted;

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
            YT_LOG_WARNING("Tablet cell removal is stuck (TabletCellId: %v, RemovingTime: %v, Threshold: %v)",
                cellId,
                removingTime,
                input.Config->CellRemovalTimeout);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "stuck_tablet_cell_removal",
                .Description = Format("Found stuck tablet cell %v removal "
                    " with removing time %v, which is more than threshold %v.",
                    cellId,
                    removingTime,
                    input.Config->CellRemovalTimeout),
            });
        }

        YT_LOG_DEBUG("Tablet cell removal in progress"
            " (BundleName: %v, TabletCellId: %v, LifeStage: %v, Decommissioned: %v)",
            bundleName,
            cellId,
            it->second->TabletCellLifeStage,
            it->second->Status->Decommissioned);
    }

    for (const auto& cellId : removeCompleted) {
        state->RemovingCells.erase(cellId);
    }
}

void CreateRemoveTabletCells(
    const TString& bundleName,
    const std::vector<TString>& bundleNodes,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& bundleState = mutations->ChangedStates[bundleName];
    auto aliveNodes = GetAliveNodes(bundleName, bundleNodes, input);

    if (!bundleInfo->EnableTabletCellManagement) {
        return;
    }

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

    int targetCellCount = GetTargetCellCount(bundleInfo);
    int cellCountDiff = targetCellCount - std::ssize(bundleInfo->TabletCellIds);

    YT_LOG_DEBUG("Managing tablet cells (BundleName: %v, TargetCellCount: %v, ExistingCount: %v)",
        bundleName,
        targetCellCount,
        std::ssize(bundleInfo->TabletCellIds));

    if (cellCountDiff < 0) {
        mutations->CellsToRemove = PeekTabletCellsToRemove(bundleName,
            std::abs(cellCountDiff),
            bundleInfo->TabletCellIds,
            aliveNodes,
            input);
        
        YT_LOG_INFO("Removing tablet cells (BundleName: %v, CellIds: %v)",
            bundleName,
            mutations->CellsToRemove);

        for (auto& cellId : mutations->CellsToRemove) {
            auto removingCellInfo = New<TRemovingTabletCellInfo>();
            removingCellInfo->RemovedTime = TInstant::Now();
            bundleState->RemovingCells[cellId] = removingCellInfo;
        }
    } else if (cellCountDiff > 0) {
        YT_LOG_INFO("Creating tablet cells (BundleName: %v, CellCount: %v)",
            bundleName,
            cellCountDiff);

        mutations->CellsToCreate[bundleName] = cellCountDiff;
    }
}

struct TQuotaDiff
{
    i64 ChunkCount = 0;
    THashMap<TString, i64> DiskSpacePerMedium;
    i64 NodeCount = 0;

    bool Empty() const
    {
        return ChunkCount == 0 &&
            NodeCount == 0 &&
            std::all_of(DiskSpacePerMedium.begin(), DiskSpacePerMedium.end(), [] (const auto& pair) {
                return pair.second == 0;
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

using TQuotaChanges = THashMap<TString, TQuotaDiff>;

void AddQuotaChanges(
    const TString& bundleName,
    const TBundleSystemOptionsPtr& bundleOptions,
    const TSchedulerInputState& input,
    int cellCount,
    TQuotaChanges& changes)
{
    if (bundleOptions->SnapshotAccount != bundleOptions->ChangelogAccount) {
        YT_LOG_DEBUG("Skip adjusting quota for bundle with different "
            "snapshot and changelog accounts (BundleName: %v, SnapshotAccount: %v, ChangelogAccount: %v)",
            bundleName,
            bundleOptions->SnapshotAccount,
            bundleOptions->ChangelogAccount);
        return;
    }

    const auto accountName = bundleOptions->SnapshotAccount;
    auto accountIt = input.SystemAccounts.find(accountName);
    if (accountIt == input.SystemAccounts.end()) {
        YT_LOG_DEBUG("Skip adjusting quota for bundle with custom account"
            " (BundleName: %v, SnapshotAccount: %v, ChangelogAccount: %v)",
            bundleName,
            bundleOptions->SnapshotAccount,
            bundleOptions->ChangelogAccount);
        return;
    }

    const auto& currentLimit = accountIt->second->ResourceLimits;
    const auto& config = input.Config;

    cellCount = std::max(cellCount, 1);
    auto multiplier = config->QuotaMultiplier * cellCount;

    TQuotaDiff quotaDiff;

    quotaDiff.ChunkCount = std::max<i64>(config->ChunkCountPerCell * multiplier, config->MinChunkCount) - currentLimit->ChunkCount;
    quotaDiff.NodeCount = std::max<i64>(config->NodeCountPerCell * multiplier, config->MinNodeCount) - currentLimit->NodeCount;

    auto getSpace = [&] (const TString& medium) -> i64 {
        auto it = currentLimit->DiskSpacePerMedium.find(medium);
        if (it == currentLimit->DiskSpacePerMedium.end()) {
            return 0;
        }
        return it->second;
    };

    i64 snapshotSpace = config->SnapshotDiskSpacePerCell * multiplier;
    i64 changelogSpace = config->JournalDiskSpacePerCell * multiplier;

    if (bundleOptions->ChangelogPrimaryMedium == bundleOptions->SnapshotPrimaryMedium) {
        quotaDiff.DiskSpacePerMedium[bundleOptions->ChangelogPrimaryMedium] = 
            snapshotSpace + changelogSpace - getSpace(bundleOptions->ChangelogPrimaryMedium);
    } else {
        quotaDiff.DiskSpacePerMedium[bundleOptions->ChangelogPrimaryMedium] = 
            changelogSpace - getSpace(bundleOptions->ChangelogPrimaryMedium);
        quotaDiff.DiskSpacePerMedium[bundleOptions->SnapshotPrimaryMedium] = 
            snapshotSpace - getSpace(bundleOptions->SnapshotPrimaryMedium);
    }

    if (!quotaDiff.Empty()) {
        changes[accountName] = quotaDiff;
    }
}

void ApplyQuotaChange(const TQuotaDiff& change, const TAccountResourcesPtr& limits)
{
    limits->ChunkCount += change.ChunkCount;
    limits->NodeCount += change.NodeCount;

    for (const auto& [medium, spaceDiff] : change.DiskSpacePerMedium) {
        limits->DiskSpacePerMedium[medium] += spaceDiff;
    }
}

void ManageSystemAccountLimit(const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    TQuotaChanges quotaChanges;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController ||
            !bundleInfo->EnableTabletCellManagement ||
            !bundleInfo->EnableSystemAccountManagement)
        {
            continue;
        }

        int cellCount = std::max<int>(GetTargetCellCount(bundleInfo), std::ssize(bundleInfo->TabletCellIds));
        AddQuotaChanges(bundleName, bundleInfo->Options, input, cellCount, quotaChanges);
    }

    if (quotaChanges.empty()) {
        return;
    }

    auto rootQuota = CloneYsonSerializable(input.RootSystemAccount->ResourceLimits);

    for (const auto& [accountName, quotaChange] : quotaChanges) {
        const auto& accountInfo = GetOrCrash(input.SystemAccounts, accountName);
        auto newQuota = CloneYsonSerializable(accountInfo->ResourceLimits);
        ApplyQuotaChange(quotaChange, newQuota);
        ApplyQuotaChange(quotaChange, rootQuota);

        mutations->ChangedSystemAccountLimit[accountName] = newQuota;

        YT_LOG_INFO("Adjusting system account resource limits (AccountName: %v, NewResourceLimit: %Qv, OldResourceLimit: %Qv)",
            accountName,
            ConvertToYsonString(newQuota, EYsonFormat::Text),
            ConvertToYsonString(accountInfo->ResourceLimits, EYsonFormat::Text));
    }

    mutations->ChangedRootSystemAccountLimit = rootQuota;
    YT_LOG_INFO("Adjusting root system account resource limits(NewResourceLimit: %Qv, OldResourceLimit: %Qv)",
        ConvertToYsonString(rootQuota, EYsonFormat::Text),
        ConvertToYsonString(input.RootSystemAccount->ResourceLimits, EYsonFormat::Text));
}

////////////////////////////////////////////////////////////////////////////////

TString GetSpareBundleName(const TString& /*zoneName*/)
{
    // TODO(capone212): consider adding zone name.
    return "spare";
}

struct TZoneDisruptedState
{
    bool NodesDisrupted = false;
    bool ProxiesDisrupted = false;
};

THashMap<TString, TZoneDisruptedState> GetZoneDisruptedState(TSchedulerInputState& input)
{
    THashMap<TString, int> zoneOfflineNodeCount;
    for (const auto& [zoneName, zoneNodes] : input.ZoneNodes) {
        for (const auto& nodeName : zoneNodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            if (nodeInfo->State == InstanceStateOnline) {
                continue;
            }

            YT_LOG_DEBUG("Node is offline (NodeName: %v, NannyService: %v)",
                nodeName,
                nodeInfo->Annotations->NannyService);

            ++zoneOfflineNodeCount[zoneName];
        }
    }

    THashMap<TString, int> zoneOfflineProxyCount;
    for (const auto& [zoneName, zoneProxies] : input.ZoneProxies) {
        for (const auto& proxyName : zoneProxies) {
            const auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
            if (proxyInfo->Alive) {
                continue;
            }

            YT_LOG_DEBUG("Proxy is offline (ProxyName: %v, NannyService: %v)",
                proxyName,
                proxyInfo->Annotations->NannyService);

            ++zoneOfflineProxyCount[zoneName];
        }
    }

    THashMap<TString, TZoneDisruptedState> result;
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        int disruptedThreshold = zoneInfo->SpareTargetConfig->TabletNodeCount * zoneInfo->DisruptedThresholdFactor;
        bool nodesDisrupted = disruptedThreshold > 0 && zoneOfflineNodeCount[zoneName] > disruptedThreshold;
        result[zoneName].NodesDisrupted = nodesDisrupted;

        YT_LOG_WARNING_IF(nodesDisrupted, "Zone is in disrupted state"
            " (ZoneName: %v, NannyService: %v, DisruptedThreshold: %v, OfflineNodeCount: %v)",
            zoneName,
            zoneInfo->TabletNodeNannyService,
            disruptedThreshold,
            zoneOfflineNodeCount[zoneName]);

        disruptedThreshold = zoneInfo->SpareTargetConfig->RpcProxyCount * zoneInfo->DisruptedThresholdFactor;
        bool proxiesDisrupted = disruptedThreshold > 0 && zoneOfflineProxyCount[zoneName] > disruptedThreshold;
        result[zoneName].ProxiesDisrupted = proxiesDisrupted;

        YT_LOG_WARNING_IF(proxiesDisrupted, "Zone is in disrupted state"
            " (ZoneName: %v, NannyService: %v, DisruptedThreshold: %v, OfflineProxyCount: %v)",
            zoneName,
            zoneInfo->RpcProxyNannyService,
            disruptedThreshold,
            zoneOfflineProxyCount[zoneName]);
    }

    return result;
}

TInstanceAnnotationsPtr GetInstanceAnnotationsToSet(
    const TString& bundleName,
    const TAllocationRequestPtr& allocationInfo,
    const TInstanceAnnotationsPtr& annotations)
{
    if (!annotations->AllocatedForBundle.empty() && annotations->Allocated) {
        return {};
    }
    auto result = NYTree::CloneYsonSerializable(annotations);
    result->YPCluster = allocationInfo->Spec->YPCluster;
    result->NannyService = allocationInfo->Spec->NannyService;
    result->AllocatedForBundle = bundleName;
    result->Allocated = true;
    *result->Resource = *allocationInfo->Spec->ResourceRequest;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeAllocatorAdapter
{
public:
    TTabletNodeAllocatorAdapter(
        TBundleControllerStatePtr state,
        const std::vector<TString>& bundleNodes,
        const THashSet<TString>& aliveBundleNodes)
        : State_(std::move(state))
        , BundleNodes_(bundleNodes)
        , AliveBundleNodes_(aliveBundleNodes)
    { }

    bool IsNewDeallocationAllowed(const TBundleInfoPtr& bundleInfo)
    {
        if (!State_->NodeAllocations.empty() ||
            !State_->NodeDeallocations.empty() ||
            !State_->RemovingCells.empty())
        {
            // It is better not to mix allocation and deallocation requests.
            return false;
        }

        if (bundleInfo->EnableTabletCellManagement) {
            if (GetTargetCellCount(bundleInfo) != std::ssize(bundleInfo->TabletCellIds)) {
                // Wait for tablet cell management to complete.
                return false;
            }
        }

        return true;
    }

    bool IsInstanceCountLimitReached(
        const TString& zoneName,
        const TZoneInfoPtr& zoneInfo,
        const TSchedulerInputState& input) const
    {
        auto it = input.ZoneNodes.find(zoneName);
        if (it == input.ZoneNodes.end()) {
            // No allocated tablet nodes for this zone
            return false;
        }

        int currentZoneNodeCount = std::ssize(it->second);
        if (currentZoneNodeCount >= zoneInfo->MaxTabletNodeCount) {
            YT_LOG_WARNING("Max nodes count limit reached"
                " (Zone: %v, CurrentZoneNodeCount: %v, MaxTabletNodeCount: %v)",
                zoneName,
                currentZoneNodeCount,
                zoneInfo->MaxTabletNodeCount);
            return true;
        }
        return false;
    }

    int GetTargetInstanceCount(const TBundleInfoPtr& bundleInfo) const
    {
        return bundleInfo->TargetConfig->TabletNodeCount;
    }

    int GetInstanceRole() const
    {
        return YTRoleTypeTabNode;
    }

    const TInstanceResourcesPtr& GetResourceGuarantee(const TBundleInfoPtr& bundleInfo) const
    {
        return bundleInfo->TargetConfig->TabletNodeResourceGuarantee;
    }

    const TString& GetInstanceType()
    {
        static const TString TabletNode = "tab";
        return TabletNode;
    }

    TIndexedEntries<TAllocationRequestState>& AllocationsState() const
    {
        return State_->NodeAllocations;
    }

    TIndexedEntries<TDeallocationRequestState>& DeallocationsState() const
    {
        return State_->NodeDeallocations;
    }

    const TTabletNodeInfoPtr& GetInstanceInfo(const TString& instanceName, const TSchedulerInputState& input)
    {
        return GetOrCrash(input.TabletNodes, instanceName);
    }

    bool IsInstanceReadyToBeDeallocated(
        const TString& instanceName,
        const TString& deallocationId,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) const
    {
        auto nodeIt = input.TabletNodes.find(instanceName);
        if (nodeIt == input.TabletNodes.end()) {
            YT_LOG_ERROR("Cannot find node from deallocation request state (DeallocationId: %v, Node: %v)",
                deallocationId,
                instanceName);
            return false;
        }

        const auto& nodeInfo = nodeIt->second;
        return EnsureNodeDecommissioned(instanceName, nodeInfo, mutations);
    }

    TString GetNannyService(const TZoneInfoPtr& zoneInfo) const
    {
        return zoneInfo->TabletNodeNannyService;
    }

    bool EnsureAllocatedInstanceTagsSet(
        const TString& nodeName,
        const TString& bundleName,
        const TAllocationRequestPtr& allocationInfo,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) const
    {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        if (nodeInfo->State != InstanceStateOnline) {
            return false;
        }

        const auto& annotations = nodeInfo->Annotations;

        if (auto changed = GetInstanceAnnotationsToSet(bundleName, allocationInfo, annotations)) {
            mutations->ChangeNodeAnnotations[nodeName] = changed;
            return false;
        }

        if (annotations->AllocatedForBundle != bundleName) {
            YT_LOG_WARNING("Inconsistent allocation state (AnnotationsBundleName: %v, ActualBundleName: %v, NodeName: %v)",
                annotations->AllocatedForBundle,
                bundleName,
                nodeName);

            mutations->AlertsToFire.push_back({
                .Id = "inconsistent_allocation_state",
                .Description = Format("Inconsistent allocation state: Node annotation bundle name %v, actual bundle name %v.",
                    annotations->AllocatedForBundle,
                    bundleName)
            });

            return false;
        }

        if (nodeInfo->Decommissioned) {
            mutations->ChangedDecommissionedFlag[nodeName] = false;
            return false;
        }

        if (AliveBundleNodes_.count(nodeName) == 0) {
            return false;
        }

        return true;
    }

    bool EnsureDeallocatedInstanceTagsSet(
        const TString& nodeName,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        const auto& instanceInfo = GetInstanceInfo(nodeName, input);
        const auto& annotations = instanceInfo->Annotations;
        if (!annotations->AllocatedForBundle.empty() || annotations->Allocated) {
            mutations->ChangeNodeAnnotations[nodeName] = New<TInstanceAnnotations>();
            return false;
        }
        return true;
    }

    const THashSet<TString>& GetAliveInstancies() const
    {
        return AliveBundleNodes_;
    }

    const std::vector<TString>& GetInstancies() const
    {
        return BundleNodes_;
    }

    std::vector<TString> PeekInstanciesToDeallocate(
        int nodeCountToRemove,
        const TSchedulerInputState& input) const
    {
        std::vector<TNodeRemoveOrder> nodesOrder;
        nodesOrder.reserve(AliveBundleNodes_.size());

        for (auto nodeName : AliveBundleNodes_) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodesOrder.push_back({GetUsedSlotCount(nodeInfo), nodeName});
        }

        auto endIt = nodesOrder.end();
        if (std::ssize(nodesOrder) > nodeCountToRemove) {
            endIt = nodesOrder.begin() + nodeCountToRemove;
            std::nth_element(nodesOrder.begin(), endIt, nodesOrder.end());
        }

        std::vector<TString> result;
        result.reserve(std::distance(nodesOrder.begin(), endIt));
        for (auto it = nodesOrder.begin(); it != endIt; ++it) {
            result.push_back(it->NodeName);
        }

        return result;
    }

private:
    TBundleControllerStatePtr State_;
    const std::vector<TString>& BundleNodes_;
    const THashSet<TString>& AliveBundleNodes_;
};

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyAllocatorAdapter
{
public:
    TRpcProxyAllocatorAdapter(
        TBundleControllerStatePtr state,
        const std::vector<TString>& bundleProxies,
        const THashSet<TString>& aliveProxies)
        : State_(std::move(state))
        , BundleProxies_(bundleProxies)
        , AliveProxies_(aliveProxies)
    { }

    bool IsNewDeallocationAllowed(const TBundleInfoPtr& /*bundleInfo*/)
    {
        if (!State_->ProxyAllocations.empty() ||
            !State_->ProxyDeallocations.empty())
        {
            // It is better not to mix allocation and deallocation requests.
            return false;
        }

        return true;
    }

    bool IsInstanceCountLimitReached(
        const TString& zoneName,
        const TZoneInfoPtr& zoneInfo,
        const TSchedulerInputState& input) const
    {
        auto it = input.ZoneProxies.find(zoneName);
        if (it == input.ZoneProxies.end()) {
            // No allocated rpc proxies for this zone
            return false;
        }

        int currentZoneProxyCount = std::ssize(it->second);
        if (currentZoneProxyCount >= zoneInfo->MaxRpcProxyCount) {
            YT_LOG_WARNING("Max Rpc proxies count limit reached"
                " (Zone: %v, CurrentZoneRpcProxyCount: %v, MaxRpcProxyCount: %v)",
                zoneName,
                currentZoneProxyCount,
                zoneInfo->MaxRpcProxyCount);
            return true;
        }
        return false;
    }

    int GetTargetInstanceCount(const TBundleInfoPtr& bundleInfo) const
    {
        return bundleInfo->TargetConfig->RpcProxyCount;
    }

    int GetInstanceRole() const
    {
        return YTRoleTypeRpcProxy;
    }

    const TInstanceResourcesPtr& GetResourceGuarantee(const TBundleInfoPtr& bundleInfo) const
    {
        return bundleInfo->TargetConfig->RpcProxyResourceGuarantee;
    }

    const TString& GetInstanceType()
    {
        static const TString RpcProxy = "rpc";
        return RpcProxy;
    }

    TIndexedEntries<TAllocationRequestState>& AllocationsState() const
    {
        return State_->ProxyAllocations;
    }

    TIndexedEntries<TDeallocationRequestState>& DeallocationsState() const
    {
        return State_->ProxyDeallocations;
    }

    const TRpcProxyInfoPtr& GetInstanceInfo(const TString& instanceName, const TSchedulerInputState& input)
    {
        return GetOrCrash(input.RpcProxies, instanceName);
    }

    bool IsInstanceReadyToBeDeallocated(
        const TString& /*instanceName*/,
        const TString& /*deallocationId*/,
        const TSchedulerInputState& /*input*/,
        TSchedulerMutations* /*mutations*/) const
    {
        return true;
    }

    TString GetNannyService(const TZoneInfoPtr& zoneInfo) const
    {
        return zoneInfo->RpcProxyNannyService;
    }

    bool EnsureAllocatedInstanceTagsSet(
        const TString& proxyName,
        const TString& bundleName,
        const TAllocationRequestPtr& allocationInfo,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) const
    {
        auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        if (!proxyInfo->Alive) {
            return false;
        }

        const auto& annotations = proxyInfo->Annotations;
        if (auto changed = GetInstanceAnnotationsToSet(bundleName, allocationInfo, annotations)) {
            mutations->ChangedProxyAnnotations[proxyName] = changed;
            return false;
        }

        if (annotations->AllocatedForBundle != bundleName) {
            YT_LOG_WARNING("Inconsistent allocation state (AnnotationsBundleName: %v, ActualBundleName: %v, ProxyName: %v)",
                annotations->AllocatedForBundle,
                bundleName,
                proxyName);

            return false;
        }

        if (AliveProxies_.count(proxyName) == 0) {
            return false;
        }

        return true;
    }

    bool EnsureDeallocatedInstanceTagsSet(
        const TString& proxyName,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        const auto& instanceInfo = GetInstanceInfo(proxyName, input);
        const auto& annotations = instanceInfo->Annotations;
        if (!annotations->AllocatedForBundle.empty() || annotations->Allocated) {
            mutations->ChangedProxyAnnotations[proxyName] = New<TInstanceAnnotations>();
            return false;
        }
        return true;
    }

    const THashSet<TString>& GetAliveInstancies() const
    {
        return AliveProxies_;
    }

    const std::vector<TString>& GetInstancies() const
    {
        return BundleProxies_;
    }

    std::vector<TString> PeekInstanciesToDeallocate(
        int proxiesCountToRemove,
        const TSchedulerInputState& /*input*/) const
    {
        YT_VERIFY(std::ssize(AliveProxies_) >= proxiesCountToRemove);
        return {AliveProxies_.begin(), std::next(AliveProxies_.begin(), proxiesCountToRemove)};
    }

private:
    TBundleControllerStatePtr State_;
    const std::vector<TString>& BundleProxies_;
    const THashSet<TString>& AliveProxies_;
};

////////////////////////////////////////////////////////////////////////////////

void ManageInstancies(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    // For each zone create virtual spare bundles
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        auto spareVirtualBundle = GetSpareBundleName(zoneName);
        auto bundleInfo = New<TBundleInfo>();
        bundleInfo->TargetConfig = zoneInfo->SpareTargetConfig;
        bundleInfo->EnableBundleController = true;
        bundleInfo->EnableTabletCellManagement = false;
        bundleInfo->Zone = zoneName;
        input.Bundles[spareVirtualBundle] = bundleInfo;
    }

    auto zoneDisrupted = GetZoneDisruptedState(input);

    TInstanceManager<TTabletNodeAllocatorAdapter> nodeAllocator(BundleControllerLogger);
    TInstanceManager<TRpcProxyAllocatorAdapter> proxyAllocator(BundleControllerLogger);

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        auto bundleState = New<TBundleControllerState>();
        if (auto it = input.BundleStates.find(bundleName); it != input.BundleStates.end()) {
            bundleState = NYTree::CloneYsonSerializable(it->second);
        }
        mutations->ChangedStates[bundleName] = bundleState;

        if (zoneDisrupted[bundleInfo->Zone].NodesDisrupted) {
            YT_LOG_WARNING("Node management skipped for bundle due zone unhealthy state"
                " (BundleName: %v, Zone: %v)",
                bundleName,
                bundleInfo->Zone);
        } else {
            const auto& bundleNodes = input.BundleNodes[bundleName];
            auto aliveNodes = GetAliveNodes(bundleName, bundleNodes, input);
            TTabletNodeAllocatorAdapter nodeAdapter(bundleState, bundleNodes, aliveNodes);
            nodeAllocator.ManageInstancies(bundleName, &nodeAdapter, input, mutations);
        }

        if (zoneDisrupted[bundleInfo->Zone].ProxiesDisrupted) {
            YT_LOG_WARNING("RpcProxies management skipped for bundle due zone unhealthy state"
                " (BundleName: %v, Zone: %v)",
                bundleName,
                bundleInfo->Zone);
        } else {
            const auto& bundleProxies = input.BundleProxies[bundleName];
            auto aliveProxies = GetAliveProxies(bundleProxies, input);
            TRpcProxyAllocatorAdapter proxyAdapter(bundleState, bundleProxies, aliveProxies);
            proxyAllocator.ManageInstancies(bundleName, &proxyAdapter, input, mutations);
        }
    }

    // TODO(capone212): cleanup stale instances: that are gone a lot of time ago (2 weeks)
}

////////////////////////////////////////////////////////////////////////////////

void ManageCells(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        const auto& bundleNodes = input.BundleNodes[bundleName];
        CreateRemoveTabletCells(bundleName, bundleNodes, input, mutations);
        ProcessRemovingCells(bundleName, bundleNodes, input, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageBundlesDynamicConfig(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    TBundlesDynamicConfig freshConfig;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->EnableTabletNodeDynamicConfig) {
            continue;
        }

        if (bundleInfo->NodeTagFilter.empty()) {
            YT_LOG_WARNING("Bundle has empty node tag filter (BundleName: %v)", bundleName);
            continue;
        }

        auto bundleConfig = New<TBundleDynamicConfig>();
        bundleConfig->CpuLimits = NYTree::CloneYsonSerializable(bundleInfo->TargetConfig->CpuLimits);
        bundleConfig->MemoryLimits = NYTree::CloneYsonSerializable(bundleInfo->TargetConfig->MemoryLimits);

        freshConfig[bundleInfo->NodeTagFilter] = bundleConfig;
    }

    if (AreNodesEqual(ConvertTo<NYTree::IMapNodePtr>(freshConfig), ConvertTo<NYTree::IMapNodePtr>(input.DynamicConfig))) {
        return;
    }

    YT_LOG_INFO("Bundles dynamic config has changed (Config: %Qv)",
        ConvertToYsonString(freshConfig, EYsonFormat::Text));

    mutations->DynamicConfig = freshConfig;
}

////////////////////////////////////////////////////////////////////////////////

void ScheduleBundles(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    input.ZoneNodes = MapZonesToInstancies(input, input.TabletNodes);
    input.ZoneProxies = MapZonesToInstancies(input, input.RpcProxies);
    input.BundleNodes = MapBundlesToInstancies(input.TabletNodes);
    input.BundleProxies = MapBundlesToInstancies(input.RpcProxies);
    input.PodIdToInstanceName = MapPodIdToInstanceName(input);

    ManageBundlesDynamicConfig(input, mutations);
    ManageInstancies(input, mutations);
    ManageCells(input, mutations);
    ManageSystemAccountLimit(input, mutations);
    ManageNodeTagFilters(input, mutations);
    ManageRpcProxyRoles(input, mutations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
