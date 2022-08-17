#include "bundle_scheduler.h"
#include "config.h"

#include <library/cpp/yt/yson_string/public.h>

#include <compare>

namespace NYT::NCellBalancer {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BundleController;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsAllocationFailed(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "FAILED";
}

bool IsAllocationCompleted(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "COMPLETED";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TString GetBundleNameFor(const TString& /*name*/, const TTabletNodeInfoPtr& info)
{
    return info->Annotations->AllocatedForBundle;
}

TString GetPodIdForNode(const TString& name, const TTabletNodeInfoPtr& /*info*/)
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

TSchedulerInputState::TBundleToNodeMapping MapBundlesToTabletNodes(const TSchedulerInputState& input)
{
    TSchedulerInputState::TBundleToNodeMapping result;

    for (const auto& [nodeName, nodeInfo] : input.TabletNodes) {
        auto bundleName = GetBundleNameFor(nodeName, nodeInfo);
        if (!bundleName.empty()) {
            result[bundleName].push_back(nodeName);
        }
    }

    return result;
}

TSchedulerInputState::TZoneToNodesMap MapZonesToTabletNodes(const TSchedulerInputState& input)
{
    THashMap<TString, TString> nannyServiceToZone;
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        if (zoneInfo->NannyService.empty()) {
            continue;
        }
        nannyServiceToZone[zoneInfo->NannyService] = zoneName;
    }

    TSchedulerInputState::TZoneToNodesMap result;
    for (const auto& [nodeName, nodeInfo] : input.TabletNodes) {
        if (!nodeInfo->Annotations->Allocated) {
            continue;
        }
        auto it = nannyServiceToZone.find(nodeInfo->Annotations->NannyService);
        if (it == nannyServiceToZone.end()) {
            continue;
        }
        const auto& zoneName = it->second;
        result[zoneName].push_back(nodeName);
    }

    return result;
}

THashMap<TString, TString> MapPodIdToNodeName(const TSchedulerInputState& input)
{
    THashMap<TString, TString> result;

    for (const auto& [nodeName, nodeInfo] : input.TabletNodes) {
        auto podId = GetPodIdForNode(nodeName, nodeInfo);
        result[podId] = nodeName;
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

TString GetPodIdTemplate(const TString& cluster, const TString& bundleName)
{
    return Format("<short-hostname>-%v-aa-tab-%v", bundleName, cluster);
}

bool MaxTabletNodesLimitReached(
    const TString& zoneName,
    const TZoneInfoPtr& zoneInfo,
    const TSchedulerInputState& input)
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

void InitNewAllocations(
    const TString& bundleName,
    const std::vector<TString>& bundleNodes,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    // TODO(capone212): think about allocation/deallocation budget.
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& bundleState = mutations->ChangedStates[bundleName];

    YT_VERIFY(bundleInfo->EnableBundleController);

    int aliveNodeCount = std::ssize(GetAliveNodes(bundleName, bundleNodes, input));
    int instanceCountToAllocate = bundleInfo->TargetConfig->TabletNodeCount -
        aliveNodeCount -
        std::ssize(bundleState->Allocations);

    YT_LOG_DEBUG("Scheduling allocations (BundleName: %v, TabletNodeCount: %v, AliveNodes: %v, "
        "RequestCount: %v, ExistingAllocations: %v)",
        bundleName,
        bundleInfo->TargetConfig->TabletNodeCount,
        aliveNodeCount,
        instanceCountToAllocate,
        std::ssize(bundleState->Allocations));

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
    if (instanceCountToAllocate > 0 && MaxTabletNodesLimitReached(bundleInfo->Zone, zoneInfo, input)) {
        mutations->AlertsToFire.push_back(TAlert{
            .Id = "zone_nodes_limit_reached",
            .Description = Format("Cannot allocate new tablet node at zone %v for bundle %v.", bundleInfo->Zone, bundleName)
        });
        return;
    }

    for (int i = 0; i < instanceCountToAllocate; ++i) {
        TString allocationId = ToString(TGuid::Create());

        YT_LOG_INFO("Init tablet node allocation for bundle (BundleName: %v, AllocationId: %v)",
            bundleName,
            allocationId);

        auto spec = New<TAllocationRequestSpec>();
        spec->YPCluster = zoneInfo->YPCluster;
        spec->NannyService = zoneInfo->NannyService;
        *spec->ResourceRequest = *bundleInfo->TargetConfig->TabletNodeResourceGuarantee;
        spec->PodIdTemplate = GetPodIdTemplate(input.Config->Cluster, bundleName);
        spec->InstanceRole = YTRoleTypeTabNode;

        auto request = New<TAllocationRequest>();
        request->Spec = spec;
        mutations->NewAllocations[allocationId] = request;
        auto allocationState = New<TAllocationRequestState>();
        allocationState->CreationTime = TInstant::Now();
        bundleState->Allocations[allocationId] = allocationState;
    }
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

std::vector<TString> PeekNodesToDeallocate(
    const THashSet<TString>& aliveNodes,
    int nodeCountToRemove,
    const TSchedulerInputState& input)
{
    std::vector<TNodeRemoveOrder> nodesOrder;
    nodesOrder.reserve(aliveNodes.size());

    for (auto nodeName : aliveNodes) {
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

int GetTargetCellCount(const TBundleInfoPtr& bundleInfo)
{
    const auto& targetConfig = bundleInfo->TargetConfig;
    return targetConfig->TabletNodeCount * targetConfig->CpuLimits->WriteThreadPoolSize;
}

void InitNewDeallocations(
    const TString& bundleName,
    const std::vector<TString>& bundleNodes,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    // TODO(capone212): think about allocation deallocation budget.
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& bundleState = mutations->ChangedStates[bundleName];
    YT_VERIFY(bundleInfo->EnableBundleController);

    if (!bundleState->Allocations.empty() ||
        !bundleState->Deallocations.empty() ||
        !bundleState->RemovingCells.empty())
    {
        // It is better not to mix allocation and deallocation requests.
        return;
    }

    if (bundleInfo->EnableTabletCellManagement) {
        if (GetTargetCellCount(bundleInfo) != std::ssize(bundleInfo->TabletCellIds)) {
            // Wait for tablet cell management to complete.
            return;
        }
    }

    auto aliveNodes = GetAliveNodes(bundleName, bundleNodes, input);
    int instanceCountToDeallocate = std::ssize(aliveNodes) - bundleInfo->TargetConfig->TabletNodeCount;

    YT_LOG_DEBUG("Scheduling deallocations (BundleName: %v, TabletNodeCount: %v, AliveNodes: %v, "
        "RequestCount: %v, ExistingDeallocations: %v)",
        bundleName,
        bundleInfo->TargetConfig->TabletNodeCount,
        std::ssize(aliveNodes),
        instanceCountToDeallocate,
        std::ssize(bundleState->Deallocations));

    if (instanceCountToDeallocate <= 0) {
        return;
    }

    const auto nodesToRemove = PeekNodesToDeallocate(aliveNodes, instanceCountToDeallocate, input);

    for (const auto& nodeName : nodesToRemove) {
        TString deallocationId = ToString(TGuid::Create());
        auto deallocationState = New<TDeallocationRequestState>();
        deallocationState->CreationTime = TInstant::Now();
        deallocationState->NodeName = nodeName;
        bundleState->Deallocations[deallocationId] = deallocationState;

        YT_LOG_INFO("Init node deallocation (BundleName: %v, NodeName: %v, DeallocationId: %v)",
            bundleName,
            nodeName,
            deallocationId);
    }
}

TString LocateAllocatedTabletNode(
    const TAllocationRequestPtr& requestInfo,
    const TSchedulerInputState& input)
{
    if (!IsAllocationCompleted(requestInfo)) {
        return {};
    }

    const auto& podId = requestInfo->Status->PodId;
    auto it = input.PodIdToNodeName.find(podId);
    if (it != input.PodIdToNodeName.end()) {
        return it->second;
    }

    return {};
}

bool EnsureAllocatedNodeTagsSet(
    const TString& nodeName,
    const TString& bundleName,
    const THashSet<TString>& aliveBundleNodes,
    const TAllocationRequestPtr & allocationInfo,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
    if (nodeInfo->State != InstanceStateOnline) {
        return false;
    }

    auto& annotations = nodeInfo->Annotations;
    if (annotations->AllocatedForBundle.empty() || !annotations->Allocated) {
        annotations->YPCluster = allocationInfo->Spec->YPCluster;
        annotations->NannyService = allocationInfo->Spec->NannyService;
        annotations->AllocatedForBundle = bundleName;
        annotations->Allocated = true;
        mutations->ChangeNodeAnnotations[nodeName] = annotations;
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
    }

    if (nodeInfo->Decommissioned) {
        mutations->ChangedDecommissionedFlag[nodeName] = false;
        return false;
    }

    if (aliveBundleNodes.count(nodeName) == 0) {
        return false;
    }

    return true;
}

bool EnsureDeallocatedNodeTagsSet(
    const TString& nodeName,
    const TTabletNodeInfoPtr& nodeInfo,
    TSchedulerMutations* mutations)
{
    const auto& annotations = nodeInfo->Annotations;
    if (!annotations->AllocatedForBundle.empty() || annotations->Allocated) {
        mutations->ChangeNodeAnnotations[nodeName] = New<TTabletNodeAnnotationsInfo>();
        return false;
    }
    return true;
}

void ProcessExistingAllocations(
    const TString& bundleName,
    const std::vector<TString>& bundleNodes,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& bundleState = mutations->ChangedStates[bundleName];
    auto aliveNodes = GetAliveNodes(bundleName, bundleNodes, input);

    TIndexedEntries<TAllocationRequestState> aliveAllocations;
    for (const auto& [allocationId, allocationState] : bundleState->Allocations) {
        auto it = input.AllocationRequests.find(allocationId);
        if (it == input.AllocationRequests.end()) {
            YT_LOG_WARNING("Cannot find allocation (AllocationId: %v)",
                allocationId);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "can_not_find_allocation_request",
                .Description = Format("Allocation request %v "
                    "found in bundle state, but is absent in hulk allocations.",
                    allocationId),
            });
            continue;
        }

        const auto& allocationInfo = it->second;

        if (IsAllocationFailed(allocationInfo)) {
            YT_LOG_WARNING("Allocation Failed (AllocationId: %v)",
                allocationId);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "table_node_allocation_failed",
                .Description = Format("Allocation request %v has failed.",
                    allocationId),
            });
            continue;
        }

        auto nodeName = LocateAllocatedTabletNode(allocationInfo, input);
        if (!nodeName.empty() && EnsureAllocatedNodeTagsSet(nodeName, bundleName, aliveNodes, allocationInfo, input, mutations)) {
            YT_LOG_INFO("Tablet node allocation completed (Node: %v, AllocationId: %v)",
                nodeName,
                allocationId);
            continue;
        }

        auto allocationAge = TInstant::Now() - allocationState->CreationTime;
        if (allocationAge > input.Config->HulkRequestTimeout) {
            YT_LOG_WARNING("Allocation Request is stuck (AllocationId: %v, AllocationAge: %v, Threshold: %v)",
                allocationId,
                allocationAge,
                input.Config->HulkRequestTimeout);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "stuck_tablet_node_allocation",
                .Description = Format("Found stuck allocation %v and age %v which is more than threshold %v.",
                    allocationId,
                    allocationAge,
                    input.Config->HulkRequestTimeout),
            });
            continue;
        }

        YT_LOG_DEBUG("Tracking existing allocation (AllocationId: %v, Bundle: %v,  Node: %v)",
            allocationId,
            bundleName,
            nodeName);

        aliveAllocations[allocationId] = allocationState;
    }

    bundleState->Allocations.swap(aliveAllocations);
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

void CreateHulkDeallocationRequest(
    const TString& deallocationId,
    const TString& nodeName,
    const TTabletNodeInfoPtr& nodeInfo,
    TSchedulerMutations* mutations)
{
    const auto& nodeAnnotations = nodeInfo->Annotations;
    auto request = New<TDeallocationRequest>();
    auto& spec = request->Spec;
    spec->YPCluster = nodeAnnotations->YPCluster;
    spec->PodId = GetPodIdForNode(nodeName, nodeInfo);
    spec->InstanceRole = YTRoleTypeTabNode;
    mutations->NewDeallocations[deallocationId] = request;
}

// Returns false if current deallocation should not be tracked any more.
bool ProcessDeallocation(
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
            .Id = "stuck_tablet_node_deallocation",
            .Description = Format("Found stuck deallocation %v and age %v which is more than threshold %v.",
                deallocationId,
                deallocationAge,
                input.Config->HulkRequestTimeout),
        });
    }

    const auto& nodeName = deallocationState->NodeName;

    auto nodeIt = input.TabletNodes.find(nodeName);
    if (nodeIt == input.TabletNodes.end()) {
        YT_LOG_ERROR("Cannot find node from deallocation request state (DeallocationId: %v, Node: %v)",
            deallocationId,
            nodeName);
        return true;
    }

    const auto& nodeInfo = nodeIt->second;

    if (!EnsureNodeDecommissioned(nodeName, nodeInfo, mutations)) {
        return true;
    }

    if (!deallocationState->HulkRequestCreated) {
        CreateHulkDeallocationRequest(deallocationId, nodeName, nodeInfo, mutations);
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
        return false;
    }

    if (IsAllocationFailed(it->second)) {
        YT_LOG_WARNING("Deallocation Failed (AllocationId: %v)",
            deallocationId);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "table_node_deallocation_failed",
            .Description = Format("Deallocation request %v has failed.",
                deallocationId),
        });
        return false;
    }

    if (IsAllocationCompleted(it->second) && EnsureDeallocatedNodeTagsSet(nodeName, nodeInfo, mutations)) {
        YT_LOG_INFO("Tablet node deallocation completed (Node: %v, AllocationId: %v)",
            nodeName,
            deallocationId);
        return false;
    }

    YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, NodeName: %v)",
        deallocationId,
        nodeName);
    return true;
}

void ProcessExistingDeallocations(
    const TString& bundleName,
    const std::vector<TString>& /*bundleNodes*/,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& bundleState = mutations->ChangedStates[bundleName];
    TIndexedEntries<TDeallocationRequestState> aliveDeallocations;

    for (const auto& [deallocationId, deallocationState] : bundleState->Deallocations) {
        if (ProcessDeallocation(deallocationId, deallocationState, input, mutations)) {
            aliveDeallocations[deallocationId] = deallocationState;
        }
    }

    bundleState->Deallocations.swap(aliveDeallocations);
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
        !bundleState->Allocations.empty() ||
        !bundleState->Deallocations.empty())
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

TString GetSpareBundleName(const TString& /*zoneName*/)
{
    // TODO(capone212): consider adding zone name.
    return "spare";
}

THashMap<TString, bool> GetZoneDisruptedState(TSchedulerInputState& input)
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

    THashMap<TString, bool> result;
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        int disruptedThreshold = zoneInfo->SpareTargetConfig->TabletNodeCount * zoneInfo->DisruptedThresholdFactor;
        bool disrupted = disruptedThreshold > 0 && zoneOfflineNodeCount[zoneName] > disruptedThreshold;

        result[zoneName] = disrupted;

        YT_LOG_WARNING_IF(disrupted, "Zone is in disrupted state"
            " (ZoneName: %v, NannyService: %v, DisruptedThreshold: %v, OfflineNodeCount: %v)",
            zoneName,
            zoneInfo->NannyService,
            disruptedThreshold,
            zoneOfflineNodeCount[zoneName]);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void ManageNodesAndCells(TSchedulerInputState& input, TSchedulerMutations* mutations)
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

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        if (zoneDisrupted[bundleInfo->Zone]) {
            YT_LOG_WARNING("Node and cells management skipped for bundle due zone unhealthy state"
                " (BundleName: %v, Zone: %v)",
                bundleName,
                bundleInfo->Zone);
            continue;
        }

        auto bundleState = New<TBundleControllerState>();
        if (auto it = input.BundleStates.find(bundleName); it != input.BundleStates.end()) {
            bundleState = NYTree::CloneYsonSerializable(it->second);
        }
        const auto& bundleNodes = input.BundleNodes[bundleName];
        mutations->ChangedStates[bundleName] = bundleState;

        ProcessExistingAllocations(bundleName, bundleNodes, input, mutations);
        InitNewAllocations(bundleName, bundleNodes, input, mutations);

        CreateRemoveTabletCells(bundleName, bundleNodes, input, mutations);
        ProcessRemovingCells(bundleName, bundleNodes, input, mutations);

        ProcessExistingDeallocations(bundleName, bundleNodes, input, mutations);
        InitNewDeallocations(bundleName, bundleNodes, input, mutations);
    }

    // TODO(capone212): cleanup stale instances: that are gone a lot of time ago (2 weeks)
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
    input.ZoneNodes = MapZonesToTabletNodes(input);
    input.BundleNodes = MapBundlesToTabletNodes(input);
    input.PodIdToNodeName = MapPodIdToNodeName(input);

    ManageBundlesDynamicConfig(input, mutations);
    ManageNodesAndCells(input, mutations);
    ManageNodeTagFilters(input, mutations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
