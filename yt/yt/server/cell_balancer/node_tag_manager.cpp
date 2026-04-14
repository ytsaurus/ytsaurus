#include "node_tag_manager.h"

#include "bundle_scheduler.h"
#include "config.h"
#include "cypress_bindings.h"
#include "input_state.h"
#include "mutations.h"
#include "node_tracker.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

// Returns the list of bundles whose tags are present at the node.
std::vector<std::string> GetBundlesByTags(
    const TTabletNodeInfoPtr& nodeInfo,
    const THashMap<std::string, std::string>& nodeTagFilterToBundleName)
{
    std::vector<std::string> result;

    for (const auto& tag : nodeInfo->UserTags) {
        if (auto it = nodeTagFilterToBundleName.find(tag); it != nodeTagFilterToBundleName.end()) {
            result.push_back(GetOrCrash(nodeTagFilterToBundleName, tag));
        }
    }

    return result;
}

// Returns number of nodes in |aliveBundleNodes| that have bundle tag
// and are not decommissioned.
int GetReadyNodeCount(
    const std::string& bundleTag,
    const THashSet<std::string>& aliveBundleNodes,
    const TIndexedEntries<TTabletNodeInfo>& tabletNodes)
{
    int readyNodeCount = 0;
    for (const auto& nodeAddress : aliveBundleNodes) {
        const auto& nodeInfo = GetOrCrash(tabletNodes, nodeAddress);
        if (nodeInfo->UserTags.contains(bundleTag) && !nodeInfo->Decommissioned) {
            ++readyNodeCount;
        }
    }

    return readyNodeCount;
}

bool AreAllTabletSlotsEmpty(const TTabletNodeInfoPtr& nodeInfo)
{
    const auto& slots = nodeInfo->TabletSlots;

    return std::all_of(slots.begin(), slots.end(), [] (const TTabletSlotPtr& slot) {
        return slot->State == TabletSlotStateEmpty;
    });
}


////////////////////////////////////////////////////////////////////////////////

TNodeTagManager::TNodeTagManager(
    std::string bundleName,
    const TSchedulerInputState& input,
    TSpareInstanceAllocator<TSpareNodesInfo>* spareNodeAllocator,
    TSchedulerMutations* mutations,
    INodeTrackerPtr nodeTracker)
    : BundleName_(std::move(bundleName))
    , Input_(input)
    , SpareNodeAllocator_(spareNodeAllocator)
    , Mutations_(mutations)
    , NodeTracker_(std::move(nodeTracker))
    , Logger(BundleControllerLogger().WithTag("Bundle: %v", BundleName_))
{ }

bool TNodeTagManager::ProcessNodeAssignment(const std::string& nodeAddress)
{
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    const auto& nodeInfo = GetOrCrash(Input_.TabletNodes, nodeAddress);

    if (!nodeInfo->UserTags.contains(nodeTagFilter)) {
        auto tags = nodeInfo->UserTags;
        tags.insert(nodeTagFilter);
        Mutations_->ChangedNodeUserTags[nodeAddress] = Mutations_->WrapMutation(std::move(tags));
        if (Input_.Config->DecommissionReleasedNodes) {
            Mutations_->ChangedDecommissionedFlag[nodeAddress] = Mutations_->WrapMutation(true);
        }

        YT_LOG_INFO("Assigning node to bundle: setting node tag and decommission flag "
            "(NodeAddress: %v, NodeTagFilter: %v, Decommissioned: %v)",
            nodeAddress,
            nodeTagFilter,
            Input_.Config->DecommissionReleasedNodes);

        return false;
    }

    bool isNodeOnline = nodeInfo->IsOnline();

    if (!isNodeOnline) {
        YT_LOG_WARNING("Node went offline during assigning to bundle "
            "(NodeAddress: %v, NodeState: %v, Banned: %v, LastSeenTime: %v)",
            nodeAddress,
            nodeInfo->State,
            nodeInfo->Banned,
            nodeInfo->LastSeenTime);
    }

    const auto& targetConfig = bundleInfo->TargetConfig;
    auto cpuLimits = GetBundleEffectiveCpuLimits(BundleName_, bundleInfo, Input_);

    if (isNodeOnline && cpuLimits->WriteThreadPoolSize != std::ssize(nodeInfo->TabletSlots)) {
        YT_LOG_DEBUG("Node has not applied dynamic bundle config yet "
            "(NodeAddress: %v, ExpectedSlotCount: %v, ActualSlotCount: %v)",
            nodeAddress,
            cpuLimits->WriteThreadPoolSize,
            std::ssize(nodeInfo->TabletSlots));

        if (NodeTracker_) {
            NodeTracker_->RequestConfigUpdate(nodeAddress);
        }

        return false;
    }

    auto tabletStatic = targetConfig->MemoryLimits->TabletStatic;
    if (isNodeOnline && tabletStatic && *tabletStatic != nodeInfo->Statistics->Memory->TabletStatic->Limit) {
        YT_LOG_DEBUG("Node has not applied dynamic bundle config yet "
            "(NodeAddress: %v, ExpectedTabletStatic: %v, ActualTabletStatic: %v)",
            nodeAddress,
            tabletStatic,
            nodeInfo->Statistics->Memory->TabletStatic->Limit);

        if (NodeTracker_) {
            NodeTracker_->RequestConfigUpdate(nodeAddress);
        }

        return false;
    }

    if (nodeInfo->Decommissioned) {
        YT_LOG_DEBUG("Removing decommissioned flag after applying bundle dynamic config "
            "(NodeAddress: %v)",
            nodeAddress);
        Mutations_->ChangedDecommissionedFlag[nodeAddress] = Mutations_->WrapMutation(false);
        return false;
    }

    return true;
}

bool TNodeTagManager::ProcessNodeReleasement(
    const std::string& nodeAddress,
    bool leaveDecommissioned)
{
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    const auto& nodeInfo = GetOrCrash(Input_.TabletNodes, nodeAddress);

    if (nodeInfo->UserTags.contains(nodeTagFilter)) {
        if (Input_.Config->DecommissionReleasedNodes) {
            if (!nodeInfo->Decommissioned) {
                Mutations_->ChangedDecommissionedFlag[nodeAddress] = Mutations_->WrapMutation(true);
                YT_LOG_DEBUG("Releasing node: setting decommissioned flag (NodeAddress: %v)",
                    nodeAddress);
                return false;
            }

            if (!AreAllTabletSlotsEmpty(nodeInfo)) {
                YT_LOG_DEBUG("Releasing node: not all tablet cells are empty (NodeAddress: %v)",
                    nodeAddress);
                return false;
            }
        } else {
            YT_LOG_DEBUG("Releasing node: will not decommission (NodeAddress: %v)",
                nodeAddress);
        }

        auto userTags = nodeInfo->UserTags;

        YT_LOG_INFO("Releasing node: removing bundle tag "
            "(NodeAddress: %v, Tags: %v)",
            nodeAddress,
            nodeInfo->UserTags);

        userTags.erase(nodeTagFilter);
        Mutations_->ChangedNodeUserTags[nodeAddress] = Mutations_->WrapMutation(userTags);
        return false;
    }

    if (nodeInfo->Decommissioned != leaveDecommissioned) {
        YT_LOG_DEBUG("Releasing node: setting target decommissioned state "
            "(NodeAddress: %v, ShouldDecommission: %v)",
            nodeAddress,
            leaveDecommissioned);
        Mutations_->ChangedDecommissionedFlag[nodeAddress] = Mutations_->WrapMutation(leaveDecommissioned);
        return false;
    }

    return true;
}

void TNodeTagManager::TryCreateSpareNodeReleasements(
    int slotsToRelease,
    const TSpareNodesInfo* spareNodesInfo)
{
    const auto& bundleState = Mutations_->ChangedStates[BundleName_];

    if (!bundleState->SpareNodeAssignments.empty()) {
        // Let's not mix spare node assigning with releasing.
        return;
    }

    if (!bundleState->RemovingCells.empty()) {
        return;
    }

    auto spareNodesIt = spareNodesInfo->UsedByBundle.find(BundleName_);
    if (spareNodesIt == spareNodesInfo->UsedByBundle.end()) {
        return;
    }

    const auto& usingSpareNodes = spareNodesIt->second;
    auto it = usingSpareNodes.begin();

    auto now = TInstant::Now();

    while (slotsToRelease > 0 && it != usingSpareNodes.end()) {
        const auto& nodeAddress = *it;

        auto nodeInfo = GetOrCrash(Input_.TabletNodes, nodeAddress);

        if (std::ssize(nodeInfo->TabletSlots) <= slotsToRelease) {
            YT_LOG_INFO("Releasing spare node (NodeAddress: %v, NodeSlotCount: %v, SlotsToRelease: %v)",
                nodeAddress,
                std::ssize(nodeInfo->TabletSlots),
                slotsToRelease);

            auto operation = New<TNodeTagFilterOperationState>();
            operation->CreationTime = now;
            bundleState->SpareNodeReleasements[nodeAddress] = operation;

            slotsToRelease -= std::ssize(nodeInfo->TabletSlots);
        }
        ++it;
    }
}

void TNodeTagManager::TryCreateSpareNodeAssignments(
    const std::string& dataCenterName,
    int slotsToAdd,
    TSpareInstanceAllocator<TSpareNodesInfo>* spareNodeAllocator)
{
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& bundleState = Mutations_->ChangedStates[BundleName_];
    const auto& zoneName = bundleInfo->Zone;

    int perNodeSlotCount = GetBundleEffectiveCpuLimits(BundleName_, bundleInfo, Input_)
        ->WriteThreadPoolSize.value_or(DefaultWriteThreadPoolSize);
    auto now = TInstant::Now();

    while (slotsToAdd > 0 && spareNodeAllocator->HasInstances(zoneName, dataCenterName)) {
        auto nodeAddress = spareNodeAllocator->Allocate(zoneName, dataCenterName, BundleName_);
        auto nodeInfo = GetOrCrash(Input_.TabletNodes, nodeAddress);

        YT_LOG_INFO("Assigning spare node (NodeAddress: %v, NodeSlotCount: %v, SlotsToAdd: %v)",
            nodeAddress,
            perNodeSlotCount,
            slotsToAdd);

        auto operation = New<TNodeTagFilterOperationState>();
        operation->CreationTime = now;
        bundleState->SpareNodeAssignments[nodeAddress] = operation;

        slotsToAdd -= perNodeSlotCount;
    }
}

void TNodeTagManager::TryCreateBundleNodesAssignment(
    const THashSet<std::string>& aliveNodes)
{
    const auto& bundleState = Mutations_->ChangedStates[BundleName_];
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    auto now = TInstant::Now();

    THashSet<std::string> deallocatingNodes;
    for (const auto& [_, deallocation] : bundleState->NodeDeallocations) {
        deallocatingNodes.insert(deallocation->InstanceName);
    }

    for (const auto& nodeAddress : aliveNodes) {
        if (bundleState->BundleNodeAssignments.contains(nodeAddress)) {
            continue;
        }

        if (deallocatingNodes.contains(nodeAddress)) {
            continue;
        }

        const auto& nodeInfo = GetOrCrash(Input_.TabletNodes, nodeAddress);

        if (bundleState->BundleNodeReleasements.contains(nodeAddress)) {
            YT_LOG_WARNING("Trying to cancel bundle node releasement "
                "(NodeAddress: %v, Decommissioned: %v, UserTags: %v)",
                nodeAddress,
                nodeInfo->Decommissioned,
                nodeInfo->UserTags);

            bundleState->BundleNodeReleasements.erase(nodeAddress);
            Mutations_->ChangedDecommissionedFlag[nodeAddress] = Mutations_->WrapMutation(false);
            continue;
        }

        if (nodeInfo->UserTags.count(nodeTagFilter) == 0) {
            auto operation = New<TNodeTagFilterOperationState>();
            operation->CreationTime = now;
            bundleState->BundleNodeAssignments[nodeAddress] = operation;

            YT_LOG_INFO("Creating node tag assignment for bundle node "
                "(NodeAddress: %v, NodeUserTags: %v)",
                nodeAddress,
                nodeInfo->UserTags);
        }
    }
}

void TNodeTagManager::TryCreateBundleNodesReleasement(
    const THashSet<std::string>& nodesToRelease)
{
    const auto& bundleState = Mutations_->ChangedStates[BundleName_];
    if (!bundleState->RemovingCells.empty()) {
        return;
    }

    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    auto now = TInstant::Now();

    for (const auto& nodeAddress : nodesToRelease) {
        if (bundleState->BundleNodeAssignments.contains(nodeAddress) ||
            bundleState->BundleNodeReleasements.contains(nodeAddress))
        {
            continue;
        }

        const auto& nodeInfo = GetOrCrash(Input_.TabletNodes, nodeAddress);

        if (nodeInfo->UserTags.contains(nodeTagFilter)) {
            auto operation = New<TNodeTagFilterOperationState>();
            operation->CreationTime = now;
            bundleState->BundleNodeReleasements[nodeAddress] = operation;

            YT_LOG_INFO("Creating node tag releasement for bundle node "
                "(NodeAddress: %v, NodeUserTags: %v)",
                nodeAddress,
                nodeInfo->UserTags);
        }
    }
}

void TNodeTagManager::RemoveTagsFromNodes(const THashSet<std::string>& nodes)
{
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;

    for (const auto& nodeAddress : nodes) {
        const auto& nodeInfo = GetOrCrash(Input_.TabletNodes, nodeAddress);

        auto userTags = nodeInfo->UserTags;

        if (!userTags.contains(nodeTagFilter)) {
            continue;
        }

        YT_LOG_INFO("Removing bundle tag from offline node "
            "(NodeAddress: %v, Tags: %v)",
            nodeAddress,
            nodeInfo->UserTags);

        userTags.erase(nodeTagFilter);
        Mutations_->ChangedNodeUserTags[nodeAddress] = Mutations_->WrapMutation(userTags);
    }
}

void TNodeTagManager::ProcessNodeAssignments(
    TIndexedEntries<TNodeTagFilterOperationState>* nodeAssignments)
{
    std::vector<std::string> finished;
    auto now = TInstant::Now();

    for (const auto& [nodeAddress, operation] : *nodeAssignments) {
        if (now - operation->CreationTime > Input_.Config->NodeAssignmentTimeout) {
            YT_LOG_WARNING("Node assignment is stuck (NodeAddress: %v)",
                nodeAddress);

            Mutations_->AlertsToFire.push_back({
                .Id = "node_assignment_is_stuck",
                .BundleName = BundleName_,
                .Description = Format("Assigning node %v for bundle %v is taking more time than expected",
                    BundleName_,
                    nodeAddress),
            });
        }

        if (ProcessNodeAssignment(nodeAddress)) {
            YT_LOG_INFO("Node assignment completed (NodeAddress: %v)",
                nodeAddress);

            finished.push_back(nodeAddress);
        }
    }

    for (const auto& node : finished) {
        nodeAssignments->erase(node);
    }
}

void TNodeTagManager::ProcessNodeReleasements(
    bool leaveDecommissioned,
    TIndexedEntries<TNodeTagFilterOperationState>* nodeReleasements)
{
    std::vector<std::string> finished;
    auto now = TInstant::Now();

    for (const auto& [nodeAddress, operation] : *nodeReleasements) {
        if (now - operation->CreationTime > Input_.Config->NodeAssignmentTimeout) {
            YT_LOG_WARNING("Node releasement is stuck (NodeAddress: %v)",
                nodeAddress);

            Mutations_->AlertsToFire.push_back({
                .Id = "node_releasement_is_stuck",
                .BundleName = BundleName_,
                .Description = Format("Releasing node %v for bundle %v is taking more time than expected",
                    BundleName_,
                    nodeAddress),
            });
        }

        if (ProcessNodeReleasement(nodeAddress, leaveDecommissioned)) {
            YT_LOG_INFO("Node releasement completed, cleaned up released node "
                "(NodeAddress: %v)",
                nodeAddress);
            finished.push_back(nodeAddress);
        }
    }

    for (const auto& node : finished) {
        nodeReleasements->erase(node);
    }
}

struct TDataCenterOrder
{
    // Data center does not have enough alive bundle nodes (even with spare ones).
    bool Unfeasible = false;

    // Data center is forbidden by admin.
    bool Forbidden = false;

    // TODO(capone212): User preferences go here.

    int AssignedTabletCellCount = 0;

    // How many nodes we have to assign to bundle, i.e. how many nodes do not have needed node tag.
    int RequiredNodeAssignmentCount = 0;

    // Just dc name alphabetical order for predictability.
    std::string DataCenter;

    auto MakeTuple() const
    {
        return std::tie(Unfeasible, Forbidden, AssignedTabletCellCount, RequiredNodeAssignmentCount, DataCenter);
    }

    bool operator<(const TDataCenterOrder& other) const
    {
        return MakeTuple() < other.MakeTuple();
    }
};

int TNodeTagManager::GetAvailableAliveTabletNodeCount(
    const std::string& dataCenterName,
    const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes,
    const TPerDataCenterSpareNodesInfo& spareNodesInfo)
{
    int result = 0;

    if (auto it = aliveBundleNodes.find(dataCenterName); it != aliveBundleNodes.end()) {
        result += std::ssize(it->second);
    }

    if (auto it = spareNodesInfo.find(dataCenterName); it != spareNodesInfo.end()) {
        const auto& dataCenterSpare = it->second;
        result += std::ssize(dataCenterSpare.FreeNodes);

        const auto& usedByBundle = dataCenterSpare.UsedByBundle;
        auto bundleIt = usedByBundle.find(BundleName_);
        if (bundleIt != usedByBundle.end()) {
            result += std::ssize(bundleIt->second);
        }
    }

    return result;
}

int TNodeTagManager::GetAssignedTabletNodeCount(
    const std::string& nodeTagFilter,
    const std::string& dataCenterName,
    const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes,
    const TPerDataCenterSpareNodesInfo& spareNodesInfo,
    const TIndexedEntries<TTabletNodeInfo>& tabletNodes)
{
    int result = 0;

    if (auto it = aliveBundleNodes.find(dataCenterName); it != aliveBundleNodes.end()) {
        for (const auto& nodeAddress : it->second) {
            auto nodeInfo = GetOrCrash(tabletNodes, nodeAddress);
            if (nodeInfo->UserTags.count(nodeTagFilter) != 0) {
                ++result;
            }
        }
    }

    if (auto it = spareNodesInfo.find(dataCenterName); it != spareNodesInfo.end()) {
        const auto& usedByBundle = it->second.UsedByBundle;

        auto bundleIt = usedByBundle.find(BundleName_);
        if (bundleIt != usedByBundle.end()) {
            result += std::ssize(bundleIt->second);
        }
    }

    return result;
}

int TNodeTagManager::GetAssignedTabletCellCount(
    const std::string& dataCenterName,
    const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes,
    const TIndexedEntries<TTabletNodeInfo>& tabletNodes)
{
    int result = 0;

    if (auto it = aliveBundleNodes.find(dataCenterName); it != aliveBundleNodes.end()) {
        for (const auto& nodeAddress : it->second) {
            auto nodeInfo = GetOrCrash(tabletNodes, nodeAddress);
            for (const auto& slot : nodeInfo->TabletSlots) {
                if (slot->State == PeerStateLeading) {
                    ++result;
                }
            }
        }
    }

    return result;
}

THashSet<std::string> TNodeTagManager::GetDataCentersToPopulate(
    const std::string& nodeTagFilter,
    const THashMap<std::string, THashSet<std::string>>& perDataCenterAliveNodes,
    const TPerDataCenterSpareNodesInfo& spareNodesInfo)
{
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& targetConfig = bundleInfo->TargetConfig;
    const auto& zoneInfo = GetOrCrash(Input_.Zones, bundleInfo->Zone);
    const auto perNodeSlotCount = GetBundleEffectiveCpuLimits(BundleName_, bundleInfo, Input_)
        ->WriteThreadPoolSize.value_or(DefaultWriteThreadPoolSize);

    int activeDataCenterCount = std::ssize(zoneInfo->DataCenters) - zoneInfo->RedundantDataCenterCount;
    YT_VERIFY(activeDataCenterCount > 0);

    int perDataCenterSlotCount = DivCeil<int>(
        std::ssize(bundleInfo->TabletCellIds) * bundleInfo->Options->PeerCount,
        activeDataCenterCount);
    int requiredPerDataCenterNodeCount = DivCeil(perDataCenterSlotCount, perNodeSlotCount);

    std::vector<TDataCenterOrder> dataCentersOrder;
    dataCentersOrder.reserve(std::ssize(zoneInfo->DataCenters));

    for (const auto& [dataCenter, dataCenterInfo] : zoneInfo->DataCenters) {
        int availableNodeCount = GetAvailableAliveTabletNodeCount(
            dataCenter,
            perDataCenterAliveNodes,
            spareNodesInfo);

        int assignedNodeCount = GetAssignedTabletNodeCount(
            nodeTagFilter,
            dataCenter,
            perDataCenterAliveNodes,
            spareNodesInfo,
            Input_.TabletNodes);

        int assignedTabletCellCount = GetAssignedTabletCellCount(
            dataCenter,
            perDataCenterAliveNodes,
            Input_.TabletNodes);
        bool perBundleForbidden = targetConfig->ForbiddenDataCenters.count(dataCenter) != 0;

        dataCentersOrder.push_back({
            .Unfeasible = availableNodeCount < requiredPerDataCenterNodeCount,
            .Forbidden = dataCenterInfo->Forbidden || perBundleForbidden,
            .AssignedTabletCellCount = -1 * assignedTabletCellCount,
            .RequiredNodeAssignmentCount = requiredPerDataCenterNodeCount - assignedNodeCount,
            .DataCenter = dataCenter,
        });

        const auto& status = dataCentersOrder.back();

        YT_LOG_DEBUG(
            "Bundle data center status "
            "(DataCenter: %v, Unfeasible: %v, Forbidden: %v, AssignedTabletCellCount: %v,"
            " PerDataCenterSlotCount: %v, RequiredPerDataCenterNodeCount: %v,"
            " RequiredNodeAssignmentCount: %v, AvailableNodeCount: %v, RequiredNodeCount: %v)",
            dataCenter,
            status.Unfeasible,
            status.Forbidden,
            status.AssignedTabletCellCount,
            perDataCenterSlotCount,
            requiredPerDataCenterNodeCount,
            status.RequiredNodeAssignmentCount,
            availableNodeCount,
            requiredPerDataCenterNodeCount);
    }

    std::sort(dataCentersOrder.begin(), dataCentersOrder.end());
    dataCentersOrder.resize(activeDataCenterCount);

    THashSet<std::string> result;
    for (const auto& item : dataCentersOrder) {
        result.insert(item.DataCenter);
    }

    YT_LOG_DEBUG("Bundle data center preference (DataCenters: %v)",
        result);

    return result;
}

// Returns the number of tablet cell peers that either
//   * do not have assigned node
//   * are assigned to a not decommissioned node
// In other way, it is the number of peers minus number of peers on decommissioned nodes,
// which is generally (if peer_count = 1) equal to the number of tablet cells but may differ
// in some corner-case scenarios.
int GetRequiredSlotCount(const TBundleInfoPtr& bundleInfo, const TSchedulerInputState& input)
{
    int requiredSlotCount = 0;
    for (const auto& cellId : bundleInfo->TabletCellIds) {
        auto it = input.TabletCells.find(cellId);
        if (it != input.TabletCells.end()) {
            for (const auto& peer : it->second->Peers) {
                auto nodeIt = input.TabletNodes.find(peer->Address);
                if (nodeIt == input.TabletNodes.end() || !nodeIt->second->Decommissioned) {
                    ++requiredSlotCount;
                }
            }
        }
    }
    return requiredSlotCount;
}

void TNodeTagManager::SetNodeTags()
{
    const auto& bundleNodes = GetOrCrash(Input_.BundleNodes, BundleName_);
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& zoneName = bundleInfo->Zone;
    const auto& targetConfig = bundleInfo->TargetConfig;
    const auto& bundleState = Mutations_->ChangedStates[BundleName_];

    auto& perDataCenterSpareNodes = SpareNodeAllocator_->SpareInstances[zoneName];

    auto perDataCenterAliveNodes = GetAliveNodes(
        BundleName_,
        bundleNodes,
        Input_,
        bundleState,
        EGracePeriodBehaviour::Immediately);

    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    const auto& zoneInfo = GetOrCrash(Input_.Zones, bundleInfo->Zone);

    if (targetConfig->EnableDrillsMode || GetDrillsNodeTagFilter(bundleInfo, BundleName_) == nodeTagFilter) {
        YT_LOG_WARNING(
            "Bundle has drills mode enabled. To disable drills mode "
            "set bundle attribute @bundle_controller_target_config/enable_drills_mode=%%false");

        Mutations_->AlertsToFire.push_back({
            .Id = "bundle_has_drills_mode_enabled",
            .BundleName = BundleName_,
            .Description = Format("Bundle %Qv has drills mode enabled",
                BundleName_),
        });
        return;
    }

    if (nodeTagFilter.empty()) {
        YT_LOG_WARNING("Bundle does not have \"node_tag_filter\" attribute");

        Mutations_->AlertsToFire.push_back({
            .Id = "bundle_with_no_tag_filter",
            .BundleName = BundleName_,
            .Description = Format("Bundle %Qv does not have \"node_tag_filter\" attribute",
                BundleName_),
        });
        return;
    }

    auto dataCentersToPopulate = GetDataCentersToPopulate(
        nodeTagFilter,
        perDataCenterAliveNodes,
        perDataCenterSpareNodes);

    if (!targetConfig->ForbiddenDataCenters.empty()) {
        Mutations_->AlertsToFire.push_back({
            .Id = "bundle_has_forbidden_dc",
            .BundleName = BundleName_,
            .Description = Format("Data centers %v are forbidden for bundle %Qv",
                targetConfig->ForbiddenDataCenters,
                BundleName_)});
    }

    // Create and process bundle node assignments/releasements.
    for (const auto& [dataCenterName, _] : zoneInfo->DataCenters) {
        const auto& aliveNodes = perDataCenterAliveNodes[dataCenterName];

        if (dataCentersToPopulate.contains(dataCenterName)) {
            TryCreateBundleNodesAssignment(aliveNodes);
        } else {
            TryCreateBundleNodesReleasement(aliveNodes);
        }
    }

    if (Input_.DynamicConfig->RemoveTagsFromOfflineNodes) {
        for (const auto& [dataCenterName, nodeAddresses] : bundleNodes) {
            THashSet<std::string> offlineNodes;

            for (const auto& address : nodeAddresses) {
                const auto& info = GetOrCrash(Input_.TabletNodes, address);
                if (!info->IsOnline()) {
                    offlineNodes.insert(address);
                }
            }

            RemoveTagsFromNodes(offlineNodes);
        }
    }

    ProcessNodeAssignments(&bundleState->BundleNodeAssignments);
    ProcessNodeReleasements(
        /*leaveDecommissioned*/ false,
        &bundleState->BundleNodeReleasements);

    int requiredSlotCount = GetRequiredSlotCount(bundleInfo, Input_);

    for (const auto& [dataCenterName, _] : zoneInfo->DataCenters) {
        const auto& aliveNodes = perDataCenterAliveNodes[dataCenterName];
        int perNodeSlotCount = GetBundleEffectiveCpuLimits(BundleName_, bundleInfo, Input_)
            ->WriteThreadPoolSize.value_or(DefaultWriteThreadPoolSize);
        const auto& spareNodes = perDataCenterSpareNodes[dataCenterName];

        int requiredDataCenterSlotCount = dataCentersToPopulate.contains(dataCenterName)
            ? DivCeil<int>(requiredSlotCount, std::ssize(dataCentersToPopulate))
            : 0;

        // Number of nodes that have bundle tag and are not decommissioned.
        int readyBundleNodeCount = GetReadyNodeCount(
            bundleInfo->NodeTagFilter,
            aliveNodes,
            Input_.TabletNodes);
        // Number of slots on ready bundles nodes.
        int actualSlotCount = perNodeSlotCount * readyBundleNodeCount;
        // Number of slots on spare nodes assigned to bundle.
        int bundleSpareNodeCount = ssize(GetOrDefaultReference(spareNodes.UsedByBundle, BundleName_));
        int usedSpareSlotCount = bundleSpareNodeCount * perNodeSlotCount;

        int slotsToRelease =
            (usedSpareSlotCount + actualSlotCount) - requiredDataCenterSlotCount;
        // We account decommissioned nodes here as well because they will be added to bundle soon.
        int slotsToAssign =
            requiredDataCenterSlotCount - (usedSpareSlotCount + std::ssize(aliveNodes) * perNodeSlotCount);

        YT_LOG_DEBUG("Checking tablet cell slots for bundle "
            "(DataCenter: %v, "
            "RequiredSlotCount: %v, "
            "BundleSlotCount: %v, "
            "UsedSpareSlotCount: %v, "
            "SlotsToRelease: %v, "
            "SlotsToAssign: %v, "
            "RequiredDataCenterSlotCount: %v)",
            dataCenterName,
            requiredSlotCount,
            actualSlotCount,
            usedSpareSlotCount,
            slotsToRelease,
            slotsToAssign,
            requiredDataCenterSlotCount);

        if (!Input_.Config->EnableSpareNodeAssignment) {
            if (slotsToRelease > 0 || slotsToAssign > 0) {
                YT_LOG_DEBUG("Spare node assignment/releasement is disabled "
                    "(DataCenter: %v, SlotsToRelease: %v, SlotsToAssign: %v)",
                    dataCenterName,
                    slotsToRelease,
                    slotsToAssign);
            }

            continue;
        }

        if (slotsToRelease > 0) {
            YT_LOG_DEBUG("Creating spare nodes releasements (DataCenter: %v, SlotsToRelease: %v)",
                dataCenterName,
                slotsToRelease);
            TryCreateSpareNodeReleasements(slotsToRelease, &spareNodes);
        } else if (slotsToAssign > 0) {
            YT_LOG_DEBUG("Creating spare nodes assignment (DataCenter: %v, SlotsToAssign: %v)",
                dataCenterName,
                slotsToAssign);
            TryCreateSpareNodeAssignments(
                dataCenterName,
                slotsToAssign,
                SpareNodeAllocator_);
        }
    }

    ProcessNodeAssignments(&bundleState->SpareNodeAssignments);
    ProcessNodeReleasements(
        /*leaveDecommissioned*/ Input_.Config->DecommissionReleasedNodes,
        &bundleState->SpareNodeReleasements);
}

////////////////////////////////////////////////////////////////////////////////

TPerDataCenterSpareNodesInfo GetSpareNodesInfo(
    const std::string& zoneName,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto zoneIt = input.Zones.find(zoneName);
    if (zoneIt == input.Zones.end()) {
        return {};
    }

    auto spareBundle = GetSpareBundleName(zoneIt->second);
    auto spareNodesIt = input.BundleNodes.find(spareBundle);
    if (spareNodesIt == input.BundleNodes.end()) {
        return {};
    }

    THashMap<std::string, std::string> nodeTagFilterToBundleName;
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        nodeTagFilterToBundleName[bundleInfo->NodeTagFilter] = bundleName;
    }

    THashMap<std::string, std::string> operationsToBundle;
    for (const auto& [bundleName, bundleState] : input.BundleStates) {
        for (const auto& [spareNode, _] : bundleState->SpareNodeAssignments) {
            operationsToBundle[spareNode] = bundleName;
        }

        for (const auto& [spareNode, _] : bundleState->SpareNodeReleasements) {
            operationsToBundle[spareNode] = bundleName;
        }
    }

    const auto& spareBundleState = GetOrInsert(mutations->ChangedStates, spareBundle, New<TBundleControllerState>);

    auto zoneAliveNodes = GetAliveNodes(
        spareBundle,
        spareNodesIt->second,
        input,
        spareBundleState,
        EGracePeriodBehaviour::Immediately);

    TPerDataCenterSpareNodesInfo result;

    for (const auto& [dataCenterName, aliveNodes] : zoneAliveNodes) {
        auto& spareNodes = result[dataCenterName];

        for (const auto& spareNodeName : aliveNodes) {
            auto nodeInfo = GetOrCrash(input.TabletNodes, spareNodeName);

            bool hasMaintenanceRequests = !nodeInfo->CmsMaintenanceRequests.empty();

            auto assignedBundlesNames = GetBundlesByTags(nodeInfo, nodeTagFilterToBundleName);
            if (assignedBundlesNames.empty() && !operationsToBundle.contains(spareNodeName)) {
                if (hasMaintenanceRequests) {
                    spareNodes.ScheduledForMaintenance.push_back(spareNodeName);
                } else {
                    spareNodes.FreeNodes.push_back(spareNodeName);
                }
                continue;
            }

            if (std::ssize(assignedBundlesNames) > 1) {
                YT_LOG_WARNING("Spare node is assigned to multiple bundles (Node: %v, Bundles: %v)",
                    spareNodeName,
                    assignedBundlesNames);

                mutations->AlertsToFire.push_back({
                    .Id = "node_with_multiple_node_tag_filters",
                    .Description = Format("Spare node %v is assigned to multiple bundles: %v.",
                        spareNodeName,
                        assignedBundlesNames),
                });
                continue;
            }

            const auto bundleName = !assignedBundlesNames.empty()
                ? assignedBundlesNames.back()
                : GetOrCrash(operationsToBundle, spareNodeName);

            if (nodeInfo->Decommissioned && !operationsToBundle.contains(spareNodeName)) {
                YT_LOG_WARNING("Spare node is externally decommissioned (Node: %v, AssignedToBundle: %v)",
                    spareNodeName,
                    bundleName);

                spareNodes.ExternallyDecommissioned.push_back(spareNodeName);
                continue;
            }

            const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
            if (!bundleInfo->EnableBundleController) {
                YT_LOG_WARNING("Spare node is occupied by unmanaged bundle (Node: %v, Bundles: %v)",
                    spareNodeName,
                    bundleName);

                spareNodes.UsedByBundle[bundleName].push_back(spareNodeName);
                continue;
            }

            auto bundleState = GetOrCrash(mutations->ChangedStates, bundleName);

            if (bundleState->SpareNodeReleasements.contains(spareNodeName)) {
                spareNodes.ReleasingByBundle[bundleName].push_back(spareNodeName);
            } else {
                spareNodes.UsedByBundle[bundleName].push_back(spareNodeName);
            }
        }
    }

    return result;
}

void InitializeZoneToSpareNodes(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [zoneName, _] : input.Zones) {
        input.ZoneToSpareNodes[zoneName] = GetSpareNodesInfo(zoneName, input, mutations);

        for (const auto& [dataCenterName, spareInfo] : input.ZoneToSpareNodes[zoneName]) {
            auto inUseSpareNodes = std::ssize(spareInfo.UsedByBundle) + std::ssize(spareInfo.ReleasingByBundle);
            if (std::ssize(spareInfo.FreeNodes) == 0 && inUseSpareNodes > 0) {
                YT_LOG_WARNING("No free spare nodes available (Zone: %v, DataCenter: %v)",
                    zoneName,
                    dataCenterName);

                mutations->AlertsToFire.push_back({
                    .Id = "no_free_spare_nodes",
                    .Description = Format("No free spare node available in zone: %v in data center: %v.",
                        zoneName,
                        dataCenterName),
                });
            }

            if (std::ssize(spareInfo.ExternallyDecommissioned) > 0) {
                mutations->AlertsToFire.push_back({
                    .Id = "externally_decommissioned_spare_nodes",
                    .Description = Format("Externally decommissioned spare nodes in zone: %v in data center: %v.",
                        zoneName,
                        dataCenterName),
                });
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageNodeTags(
    TSchedulerInputState& input,
    TSpareInstanceAllocator<TSpareNodesInfo>& spareNodesAllocator,
    TSchedulerMutations* mutations,
    const INodeTrackerPtr& nodeTracker)
{
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController ||
            !bundleInfo->EnableNodeTagFilterManagement)
        {
            continue;
        }

        if (!input.Zones.contains(bundleInfo->Zone)) {
            continue;
        }

        TNodeTagManager nodeTagManager(
            bundleName,
            input,
            &spareNodesAllocator,
            mutations,
            nodeTracker);

        nodeTagManager.SetNodeTags();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
