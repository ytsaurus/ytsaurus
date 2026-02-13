#include "allocator_adapter.h"

#include "config.h"
#include "cypress_bindings.h"
#include "helpers.h"
#include "input_state.h"
#include "mutations.h"

namespace NYT::NCellBalancer {

using namespace NBundleControllerClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TNodeRemoveOrder
{
    bool MaintenanceIsNotRequested = true;
    bool HasUpdatedResources = true;
    int UsedSlotCount = 0;
    std::string NodeName;

    auto AsTuple() const
    {
        return std::tie(MaintenanceIsNotRequested, HasUpdatedResources, UsedSlotCount, NodeName);
    }

    bool operator<(const TNodeRemoveOrder& other) const
    {
        return AsTuple() < other.AsTuple();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeAllocatorAdapter
    : public IAllocatorAdapter
{
public:
    using TSpareInstanceInfo = TSpareNodesInfo;

    TTabletNodeAllocatorAdapter(
        TBundleControllerStatePtr state,
        const TDataCenterToInstanceMap& bundleNodes,
        const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes)
        : State_(std::move(state))
        , BundleNodes_(bundleNodes)
        , AliveBundleNodes_(aliveBundleNodes)
    { }

    bool IsNewAllocationAllowed(
        const TBundleInfoPtr& /*bundleInfo*/,
        const std::string& dataCenterName,
        const TSchedulerInputState& input)
    {
        if (GetInProgressAssignmentCount(dataCenterName, input) > 0) {
            // Do not mix node tag operations with new node allocations.
            return false;
        }

        return true;
    }

    int GetInProgressAssignmentCount(const std::string& dataCenterName, const TSchedulerInputState& input)
    {
        auto dataCenterPredicate = [&] (const auto& pair) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, pair.first);
            return nodeInfo->BundleControllerAnnotations->DataCenter == dataCenterName;
        };

        const auto& assignments = State_->BundleNodeAssignments;
        const auto& releasements = State_->BundleNodeReleasements;

        return std::count_if(assignments.begin(), assignments.end(), dataCenterPredicate) +
            std::count_if(releasements.begin(), releasements.end(), dataCenterPredicate);
    }

    bool IsDataCenterDisrupted(const TDataCenterDisruptedState& dcState)
    {
        return dcState.IsNodesDisrupted();
    }

    bool IsNewDeallocationAllowed(
        const TBundleInfoPtr& bundleInfo,
        const std::string& dataCenterName,
        const TSchedulerInputState& input)
    {
        bool deallocationsInDataCenter = std::any_of(
            State_->NodeDeallocations.begin(),
            State_->NodeDeallocations.end(),
            [&] (const auto& record) {
                return record.second->DataCenter.value_or(DefaultDataCenterName) == dataCenterName;
            });

        if (!State_->NodeAllocations.empty() ||
            deallocationsInDataCenter ||
            !State_->RemovingCells.empty() ||
            GetInProgressAssignmentCount(dataCenterName, input) > 0)
        {
            // It is better not to mix allocation and deallocation requests.
            return false;
        }

        const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);

        if (bundleInfo->EnableTabletCellManagement) {
            if (GetTargetCellCount(bundleInfo, zoneInfo) != std::ssize(bundleInfo->TabletCellIds)) {
                // Wait for tablet cell management to complete.
                return false;
            }
        }

        if (bundleInfo->EnableNodeTagFilterManagement) {
            // Check that all alive instances have appropriate node_tag_filter and slots count
            auto expectedSlotCount = bundleInfo->TargetConfig->CpuLimits->WriteThreadPoolSize;

            std::vector<std::string> notReadyNodes;
            const auto aliveDataCenterNodes = GetAliveInstances(dataCenterName);

            for (const auto& nodeName : aliveDataCenterNodes) {
                const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
                if (nodeInfo->UserTags.count(bundleInfo->NodeTagFilter) == 0 ||
                    std::ssize(nodeInfo->TabletSlots) != expectedSlotCount)
                {
                    YT_LOG_DEBUG("Node is not ready (NodeName: %v, "
                        "ExpectedSlotCount: %v, NodeTagFilter: %v, SlotCount: %v, UserTags: %v)",
                        nodeName, expectedSlotCount, bundleInfo->NodeTagFilter, std::ssize(nodeInfo->TabletSlots), nodeInfo->UserTags);

                    notReadyNodes.push_back(nodeName);
                }
            }

            if (!notReadyNodes.empty() && std::ssize(notReadyNodes) != std::ssize(aliveDataCenterNodes)) {
                // Wait while all alive nodes have updated settings.

                YT_LOG_INFO("Skipping nodes deallocation because nodes are node ready (DataCenter: %v)",
                    dataCenterName);
                return false;
            }
        }

        return true;
    }

    bool IsInstanceCountLimitReached(
        const std::string& zoneName,
        const std::string& dataCenterName,
        const TZoneInfoPtr& zoneInfo,
        const TSchedulerInputState& input) const
    {
        auto zoneIt = input.ZoneNodes.find(zoneName);
        if (zoneIt == input.ZoneNodes.end()) {
            // No allocated tablet nodes for this zone
            return false;
        }

        auto dataCenterIt = zoneIt->second.PerDataCenter.find(dataCenterName);
        if (dataCenterIt == zoneIt->second.PerDataCenter.end()) {
            // No allocated tablet nodes for this dc
            return false;
        }

        int currentDataCenterNodeCount = std::ssize(dataCenterIt->second);
        int datacenterMaxNodeCount = zoneInfo->MaxTabletNodeCount / std::ssize(zoneInfo->DataCenters);

        if (currentDataCenterNodeCount >= datacenterMaxNodeCount) {
            YT_LOG_WARNING("Max nodes count limit reached"
                " (Zone: %v, DataCenter: %v, CurrentDataCenterNodeCount: %v, ZoneMaxTabletNodeCount: %v, DatacenterMaxTabletNodeCount: %v)",
                zoneName,
                dataCenterName,
                currentDataCenterNodeCount,
                zoneInfo->MaxTabletNodeCount,
                datacenterMaxNodeCount);
            return true;
        }
        return false;
    }

    int GetTargetInstanceCount(const TBundleInfoPtr& bundleInfo, const TZoneInfoPtr& zoneInfo) const
    {
        return GetTargetDataCenterInstanceCount(bundleInfo->TargetConfig->TabletNodeCount, zoneInfo);
    }

    int GetInstanceRole() const
    {
        return YTRoleTypeTabNode;
    }

    const TInstanceResourcesPtr& GetResourceGuarantee(const TBundleInfoPtr& bundleInfo) const
    {
        return bundleInfo->TargetConfig->TabletNodeResourceGuarantee;
    }

    const std::optional<std::string> GetHostTagFilter(const TBundleInfoPtr& bundleInfo, const TSchedulerInputState& input) const
    {
        auto resources = bundleInfo->TargetConfig->TabletNodeResourceGuarantee;
        const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);

        for (const auto& [name, instanceSize] : zoneInfo->TabletNodeSizes) {
            if (*instanceSize->ResourceGuarantee == *resources) {
                return instanceSize->HostTagFilter;
            }
        }

        return {};
    }

    const std::string& GetInstanceType()
    {
        static const std::string TabletNode = "tab";
        return TabletNode;
    }

    const std::string& GetHumanReadableInstanceType() const
    {
        static const std::string TabletNode = "tablet node";
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

    TInstanceInfoBasePtr FindInstanceInfo(const std::string& instanceName, const TSchedulerInputState& input)
    {
        return GetOrDefault(input.TabletNodes, instanceName, nullptr);
    }

    TInstanceInfoBasePtr GetInstanceInfo(const std::string& instanceName, const TSchedulerInputState& input)
    {
        return GetOrCrash(input.TabletNodes, instanceName);
    }

    void SetInstanceAnnotations(const std::string& instanceName, TBundleControllerInstanceAnnotationsPtr bundleControllerAnnotations, TSchedulerMutations* mutations)
    {
        mutations->ChangedNodeAnnotations[instanceName] = mutations->WrapMutation(std::move(bundleControllerAnnotations));
    }

    bool IsInstanceReadyToBeDeallocated(
        const std::string& instanceName,
        const std::string& deallocationId,
        TDuration deallocationAge,
        const std::string& bundleName,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) const
    {
        auto nodeIt = input.TabletNodes.find(instanceName);
        if (nodeIt == input.TabletNodes.end()) {
            YT_LOG_ERROR("Cannot find node from deallocation request state (DeallocationId: %v, Node: %v, BundleName: %v)",
                deallocationId,
                instanceName,
                bundleName);
            return false;
        }

        if (!input.Config->DecommissionReleasedNodes) {
            YT_LOG_INFO("Skipping node decommissioning due to configuration (DeallocationId: %v, Node: %v)",
                deallocationId,
                instanceName);
            return true;
        }

        const auto& nodeInfo = nodeIt->second;

        if (!nodeInfo->Decommissioned) {
            YT_LOG_INFO("Decommissioning node before deallocation (DeallocationId: %v, Node: %v)",
                deallocationId,
                instanceName);
            mutations->ChangedDecommissionedFlag[instanceName] = mutations->WrapMutation(true);
            return false;
        }

        if (GetUsedSlotCount(nodeInfo) == 0) {
            YT_LOG_INFO("All tablet slots are empty, node is ready for deallocation (DeallocationId: %v, Node: %v)",
                deallocationId,
                instanceName);
            return true;
        }

        if (deallocationAge > input.Config->DecommissionedNodeDrainTimeout) {
            YT_LOG_INFO("Tablet cell migration taking too long, node deallocation allowed without full drain "
                "(DeallocationId: %v, Node: %v, DeallocationAge: %v, Timeout: %v)",
                deallocationId,
                instanceName,
                deallocationAge,
                input.Config->DecommissionedNodeDrainTimeout);

            // Long deallocations usually are caused by hardware or connectivity problems.
            // It is better to remove cells from the broken node forcefully.
            auto bundleInfo = GetOrCrash(input.Bundles, bundleName);
            if (nodeInfo->UserTags.contains(bundleInfo->NodeTagFilter)) {
                YT_LOG_INFO("Tablet cell migration taking too long, will ban node "
                    "(DeallocationId: %v, Node: %v)",
                    deallocationId,
                    instanceName);

                mutations->ChangedBannedFlag[instanceName] = mutations->WrapMutation(true);
            }
        }

        // Wait tablet cells to migrate.
        return GetUsedSlotCount(nodeInfo) == 0;
    }

    std::string GetNannyService(const TDataCenterInfoPtr& dataCenterInfo) const
    {
        return dataCenterInfo->TabletNodeNannyService;
    }

    bool EnsureAllocatedInstanceTagsSet(
        const std::string& nodeName,
        const std::string& bundleName,
        const std::string& dataCenterName,
        const TAllocationRequestPtr& allocationInfo,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) const
    {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        if (nodeInfo->State != InstanceStateOnline) {
            return false;
        }

        if (nodeInfo->Decommissioned) {
            YT_LOG_DEBUG("Removing decommissioned flag from node (BundleName: %v, Node: %v)",
                bundleName,
                nodeName);
            mutations->ChangedDecommissionedFlag[nodeName] = mutations->WrapMutation(false);
            return false;
        }

        if (!nodeInfo->UserTags.empty()) {
            YT_LOG_INFO("Removing node user tags (BundleName: %v, Node: %v)",
                bundleName,
                nodeName);
            mutations->ChangedNodeUserTags[nodeName] = mutations->WrapMutation<TSchedulerMutations::TUserTags>({});
            return false;
        }

        const auto& bundleControllerAnnotations = nodeInfo->BundleControllerAnnotations;

        if (auto changed = GetBundleControllerInstanceAnnotationsToSet(bundleName, dataCenterName, allocationInfo, bundleControllerAnnotations)) {
            YT_LOG_DEBUG("Setting node annotations (BundleName: %v, NodeName: %v, Annotations: %v)",
                bundleName,
                nodeName,
                ConvertToYsonString(changed, EYsonFormat::Text));
            mutations->ChangedNodeAnnotations[nodeName] = mutations->WrapMutation(changed);
            return false;
        }

        if (bundleControllerAnnotations->AllocatedForBundle != bundleName) {
            YT_LOG_WARNING("Inconsistent allocation state (AnnotationsBundleName: %v, ActualBundleName: %v, NodeName: %v)",
                bundleControllerAnnotations->AllocatedForBundle,
                bundleName,
                nodeName);

            mutations->AlertsToFire.push_back({
                .Id = "inconsistent_allocation_state",
                .BundleName = bundleName,
                .Description = Format("Inconsistent allocation state: Node annotation bundle name %v, actual bundle name %v.",
                    bundleControllerAnnotations->AllocatedForBundle,
                    bundleName)
            });

            return false;
        }

        if (GetAliveInstances(dataCenterName).count(nodeName) == 0) {
            return false;
        }

        return true;
    }

    bool EnsureDeallocatedInstanceTagsSet(
        const std::string& bundleName,
        const std::string& nodeName,
        const std::string& strategy,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        YT_VERIFY(!strategy.empty());

        auto instanceInfoBase = GetInstanceInfo(nodeName, input);
        auto* instanceInfo = dynamic_cast<TTabletNodeInfo*>(instanceInfoBase.Get());
        const auto& bundleControllerAnnotations = instanceInfo->BundleControllerAnnotations;
        if (strategy != DeallocationStrategyReturnToSpareBundle && (!bundleControllerAnnotations->AllocatedForBundle.empty() || bundleControllerAnnotations->Allocated)) {
            auto newAnnotations = New<TBundleControllerInstanceAnnotations>();
            newAnnotations->DeallocatedAt = TInstant::Now();
            newAnnotations->DeallocationStrategy = strategy;
            mutations->ChangedNodeAnnotations[nodeName] = mutations->WrapMutation(newAnnotations);
            return false;
        }

        // Prevent node from applying wrong node config and from setting
        // wrong node tag filters.
        if (strategy == DeallocationStrategyReturnToSpareBundle && bundleControllerAnnotations->AllocatedForBundle == bundleName) {
            auto newAnnotations = NYTree::CloneYsonStruct(bundleControllerAnnotations);
            newAnnotations->AllocatedForBundle = "";
            mutations->ChangedNodeAnnotations[nodeName] = mutations->WrapMutation(newAnnotations);
            return false;
        }

        if (!instanceInfo->UserTags.empty()) {
            mutations->ChangedNodeUserTags[nodeName] = {};
            return false;
        }

        if (strategy == DeallocationStrategyReturnToBB) {
            if (!instanceInfo->EnableBundleBalancer || *instanceInfo->EnableBundleBalancer == false) {
                YT_LOG_DEBUG("Returning node to BundleBalancer (NodeName: %v)",
                    nodeName);

                mutations->ChangedEnableBundleBalancerFlag[nodeName] = mutations->WrapMutation(true);
            }
        }
        return true;
    }

    void SetDefaultSpareAttributes(const std::string& nodeName, TSchedulerMutations* mutations) const
    {
        mutations->ChangedNodeUserTags[nodeName] = {};
    }

    const THashSet<std::string>& GetAliveInstances(const std::string& dataCenterName) const
    {
        const static THashSet<std::string> Dummy;

        auto it = AliveBundleNodes_.find(dataCenterName);
        if (it != AliveBundleNodes_.end()) {
            return it->second;
        }

        return Dummy;
    }

    const std::vector<std::string>& GetInstances(const std::string& dataCenterName) const
    {
        const static std::vector<std::string> Dummy;

        auto it = BundleNodes_.find(dataCenterName);
        if (it != BundleNodes_.end()) {
            return it->second;
        }

        return Dummy;
    }

    std::vector<std::string> PickInstancesToDeallocate(
        int nodeCountToRemove,
        const std::string& dataCenterName,
        const TBundleInfoPtr& bundleInfo,
        const TSchedulerInputState& input) const
    {
        const auto& aliveDataCenterNodes = GetAliveInstances(dataCenterName);

        std::vector<TNodeRemoveOrder> nodesOrder;

        nodesOrder.reserve(aliveDataCenterNodes.size());

        const auto& targetResource = GetResourceGuarantee(bundleInfo);

        for (auto nodeName : aliveDataCenterNodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            const auto& instanceResource = nodeInfo->BundleControllerAnnotations->Resource;

            nodesOrder.push_back(TNodeRemoveOrder{
                .MaintenanceIsNotRequested = nodeInfo->CmsMaintenanceRequests.empty(),
                .HasUpdatedResources = (*targetResource == *instanceResource),
                .UsedSlotCount = GetUsedSlotCount(nodeInfo),
                .NodeName = nodeName,
            });
        }

        auto endIt = nodesOrder.end();
        if (std::ssize(nodesOrder) > nodeCountToRemove) {
            endIt = nodesOrder.begin() + nodeCountToRemove;
            std::nth_element(nodesOrder.begin(), endIt, nodesOrder.end());
        }

        std::vector<std::string> result;
        result.reserve(std::distance(nodesOrder.begin(), endIt));
        for (auto it = nodesOrder.begin(); it != endIt; ++it) {
            result.push_back(it->NodeName);
        }

        return result;
    }

private:
    TBundleControllerStatePtr State_;
    const TDataCenterToInstanceMap& BundleNodes_;
    const THashMap<std::string, THashSet<std::string>>& AliveBundleNodes_;
};


////////////////////////////////////////////////////////////////////////////////

IAllocatorAdapterPtr CreateTabletNodeAllocatorAdapter(
    TBundleControllerStatePtr state,
    const TDataCenterToInstanceMap& bundleNodes,
    const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes)
{
    return New<TTabletNodeAllocatorAdapter>(
        std::move(state),
        bundleNodes,
        aliveBundleNodes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
