#include "fair_share_tree_snapshot.h"

#include "fair_share_tree.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeSnapshot::TFairShareTreeSnapshot(
    TTreeSnapshotId id,
    TSchedulerRootElementPtr rootElement,
    TNonOwningOperationElementMap enabledOperationIdToElement,
    TNonOwningOperationElementMap disabledOperationIdToElement,
    TNonOwningPoolElementMap poolNameToElement,
    TFairShareStrategyTreeConfigPtr treeConfig,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    const TJobResources& resourceUsage,
    const TJobResources& resourceLimits,
    int nodeCount,
    TFairShareTreeSchedulingSnapshotPtr schedulingSnapshot)
    : Id_(id)
    , RootElement_(std::move(rootElement))
    , EnabledOperationMap_(std::move(enabledOperationIdToElement))
    , DisabledOperationMap_(std::move(disabledOperationIdToElement))
    , PoolMap_(std::move(poolNameToElement))
    , TreeConfig_(std::move(treeConfig))
    , ControllerConfig_(std::move(controllerConfig))
    , ResourceUsage_(resourceUsage)
    , ResourceLimits_(resourceLimits)
    , NodeCount_(nodeCount)
    , SchedulingSnapshot_(std::move(schedulingSnapshot))
{ }

TSchedulerPoolElement* TFairShareTreeSnapshot::FindPool(const TString& poolName) const
{
    auto it = PoolMap_.find(poolName);
    return it != PoolMap_.end() ? it->second : nullptr;
}

TSchedulerOperationElement* TFairShareTreeSnapshot::FindEnabledOperationElement(TOperationId operationId) const
{
    auto it = EnabledOperationMap_.find(operationId);
    return it != EnabledOperationMap_.end() ? it->second : nullptr;
}

TSchedulerOperationElement* TFairShareTreeSnapshot::FindDisabledOperationElement(TOperationId operationId) const
{
    auto it = DisabledOperationMap_.find(operationId);
    return it != DisabledOperationMap_.end() ? it->second : nullptr;
}

bool TFairShareTreeSnapshot::IsElementEnabled(const TSchedulerElement* element) const
{
    if (element->IsOperation()) {
        const auto* operationElement = static_cast<const TSchedulerOperationElement*>(element);
        return static_cast<bool>(FindEnabledOperationElement(operationElement->GetOperationId()));
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeSetSnapshot::TFairShareTreeSetSnapshot(std::vector<IFairShareTreePtr> trees, int topologyVersion)
    : Trees_(std::move(trees))
    , TopologyVersion_(topologyVersion)
{ }

THashMap<TString, IFairShareTreePtr> TFairShareTreeSetSnapshot::BuildIdToTreeMapping() const
{
    THashMap<TString, IFairShareTreePtr> idToTree;
    idToTree.reserve(Trees_.size());
    for (const auto& tree : Trees_) {
        idToTree.emplace(tree->GetId(), tree);
    }
    return idToTree;
}

////////////////////////////////////////////////////////////////////////////////

TResourceUsageSnapshotPtr BuildResourceUsageSnapshot(const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    YT_VERIFY(treeSnapshot);

    auto operationResourceUsageMap = THashMap<TOperationId, TJobResources>(treeSnapshot->EnabledOperationMap().size());
    auto poolResourceUsageMap = THashMap<TString, TJobResources>(treeSnapshot->PoolMap().size());
    auto aliveOperationIds = THashSet<TOperationId>(treeSnapshot->EnabledOperationMap().size());

    for (const auto& [operationId, element] : treeSnapshot->EnabledOperationMap()) {
        if (!element->IsAlive()) {
            continue;
        }

        aliveOperationIds.insert(operationId);
        auto resourceUsage = element->GetInstantResourceUsage();
        operationResourceUsageMap[operationId] = resourceUsage;

        const TSchedulerCompositeElement* parentPool = element->GetParent();
        while (parentPool) {
            poolResourceUsageMap[parentPool->GetId()] += resourceUsage;
            parentPool = parentPool->GetParent();
        }
    }

    auto resourceUsageSnapshot = New<TResourceUsageSnapshot>();
    resourceUsageSnapshot->BuildTime = GetCpuInstant();
    resourceUsageSnapshot->OperationIdToResourceUsage = std::move(operationResourceUsageMap);
    resourceUsageSnapshot->PoolToResourceUsage = std::move(poolResourceUsageMap);
    resourceUsageSnapshot->AliveOperationIds = std::move(aliveOperationIds);

    return resourceUsageSnapshot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
