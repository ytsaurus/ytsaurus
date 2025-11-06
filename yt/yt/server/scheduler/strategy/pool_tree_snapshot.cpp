#include "pool_tree_snapshot.h"

#include "pool_tree.h"

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSnapshot::TPoolTreeSnapshot(
    TTreeSnapshotId id,
    TPoolTreeRootElementPtr rootElement,
    TNonOwningOperationElementMap enabledOperationIdToElement,
    TNonOwningOperationElementMap disabledOperationIdToElement,
    TNonOwningPoolElementMap poolNameToElement,
    TStrategyTreeConfigPtr treeConfig,
    TStrategyOperationControllerConfigPtr controllerConfig,
    const TJobResources& resourceUsage,
    const TJobResources& resourceLimits,
    TNodeIdToAddress nodeAddresses,
    NPolicy::TPoolTreeSnapshotStatePtr schedulingPolicyState,
    TJobResourcesByTagFilter resourceLimitsByTagFilter)
    : Id_(id)
    , RootElement_(std::move(rootElement))
    , EnabledOperationMap_(std::move(enabledOperationIdToElement))
    , DisabledOperationMap_(std::move(disabledOperationIdToElement))
    , PoolMap_(std::move(poolNameToElement))
    , TreeConfig_(std::move(treeConfig))
    , ControllerConfig_(std::move(controllerConfig))
    , ResourceUsage_(resourceUsage)
    , ResourceLimits_(resourceLimits)
    , NodeAddresses_(std::move(nodeAddresses))
    , SchedulingPolicyState_(std::move(schedulingPolicyState))
    , ResourceLimitsByTagFilter_(std::move(resourceLimitsByTagFilter))
{ }

TPoolTreePoolElement* TPoolTreeSnapshot::FindPool(const TString& poolName) const
{
    auto it = PoolMap_.find(poolName);
    return it != PoolMap_.end() ? it->second : nullptr;
}

TPoolTreeOperationElement* TPoolTreeSnapshot::FindEnabledOperationElement(TOperationId operationId) const
{
    auto it = EnabledOperationMap_.find(operationId);
    return it != EnabledOperationMap_.end() ? it->second : nullptr;
}

TPoolTreeOperationElement* TPoolTreeSnapshot::FindDisabledOperationElement(TOperationId operationId) const
{
    auto it = DisabledOperationMap_.find(operationId);
    return it != DisabledOperationMap_.end() ? it->second : nullptr;
}

bool TPoolTreeSnapshot::IsElementEnabled(const TPoolTreeElement* element) const
{
    if (element->IsOperation()) {
        const auto* operationElement = static_cast<const TPoolTreeOperationElement*>(element);
        return static_cast<bool>(FindEnabledOperationElement(operationElement->GetOperationId()));
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSetSnapshot::TPoolTreeSetSnapshot(std::vector<IPoolTreePtr> trees, int topologyVersion)
    : Trees_(std::move(trees))
    , TopologyVersion_(topologyVersion)
{ }

THashMap<std::string, IPoolTreePtr> TPoolTreeSetSnapshot::BuildIdToTreeMapping() const
{
    THashMap<std::string, IPoolTreePtr> idToTree;
    idToTree.reserve(Trees_.size());
    for (const auto& tree : Trees_) {
        idToTree.emplace(tree->GetId(), tree);
    }
    return idToTree;
}

////////////////////////////////////////////////////////////////////////////////

TResourceUsageSnapshotPtr BuildResourceUsageSnapshot(const TPoolTreeSnapshotPtr& treeSnapshot)
{
    YT_VERIFY(treeSnapshot);

    auto operationResourceUsageMap = THashMap<TOperationId, TJobResources>(treeSnapshot->EnabledOperationMap().size());
    auto operationResourceUsageWithPrecommitMap = THashMap<TOperationId, TJobResources>(treeSnapshot->EnabledOperationMap().size());
    auto poolResourceUsageMap = THashMap<TString, TJobResources>(treeSnapshot->PoolMap().size());
    auto poolResourceUsageWithPrecommitMap = THashMap<TString, TJobResources>(treeSnapshot->PoolMap().size());
    auto aliveOperationIds = THashSet<TOperationId>(treeSnapshot->EnabledOperationMap().size());

    for (const auto& [operationId, element] : treeSnapshot->EnabledOperationMap()) {
        if (!element->IsAlive()) {
            continue;
        }

        aliveOperationIds.insert(operationId);

        auto resourceUsage = element->GetInstantResourceUsage();
        auto resourceUsageWithPrecommit = element->GetInstantResourceUsage(/*withPrecommit*/ true);

        operationResourceUsageMap[operationId] = resourceUsage;
        operationResourceUsageWithPrecommitMap[operationId] = resourceUsageWithPrecommit;

        const TPoolTreeCompositeElement* parentPool = element->GetParent();
        while (parentPool) {
            poolResourceUsageMap[parentPool->GetId()] += resourceUsage;
            poolResourceUsageWithPrecommitMap[parentPool->GetId()] += resourceUsageWithPrecommit;
            parentPool = parentPool->GetParent();
        }
    }

    auto resourceUsageSnapshot = New<TResourceUsageSnapshot>();
    resourceUsageSnapshot->BuildTime = GetCpuInstant();
    resourceUsageSnapshot->OperationIdToResourceUsage = std::move(operationResourceUsageMap);
    resourceUsageSnapshot->OperationIdToResourceUsageWithPrecommit = std::move(operationResourceUsageWithPrecommitMap);
    resourceUsageSnapshot->PoolToResourceUsage = std::move(poolResourceUsageMap);
    resourceUsageSnapshot->PoolToResourceUsageWithPrecommit = std::move(poolResourceUsageWithPrecommitMap);
    resourceUsageSnapshot->AliveOperationIds = std::move(aliveOperationIds);

    return resourceUsageSnapshot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
