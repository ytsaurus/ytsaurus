#include "fair_share_tree_snapshot.h"


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
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
