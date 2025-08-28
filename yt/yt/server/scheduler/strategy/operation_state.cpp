#include "operation_state.h"
#include "operation_controller.h"

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

TStrategyOperationState::TStrategyOperationState(
    IOperationPtr host,
    const TStrategyOperationControllerConfigPtr& config,
    const std::vector<IInvokerPtr>& nodeShardInvokers)
    : Host_(std::move(host))
    , Controller_(New<TOperationController>(Host_, config, nodeShardInvokers))
{ }

TPoolName TStrategyOperationState::GetPoolNameByTreeId(const TString& treeId) const
{
    return GetOrCrash(TreeIdToPoolNameMap_, treeId);
}

void TStrategyOperationState::UpdateConfig(const TStrategyOperationControllerConfigPtr& config)
{
    Controller_->UpdateConfig(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
