#include "scheduling_policy.h"

#include "scheduling_policy_detail.h"

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

TPostUpdateContext::TPostUpdateContext(EPolicyKind policyKind)
    : PolicyKind(policyKind)
{ }

TPoolTreeSnapshotState::TPoolTreeSnapshotState(EPolicyKind policyKind)
    : PolicyKind(policyKind)
{ }

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateSchedulingPolicy(
    std::string treeId,
    NLogging::TLogger logger,
    TWeakPtr<ISchedulingPolicyHost> host,
    IPoolTreeHost* treeHost,
    IStrategyHost* strategyHost,
    TStrategyTreeConfigPtr config,
    NProfiling::TProfiler profiler)
{
    return New<TSchedulingPolicy>(
        std::move(treeId),
        std::move(logger),
        host,
        treeHost,
        strategyHost,
        config,
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
