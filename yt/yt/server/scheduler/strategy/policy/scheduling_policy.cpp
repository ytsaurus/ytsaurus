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


TError TSchedulingPolicyStaticCaller::CheckOperationIsStuck(
    const TPoolTreeSnapshotPtr& treeSnapshot,
    const TPoolTreeOperationElement* element,
    TInstant now,
    TInstant activationTime,
    const TOperationStuckCheckOptionsPtr& options)
{
    const auto& state = treeSnapshot->SchedulingPolicyState();
    YT_ASSERT(state);

    if (state->PolicyKind == EPolicyKind::Gpu) {
        return {};
    }

    return TSchedulingPolicy::CheckOperationIsStuck(
        treeSnapshot,
        element,
        now,
        activationTime,
        options);
}

void TSchedulingPolicyStaticCaller::BuildOperationProgress(
    const TPoolTreeSnapshotPtr& treeSnapshot,
    const TPoolTreeOperationElement* element,
    IStrategyHost* const strategyHost,
    NYTree::TFluentMap fluent)
{
    const auto& state = treeSnapshot->SchedulingPolicyState();
    YT_ASSERT(state);

    if (state->PolicyKind == EPolicyKind::Gpu) {
        return;
    }

    TSchedulingPolicy::BuildOperationProgress(
        treeSnapshot,
        element,
        strategyHost,
        fluent);
}

void TSchedulingPolicyStaticCaller::BuildElementYson(
    const TPoolTreeSnapshotPtr& treeSnapshot,
    const TPoolTreeElement* element,
    const TFieldFilter& filter,
    NYTree::TFluentMap fluent)
{
    const auto& state = treeSnapshot->SchedulingPolicyState();
    YT_ASSERT(state);

    if (state->PolicyKind == EPolicyKind::Gpu) {
        return;
    }

    TSchedulingPolicy::BuildElementYson(
        treeSnapshot,
        element,
        filter,
        fluent);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
