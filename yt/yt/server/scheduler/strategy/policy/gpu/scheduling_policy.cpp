#include "scheduling_policy.h"
#include "scheduling_policy_detail.h"

#include "helpers.h"

#include <yt/yt/server/scheduler/strategy/helpers.h>


namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateGpuSchedulingPolicy(
    std::string treeId,
    NLogging::TLogger Logger,
    TWeakPtr<ISchedulingPolicyHost> host,
    IPoolTreeHost* /*treeHost*/,
    IStrategyHost* strategyHost,
    TStrategyTreeConfigPtr config,
    NProfiling::TProfiler profiler)
{
    if (config->GpuSchedulingPolicy->Mode == EGpuSchedulingPolicyMode::Noop) {
        return New<TNoopSchedulingPolicy>(treeId);
    }

    YT_LOG_WARNING_UNLESS(IsGpuPoolTree(config),
        "GPU scheduling policy configured for a non-GPU pool tree");

    return New<TSchedulingPolicy>(
        std::move(host),
        strategyHost,
        treeId,
        config->GpuSchedulingPolicy,
        profiler.WithPrefix("/gpu_policy"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateDryRunOrNoopSchedulingPolicy(
    std::string treeId,
    NLogging::TLogger logger,
    TWeakPtr<ISchedulingPolicyHost> host,
    IPoolTreeHost* treeHost,
    IStrategyHost* strategyHost,
    TStrategyTreeConfigPtr config,
    NProfiling::TProfiler profiler)
{
    if (config->GpuSchedulingPolicy->Mode == EGpuSchedulingPolicyMode::Allocating) {
        return New<TNoopSchedulingPolicy>(treeId);
    }

    if (config->GpuSchedulingPolicy->Mode == EGpuSchedulingPolicyMode::DryRun) {
        YT_ASSERT(config->PolicyKind == EPolicyKind::Classic);
    }

    return CreateGpuSchedulingPolicy(
        treeId,
        std::move(logger),
        host,
        treeHost,
        strategyHost,
        config,
        std::move(profiler));
}

ISchedulingPolicyPtr CreateAllocatingSchedulingPolicy(
    std::string treeId,
    NLogging::TLogger logger,
    TWeakPtr<ISchedulingPolicyHost> host,
    IPoolTreeHost* treeHost,
    IStrategyHost* strategyHost,
    TStrategyTreeConfigPtr config,
    NProfiling::TProfiler profiler)
{
    YT_VERIFY(config->GpuSchedulingPolicy->Mode == EGpuSchedulingPolicyMode::Allocating);
    YT_VERIFY(config->PolicyKind == EPolicyKind::Gpu);

    return CreateGpuSchedulingPolicy(
        treeId,
        std::move(logger),
        host,
        treeHost,
        strategyHost,
        config,
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
