#include "scheduling_policy.h"
#include "scheduling_policy_detail.h"

#include "helpers.h"

#include <yt/yt/server/scheduler/strategy/helpers.h>


namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateSchedulingPolicy(
    TWeakPtr<ISchedulingPolicyHost> host,
    IStrategyHost* strategyHost,
    const std::string& treeId,
    const TStrategyTreeConfigPtr& config,
    NProfiling::TProfiler profiler)
{
    const auto& Logger = GetLogger(treeId);

    switch (config->GpuSchedulingPolicy->Mode) {
        case EGpuSchedulingPolicyMode::Noop:
            return New<TNoopSchedulingPolicy>(treeId);
        case EGpuSchedulingPolicyMode::DryRun: {
            YT_LOG_WARNING_UNLESS(IsGpuPoolTree(config),
                "GPU scheduling policy configured for a non-GPU pool tree");

            return New<TSchedulingPolicy>(
                std::move(host),
                strategyHost,
                treeId,
                config->GpuSchedulingPolicy,
                std::move(profiler));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
