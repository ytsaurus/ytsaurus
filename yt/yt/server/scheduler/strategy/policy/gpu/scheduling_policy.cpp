#include "scheduling_policy.h"

#include "noop_scheduling_policy.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

Y_WEAK ISchedulingPolicyPtr CreateDryRunOrNoopSchedulingPolicy(
    std::string /*treeId*/,
    NLogging::TLogger logger,
    TWeakPtr<ISchedulingPolicyHost> /*host*/,
    IPoolTreeHost* /*treeHost*/,
    IStrategyHost* /*strategyHost*/,
    TStrategyTreeConfigPtr /*config*/,
    NProfiling::TProfiler /*profiler*/)
{
    return CreateNoopSchedulingPolicy(std::move(logger));
}

Y_WEAK ISchedulingPolicyPtr CreateAllocatingSchedulingPolicy(
    std::string /*treeId*/,
    NLogging::TLogger /*logger*/,
    TWeakPtr<ISchedulingPolicyHost> /*host*/,
    IPoolTreeHost* /*treeHost*/,
    IStrategyHost* /*strategyHost*/,
    TStrategyTreeConfigPtr /*config*/,
    NProfiling::TProfiler /*profiler*/)
{
    THROW_ERROR_EXCEPTION("GPU scheduling policy is not supported in this build");
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK INodePtr ConvertGpuToClassicPersistentState(const INodePtr& /*node*/)
{
    THROW_ERROR_EXCEPTION("GPU scheduling policy is not supported in this build");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
