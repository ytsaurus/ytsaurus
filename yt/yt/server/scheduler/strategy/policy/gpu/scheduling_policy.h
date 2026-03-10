#pragma once

#include "public.h"

#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy.h>

#include <yt/yt/server/scheduler/strategy/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingPolicy
    : public NPolicy::ISchedulingPolicy
{
    // TODO(eshcherbin): Update node descriptor in ProcessSchedulingHeartbeat after dry-run scheduling policy is no more.
    virtual void UpdateNodeDescriptor(NNodeTrackerClient::TNodeId nodeId, TExecNodeDescriptorPtr descriptor) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulingPolicy)

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateSchedulingPolicy(
    TWeakPtr<ISchedulingPolicyHost> host,
    IStrategyHost* strategyHost,
    const std::string& treeId,
    const TStrategyTreeConfigPtr& config,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
