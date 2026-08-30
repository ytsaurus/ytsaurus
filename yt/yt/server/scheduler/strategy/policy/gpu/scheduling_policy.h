#pragma once

#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy.h>

#include <yt/yt/server/scheduler/strategy/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateDryRunOrNoopSchedulingPolicy(
    std::string treeId,
    NLogging::TLogger logger,
    TWeakPtr<ISchedulingPolicyHost> host,
    IPoolTreeHost* treeHost,
    IStrategyHost* strategyHost,
    TStrategyTreeConfigPtr config,
    NProfiling::TProfiler profiler);

ISchedulingPolicyPtr CreateAllocatingSchedulingPolicy(
    std::string treeId,
    NLogging::TLogger logger,
    TWeakPtr<ISchedulingPolicyHost> host,
    IPoolTreeHost* treeHost,
    IStrategyHost* strategyHost,
    TStrategyTreeConfigPtr config,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

// COMPAT(bystrovserg): Converts the GPU policy's persistent state into the classic policy format.
NYTree::INodePtr ConvertGpuToClassicPersistentState(const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
