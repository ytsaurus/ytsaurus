#pragma once

#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

//! A policy which does nothing. Used when the GPU policy is disabled for the tree.
ISchedulingPolicyPtr CreateNoopSchedulingPolicy(NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
