#pragma once

#include <yt/server/lib/scheduler/public.h>

#include <yt/ytlib/controller_agent/public.h>

#include <yt/ytlib/scheduler/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TExecNodeDescriptorMap;
using NScheduler::TRefCountedExecNodeDescriptorMapPtr;
using NScheduler::TIncarnationId;
using NScheduler::TAgentId;

////////////////////////////////////////////////////////////////////////////////

struct TJobSummary;
struct TCompletedJobSummary;
struct TAbortedJobSummary;
using TFailedJobSummary = TJobSummary;
struct TRunningJobSummary;

DECLARE_REFCOUNTED_CLASS(TProgressCounter)
DECLARE_REFCOUNTED_STRUCT(IJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration PrepareYieldPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
