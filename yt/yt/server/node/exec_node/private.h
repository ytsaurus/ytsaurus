#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJob)
DECLARE_REFCOUNTED_CLASS(TAllocation)

DECLARE_REFCOUNTED_STRUCT(TSchedulerHeartbeatContext)

DECLARE_REFCOUNTED_STRUCT(TAgentHeartbeatContext)

struct TControllerAgentDescriptor;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ExecNodeLogger, "ExecNode");
inline const NProfiling::TProfiler ExecNodeProfiler("/exec_node");

inline const NLogging::TLogger JobInputCacheLogger("JobInputCache");
inline const NProfiling::TProfiler JobInputCacheProfiler = ExecNodeProfiler.WithPrefix("/job_input_cache");

constexpr int TmpfsRemoveAttemptCount = 5;

////////////////////////////////////////////////////////////////////////////////

struct TShellCommandOutput
{
    TString Stdout;
    TString Stderr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
