#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJob)

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger ExecNodeLogger("ExecNode");
inline const NProfiling::TProfiler ExecNodeProfiler("/exec_node");

constexpr int TmpfsRemoveAttemptCount = 5;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
