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

} // namespace NYT::NControllerAgent
