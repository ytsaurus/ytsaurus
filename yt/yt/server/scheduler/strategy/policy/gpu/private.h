#pragma once

#include "public.h"

#include <library/cpp/yt/misc/global.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, GpuSchedulingPolicyLogger, "GpuSchedulingPolicy");

////////////////////////////////////////////////////////////////////////////////

using TSchedulingModule = std::string;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
