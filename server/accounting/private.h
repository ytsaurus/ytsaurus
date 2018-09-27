#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYP {
namespace NServer {
namespace NAccounting {

////////////////////////////////////////////////////////////////////////////////

extern const NYT::NLogging::TLogger Logger;
extern const NYT::NProfiling::TProfiler Profiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NAccounting
} // namespace NServer
} // namespace NYP
