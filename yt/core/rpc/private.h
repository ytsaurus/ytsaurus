#pragma once

#include <core/misc/common.h>
#include <core/logging/log.h>
#include <core/profiling/profiler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger RpcServerLogger;
extern const NLog::TLogger RpcClientLogger;

extern NProfiling::TProfiler RpcServerProfiler;
extern NProfiling::TProfiler RpcClientProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
