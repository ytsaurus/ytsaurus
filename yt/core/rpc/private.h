#pragma once

#include <core/misc/public.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger RpcServerLogger;
extern const NLogging::TLogger RpcClientLogger;

extern NProfiling::TProfiler RpcServerProfiler;
extern NProfiling::TProfiler RpcClientProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
