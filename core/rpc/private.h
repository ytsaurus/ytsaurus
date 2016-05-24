#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/misc/public.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger RpcServerLogger;
extern const NLogging::TLogger RpcClientLogger;

extern const NProfiling::TProfiler RpcServerProfiler;
extern const NProfiling::TProfiler RpcClientProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
