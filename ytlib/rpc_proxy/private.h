#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger RpcProxyClientLogger;
extern const NProfiling::TProfiler RpcProxyClientProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
