#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger RpcProxyLogger;
extern const NProfiling::TProfiler RpcProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
