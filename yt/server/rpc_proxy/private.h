#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger RpcProxyLogger;
extern const NProfiling::TProfiler RpcProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
