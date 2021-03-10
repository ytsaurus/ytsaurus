#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger RpcProxyLogger;

extern const NProfiling::TProfiler RpcProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
