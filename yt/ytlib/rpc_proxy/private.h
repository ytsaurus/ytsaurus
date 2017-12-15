#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConnection)
DECLARE_REFCOUNTED_CLASS(TClientBase)
DECLARE_REFCOUNTED_CLASS(TClient)
DECLARE_REFCOUNTED_CLASS(TTransaction)

extern const NLogging::TLogger RpcProxyClientLogger;
extern const NProfiling::TProfiler RpcProxyClientProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
