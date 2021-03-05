#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger RpcServerLogger;
extern const NLogging::TLogger RpcClientLogger;

extern const NProfiling::TRegistry RpcServerProfiler;
extern const NProfiling::TRegistry RpcClientProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
