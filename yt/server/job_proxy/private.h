#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>

#include <yt/core/profiling/profiler.h>

#include <util/generic/ptr.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger JobProxyLogger;
extern const NProfiling::TProfiler JobProxyProfiler;
extern const TDuration RpcServerShutdownTimeout;

const TString SatelliteConfigFileName("satellite_config.yson");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy

