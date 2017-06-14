#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>

#include <yt/core/profiling/profiler.h>

#include <util/generic/ptr.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger JobProxyLogger;
extern const NProfiling::TProfiler JobProxyProfiler;
extern const TDuration RpcServerShutdownTimeout;

const TString SatelliteConfigFileName("satellite_config.yson");

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

