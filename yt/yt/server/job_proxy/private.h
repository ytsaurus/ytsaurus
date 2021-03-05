#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/profiling/profiler.h>

#include <util/generic/ptr.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger JobProxyLogger;
extern const TDuration RpcServerShutdownTimeout;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy

