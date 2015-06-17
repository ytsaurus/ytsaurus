#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <util/generic/ptr.h>
#include <util/stream/base.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger JobProxyLogger;
extern const NProfiling::TProfiler JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

