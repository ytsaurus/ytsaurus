#pragma once

#include "public.h"

#include <core/misc/common.h>
#include <core/misc/error.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <util/generic/ptr.h>
#include <util/stream/base.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger JobProxyLogger;
extern NProfiling::TProfiler JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

