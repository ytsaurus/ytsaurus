#pragma once

#include "public.h"

#include <ytlib/misc/common.h>
#include <ytlib/misc/error.h>

#include <ytlib/logging/log.h>

#include <ytlib/profiling/profiler.h>

#include <util/generic/ptr.h>
#include <util/stream/base.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger JobProxyLogger;
extern NProfiling::TProfiler JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

