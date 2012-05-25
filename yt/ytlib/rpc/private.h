#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger RpcServerLogger;
extern NLog::TLogger RpcClientLogger;

extern NProfiling::TProfiler RpcServerProfiler;
extern NProfiling::TProfiler RpcClientProfiler;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NRpc
} // namespace NYT
