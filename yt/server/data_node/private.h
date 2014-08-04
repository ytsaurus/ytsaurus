#pragma once

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger DataNodeLogger;
extern NProfiling::TProfiler DataNodeProfiler;

NRpc::IChannelFactoryPtr GetDataNodeChannelFactory();

extern Stroka CellIdFileName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
