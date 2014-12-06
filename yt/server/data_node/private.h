#pragma once

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger DataNodeLogger;
extern NProfiling::TProfiler DataNodeProfiler;

extern NRpc::IChannelFactoryPtr ChannelFactory;

extern Stroka CellIdFileName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
