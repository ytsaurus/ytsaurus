#pragma once

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger DataNodeLogger;
extern const NProfiling::TProfiler DataNodeProfiler;

extern NRpc::IChannelFactoryPtr ChannelFactory;

extern Stroka CellIdFileName;
extern Stroka MultiplexedDirectory;
extern Stroka TrashDirectory;
extern Stroka CleanExtension;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
