#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger DataNodeLogger;
extern const NProfiling::TProfiler DataNodeProfiler;

extern const Stroka CellIdFileName;
extern const Stroka MultiplexedDirectory;
extern const Stroka TrashDirectory;
extern const Stroka CleanExtension;
extern const Stroka SealedFlagExtension;
extern const Stroka ArtifactMetaSuffix;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
