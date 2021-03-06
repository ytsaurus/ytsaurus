#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger DataNodeLogger;
extern const NProfiling::TProfiler DataNodeProfiler;
extern const NProfiling::TProfiler LocationProfiler;
extern const NLogging::TLogger P2PLogger;
extern const NProfiling::TProfiler P2PProfiler;

extern const TString CellIdFileName;
extern const TString LocationUuidFileName;
extern const TString MultiplexedDirectory;
extern const TString TrashDirectory;
extern const TString CleanExtension;
extern const TString SealedFlagExtension;
extern const TString ArtifactMetaSuffix;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
