#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSnapshotJob)

DECLARE_REFCOUNTED_CLASS(TSnapshotBuilder)
DECLARE_REFCOUNTED_CLASS(TSnapshotDownloader)

class TOperationControllerBase;

DECLARE_REFCOUNTED_CLASS(TChunkListPool)

DECLARE_REFCOUNTED_CLASS(TMasterConnector)

class TJobMetricsUpdater;

////////////////////////////////////////////////////////////////////////////////

extern const double ApproximateSizesBoostFactor;
extern const double JobSizeBoostFactor;

extern const TDuration PrepareYieldPeriod;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger OperationLogger;
extern const NLogging::TLogger MasterConnectorLogger;
extern const NProfiling::TProfiler ControllerAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

