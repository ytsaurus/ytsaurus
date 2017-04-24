#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSnapshotJob)

DECLARE_REFCOUNTED_CLASS(TSnapshotBuilder)
DECLARE_REFCOUNTED_CLASS(TSnapshotDownloader)

class TOperationControllerBase;

DECLARE_REFCOUNTED_STRUCT(TChunkStripe)

DECLARE_REFCOUNTED_CLASS(TChunkListPool)

DECLARE_REFCOUNTED_CLASS(TControllersMasterConnector)

struct IChunkPoolInput;
struct IChunkPoolOutput;
struct IChunkPool;
struct IShuffleChunkPool;

////////////////////////////////////////////////////////////////////

extern const double ApproximateSizesBoostFactor;
extern const double JobSizeBoostFactor;

extern const TDuration PrepareYieldPeriod;

////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger OperationLogger;
extern const NLogging::TLogger ControllersMasterConnectorLogger;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

