#pragma once

#include "public.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSnapshotJob)

DECLARE_REFCOUNTED_STRUCT(TChunkStripe)

DECLARE_REFCOUNTED_CLASS(TChunkListPool)

struct IChunkPoolInput;
struct IChunkPoolOutput;
struct IChunkPool;
struct IShuffleChunkPool;

DECLARE_REFCOUNTED_CLASS(TSnapshotBuilder)
DECLARE_REFCOUNTED_CLASS(TSnapshotDownloader)

class TOperationControllerBase;

DECLARE_REFCOUNTED_CLASS(TSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerElementSharedState)
DECLARE_REFCOUNTED_CLASS(TOperationElement)
DECLARE_REFCOUNTED_CLASS(TOperationElementSharedState)
DECLARE_REFCOUNTED_CLASS(TCompositeSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TPool)
DECLARE_REFCOUNTED_CLASS(TRootElement)

struct TFairShareContext;

class TJobMetrics;
class TJobMetricsUpdater;
struct TOperationControllerInitializeResult;
class TProgressCounter;

DECLARE_REFCOUNTED_CLASS(TControllersMasterConnector)

using TOperationElementByIdMap = yhash_map<TOperationId, TOperationElement*>;

DEFINE_ENUM(ESchedulableStatus,
    (Normal)
    (BelowMinShare)
    (BelowFairShare)
);

extern const double ApproximateSizesBoostFactor;
extern const double JobSizeBoostFactor;

extern const Stroka RootPoolName;

extern const NLogging::TLogger SchedulerLogger;
extern const NLogging::TLogger OperationLogger;
extern const NLogging::TLogger ControllersMasterConnectorLogger;
extern const NProfiling::TProfiler SchedulerProfiler;

extern const TDuration PrepareYieldPeriod;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

