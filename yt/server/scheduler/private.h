#pragma once

#include "public.h"
#include "operation.h"
#include "operation_controller.h"
#include "job.h"
#include "exec_node.h"

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TChunkStripe)
DECLARE_REFCOUNTED_STRUCT(TChunkStripeList)

DECLARE_REFCOUNTED_CLASS(TChunkListPool)
DECLARE_REFCOUNTED_CLASS(TChunkTeleporter)

struct IChunkPoolInput;
struct IChunkPoolOutput;
struct IChunkPool;
struct IShuffleChunkPool;

DECLARE_REFCOUNTED_CLASS(TSnapshotBuilder)
DECLARE_REFCOUNTED_CLASS(TSnapshotDownloader)

class TOperationControllerBase;

extern const double ApproximateSizesBoostFactor;

extern const Stroka RootPoolName;

extern const NLogging::TLogger SchedulerLogger;
extern const NLogging::TLogger OperationLogger;
extern const NProfiling::TProfiler SchedulerProfiler;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

