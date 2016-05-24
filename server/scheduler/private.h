#pragma once

#include "public.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"
#include "operation_controller.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

struct TChunkStripe;
typedef TIntrusivePtr<TChunkStripe> TChunkStripePtr;

struct TChunkStripeList;
typedef TIntrusivePtr<TChunkStripeList> TChunkStripeListPtr;

class TChunkListPool;
typedef TIntrusivePtr<TChunkListPool> TChunkListPoolPtr;

struct IChunkPoolInput;
struct IChunkPoolOutput;
struct IChunkPool;
struct IShuffleChunkPool;

class TSnapshotBuilder;
typedef TIntrusivePtr<TSnapshotBuilder> TSnapshotBuilderPtr;

class TSnapshotDownloader;
typedef TIntrusivePtr<TSnapshotDownloader> TSnapshotDownloaderPtr;

class TOperationControllerBase;

extern const double ApproximateSizesBoostFactor;

extern const Stroka RootPoolName;

extern const NLogging::TLogger SchedulerLogger;
extern const NLogging::TLogger OperationLogger;
extern const NProfiling::TProfiler SchedulerProfiler;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

