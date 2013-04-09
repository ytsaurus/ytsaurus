#pragma once

#include "public.h"
#include "operation.h"
#include "operation_controller.h"
#include "job.h"
#include "exec_node.h"
#include "config.h"

#include <ytlib/logging/log.h>

#include <ytlib/profiling/profiler.h>

#include <ytlib/scheduler/config.h>

#include <server/job_proxy/config.h>

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

template <class TFetcher>
class TChunkInfoCollector;

class TSamplesFetcher;
typedef TIntrusivePtr<TSamplesFetcher> TSamplesFetcherPtr;

typedef TChunkInfoCollector<TSamplesFetcher> TSamplesCollector;
typedef TIntrusivePtr<TSamplesCollector> TSamplesCollectorPtr;

class TChunkSplitsFetcher;
typedef TIntrusivePtr<TChunkSplitsFetcher> TChunkSplitsFetcherPtr;

typedef TChunkInfoCollector<TChunkSplitsFetcher> TChunkSplitsCollector;
typedef TIntrusivePtr<TChunkSplitsCollector> TChunkSplitsCollectorPtr;

extern const double ApproximateSizesBoostFactor;

extern NLog::TLogger SchedulerLogger;
extern NLog::TLogger OperationLogger;
extern NProfiling::TProfiler SchedulerProfiler;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

