#pragma once

#include "public.h"
#include "operation.h"
#include "operation_controller.h"
#include "job.h"
#include "exec_node.h"
#include "config.h"

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>
#include <ytlib/job_proxy/config.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

struct TChunkStripe;
typedef TIntrusivePtr<TChunkStripe> TChunkStripePtr;

struct TPoolExtractionResult;
typedef TIntrusivePtr<TPoolExtractionResult> TPoolExtractionResultPtr;

class TChunkPool;
typedef TIntrusivePtr<TChunkPool> TChunkPoolPtr;

class TChunkListPool;
typedef TIntrusivePtr<TChunkListPool> TChunkListPoolPtr;

class TSamplesFetcher;
typedef TIntrusivePtr<TSamplesFetcher> TSamplesFetcherPtr;

extern NLog::TLogger SchedulerLogger;
extern NLog::TLogger OperationLogger;
extern NProfiling::TProfiler SchedulerProfiler;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

