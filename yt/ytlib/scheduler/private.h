#pragma once

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

class TChunkListPool;
typedef TIntrusivePtr<TChunkListPool> TChunkListPoolPtr;

class TSamplesFetcher;
typedef TIntrusivePtr<TSamplesFetcher> TSamplesFetcherPtr;

extern NLog::TLogger SchedulerLogger;
extern NLog::TLogger OperationsLogger;
extern NProfiling::TProfiler SchedulerProfiler;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

