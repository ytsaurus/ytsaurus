#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/global.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOperation)

DECLARE_REFCOUNTED_CLASS(TAllocation)

DECLARE_REFCOUNTED_CLASS(TExecNode)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnutilizedResourceReason,
    (Utilized)
    (Unknown)
    (Timeout)
    (Throttling)
    (FinishedJobs)
    (NodeHasWaitingAllocations)
    (AcceptableFragmentation)
    (ExcessiveFragmentation)
);

////////////////////////////////////////////////////////////////////////////////

// For each memory capacity gives the number of nodes with this much memory.
using TMemoryDistribution = THashMap<i64, int>;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerEventLogger, NLogging::TLogger("SchedulerEventLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerStructuredLogger, NLogging::TLogger("SchedulerStructuredLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerGpuEventLogger, NLogging::TLogger("SchedulerGpuStructuredLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerResourceMeteringLogger, NLogging::TLogger("SchedulerResourceMetering").WithEssential());

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, SchedulerProfiler, "/scheduler");

////////////////////////////////////////////////////////////////////////////////

} // NAMESPACE NYT::NSCHEDULER
