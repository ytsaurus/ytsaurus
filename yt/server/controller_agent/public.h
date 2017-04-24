#pragma once

#include <yt/server/scheduler/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TChunkStripeList)

DECLARE_REFCOUNTED_STRUCT(IChunkSliceFetcherFactory)

DECLARE_REFCOUNTED_STRUCT(IJobSizeConstraints)
DECLARE_REFCOUNTED_CLASS(TJobSizeAdjusterConfig)

DECLARE_REFCOUNTED_STRUCT(TScheduleJobStatistics)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
