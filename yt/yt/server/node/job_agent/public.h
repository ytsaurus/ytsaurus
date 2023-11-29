#pragma once

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobOrigin,
    ((Master)    (0))
    ((Scheduler) (1))
);

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::TOperationId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

DECLARE_REFCOUNTED_CLASS(TMappedMemoryControllerConfig)
DECLARE_REFCOUNTED_CLASS(TResourceHolder)
DECLARE_REFCOUNTED_CLASS(IJobResourceManager)

struct TChunkCacheStatistics
{
    i64 CacheHitArtifactsSize = 0;
    i64 CacheMissArtifactsSize = 0;
    i64 CacheBypassedArtifactsSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
