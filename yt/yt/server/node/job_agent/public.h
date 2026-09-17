#pragma once

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <library/cpp/yt/cpu_clock/public.h>

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

DECLARE_REFCOUNTED_STRUCT(TMappedMemoryControllerConfig)
DECLARE_REFCOUNTED_CLASS(TResourceOwner)
DECLARE_REFCOUNTED_CLASS(TResourceHolder)
DECLARE_REFCOUNTED_CLASS(TJobResourceManager)

struct TArtifactStatistics
{
    i64 CacheHitArtifactsSize = 0;
    i64 CacheMissArtifactsSize = 0;
    i64 CacheBypassedArtifactsSize = 0;

    //! Bytes of files served from cache (disk or memory).
    i64 FilesCachedSize = 0;
    //! Bytes of layers served from cache (disk or memory).
    i64 LayersCachedSize = 0;

    //! Bytes downloaded from data nodes for files (cache miss + bypass).
    i64 FilesDownloadedSize = 0;
    //! Bytes copied from cache to sandbox for files with copy_file=true.
    i64 FilesCopiedSize = 0;
    //! Bytes downloaded from data nodes for layers (cache miss only).
    i64 LayersDownloadedSize = 0;
    //! Compressed archive size of layers imported by Porto (excludes SquashFS layers).
    i64 LayersImportedSize = 0;

    //! Wall time (monotonic clock) of caching file artifacts (excludes cache-bypassed and virtual-sandbox files).
    TCpuDuration FilesDownloadedDuration = 0;

    //! Sum of per-file network download durations (monotonic clock).
    TCpuDuration FilesDownloadedAggrDuration = 0;
    //! Sum of per-file copying durations from cache to sandbox (monotonic clock).
    TCpuDuration FilesCopiedAggrDuration = 0;
    //! Sum of per-layer network download durations (monotonic clock).
    TCpuDuration LayersDownloadedAggrDuration = 0;
    //! Sum of per-layer Porto import durations (monotonic clock).
    TCpuDuration LayersImportedAggrDuration = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
