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

    //! Bytes of files served from cache (disk or memory, including files awaited
    //! while another job was fetching them).
    i64 FilesCachedSize = 0;
    //! Bytes of files awaited while another job was fetching them into the chunk cache.
    i64 FilesChunkCacheAsyncHitSize = 0;
    //! Bytes of layers served from cache (layer volume cache or local chunk cache,
    //! including layers awaited while another job was inserting or fetching them).
    i64 LayersCachedSize = 0;
    //! Bytes of layers served from the layer volume cache (tmpfs, SLRU or SquashFS volume cache).
    i64 LayersLayerCacheHitSize = 0;
    //! Bytes of layers served from the local chunk cache.
    i64 LayersChunkCacheHitSize = 0;
    //! Bytes of layers awaited while another job was inserting them into the layer volume cache.
    i64 LayersLayerCacheAsyncHitSize = 0;
    //! Bytes of layers awaited while another job was fetching them into the chunk cache.
    i64 LayersChunkCacheAsyncHitSize = 0;

    //! Bytes downloaded from data nodes for files (cache miss + bypass).
    i64 FilesDownloadedSize = 0;
    //! Bytes copied from cache to sandbox for files with copy_file=true.
    i64 FilesCopiedSize = 0;
    //! Bytes of layers fetched over the network.
    i64 LayersDownloadedSize = 0;
    //! Compressed archive size of layers imported by Porto (excludes SquashFS layers).
    i64 LayersImportedSize = 0;

    //! Sum of monotonic CPU durations of per-file network downloads.
    TCpuDuration FilesDownloadCpuDuration = 0;
    //! Sum of monotonic CPU durations of per-file copying from cache to sandbox.
    TCpuDuration FilesCopyCpuDuration = 0;
    //! Sum of monotonic CPU durations of awaiting a concurrent file fetch into the chunk cache.
    TCpuDuration FilesChunkCacheAsyncHitCpuDuration = 0;
    //! Sum of monotonic CPU durations of per-layer network downloads.
    TCpuDuration LayersDownloadCpuDuration = 0;
    //! Sum of monotonic CPU durations of per-layer Porto imports.
    TCpuDuration LayersImportCpuDuration = 0;
    //! Sum of monotonic CPU durations of awaiting a concurrent layer insertion into the layer volume cache.
    TCpuDuration LayersLayerCacheAsyncHitCpuDuration = 0;
    //! Sum of monotonic CPU durations of awaiting a concurrent layer fetch into the chunk cache.
    TCpuDuration LayersChunkCacheAsyncHitCpuDuration = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
