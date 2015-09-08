#pragma once

#include "public.h"

#include <ytlib/api/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/scheduler/job.pb.h>
#include <ytlib/scheduler/statistics.h>

#include <ytlib/job_tracker_client/public.h>

#include <core/logging/log.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

//! Represents a context for running jobs inside job proxy.
struct IJobHost
    : public virtual TRefCounted
{
    virtual TJobProxyConfigPtr GetConfig() = 0;
    virtual const NJobTrackerClient::NProto::TJobSpec& GetJobSpec() const = 0;

    virtual const NNodeTrackerClient::NProto::TNodeResources& GetResourceUsage() const = 0;
    virtual void SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& usage) = 0;

    virtual void ReleaseNetwork() = 0;

    virtual NApi::IClientPtr GetClient() const = 0;

    virtual NChunkClient::IBlockCachePtr GetBlockCache() const = 0;

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetInputNodeDirectory() const = 0;
    virtual NNodeTrackerClient::TNodeDirectoryPtr GetAuxNodeDirectory() const = 0;

    virtual NLogging::TLogger GetLogger() const = 0;

    virtual std::vector<NChunkClient::TChunkId> DumpInputContext(const NJobTrackerClient::TJobId& jobId) = 0;
    virtual NYson::TYsonString Strace(const NJobTrackerClient::TJobId& jobId) = 0;

};

DEFINE_REFCOUNTED_TYPE(IJobHost)

////////////////////////////////////////////////////////////////////////////////

//! Represents a job running inside job proxy.
struct IJob
    : public virtual TRefCounted
{
    virtual ~IJob()
    { }

    virtual NJobTrackerClient::NProto::TJobResult Run() = 0;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const = 0;

    virtual double GetProgress() const = 0;

    virtual NScheduler::TStatistics GetStatistics() const = 0;

    virtual std::vector<NChunkClient::TChunkId> DumpInputContext() = 0;
    virtual NYson::TYsonString Strace() = 0;

};

DEFINE_REFCOUNTED_TYPE(IJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
