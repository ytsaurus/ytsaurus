#pragma once

#include "public.h"

#include <yt/server/exec_agent/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/ytlib/job_proxy/job_spec_helper.h>

#include <yt/ytlib/job_tracker_client/public.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/job.pb.h>

#include <yt/core/logging/log.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

//! Represents a context for running jobs inside job proxy.
struct IJobHost
    : public virtual TRefCounted
{
    virtual TJobProxyConfigPtr GetConfig() const = 0;
    virtual NExecAgent::TCGroupJobEnvironmentConfigPtr GetCGroupsConfig() const = 0;
    virtual const NJobTrackerClient::TOperationId& GetOperationId() const = 0;
    virtual const NJobTrackerClient::TJobId& GetJobId() const = 0;

    virtual const IJobSpecHelperPtr& GetJobSpecHelper() const = 0;

    virtual void SetUserJobMemoryUsage(i64 memoryUsage) = 0;

    virtual void ReleaseNetwork() = 0;

    virtual NApi::INativeClientPtr GetClient() const = 0;

    virtual void OnPrepared() = 0;

    virtual NChunkClient::IBlockCachePtr GetBlockCache() const = 0;

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetInputNodeDirectory() const = 0;

    virtual const NNodeTrackerClient::TNodeDescriptor& LocalDescriptor() const = 0;

    virtual NLogging::TLogger GetLogger() const = 0;

    virtual NRpc::IServerPtr GetRpcServer() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobHost)

////////////////////////////////////////////////////////////////////////////////

//! Represents a job running inside job proxy.
struct IJob
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;
    virtual NJobTrackerClient::NProto::TJobResult Run() = 0;

    //! Tries to clean up (e.g. user processes), best effort guarantees.
    //! Used during abnormal job proxy termination.
    virtual void Abort() = 0;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const = 0;
    virtual std::vector<NChunkClient::TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const = 0;

    virtual double GetProgress() const = 0;

    virtual NJobTrackerClient::TStatistics GetStatistics() const = 0;

    virtual std::vector<NChunkClient::TChunkId> DumpInputContext() = 0;
    virtual TString GetStderr() = 0;
    virtual NYson::TYsonString StraceJob() = 0;
    virtual void SignalJob(const TString& signalName) = 0;
    virtual NYson::TYsonString PollJobShell(const NYson::TYsonString& parameters) = 0;
    virtual void Interrupt() = 0;

};

DEFINE_REFCOUNTED_TYPE(IJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
