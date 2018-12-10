#pragma once

#include "public.h"

#include <yt/server/exec_agent/public.h>

#include <yt/server/job_proxy/environment.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/ytlib/job_proxy/job_spec_helper.h>

#include <yt/ytlib/job_tracker_client/public.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/job_prober_client/job_probe.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/core/logging/log.h>

#include <yt/core/rpc/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

//! Represents a context for running jobs inside job proxy.
struct IJobHost
    : public virtual TRefCounted
{
    virtual TJobProxyConfigPtr GetConfig() const = 0;
    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment() const = 0;
    virtual const NJobTrackerClient::TOperationId& GetOperationId() const = 0;
    virtual const NJobTrackerClient::TJobId& GetJobId() const = 0;

    virtual const IJobSpecHelperPtr& GetJobSpecHelper() const = 0;

    virtual void SetUserJobMemoryUsage(i64 memoryUsage) = 0;

    virtual void ReleaseNetwork() = 0;

    virtual NApi::NNative::IClientPtr GetClient() const = 0;

    virtual void OnPrepared() = 0;

    virtual NChunkClient::IBlockCachePtr GetBlockCache() const = 0;

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetInputNodeDirectory() const = 0;

    virtual const NNodeTrackerClient::TNodeDescriptor& LocalDescriptor() const = 0;

    virtual NLogging::TLogger GetLogger() const = 0;

    virtual NRpc::IServerPtr GetRpcServer() const = 0;

    virtual TString GetPreparationPath() const = 0;
    virtual TString GetSlotPath() const = 0;

    virtual NChunkClient::TTrafficMeterPtr GetTrafficMeter() const = 0;

    virtual NConcurrency::IThroughputThrottlerPtr GetInBandwidthThrottler() const = 0;
    virtual NConcurrency::IThroughputThrottlerPtr GetOutBandwidthThrottler() const = 0;
    virtual NConcurrency::IThroughputThrottlerPtr GetOutRpsThrottler() const = 0;

    virtual TString AdjustPath(const TString& path) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobHost)

////////////////////////////////////////////////////////////////////////////////

struct IJob
    : public NJobProberClient::IJobProbe
{
    virtual void Initialize() = 0;
    virtual NJobTrackerClient::NProto::TJobResult Run() = 0;

    //! Tries to clean up (e.g. user processes), best effort guarantees.
    //! Used during abnormal job proxy termination.
    virtual void Cleanup() = 0;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const = 0;
    virtual NChunkClient::TInterruptDescriptor GetInterruptDescriptor() const = 0;

    virtual double GetProgress() const = 0;

    virtual ui64 GetStderrSize() const = 0;

    virtual std::optional<TString> GetFailContext() = 0;

    virtual NJobTrackerClient::TStatistics GetStatistics() const = 0;

    virtual TCpuStatistics GetCpuStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
