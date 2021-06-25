#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/job_report.h>

#include <yt/yt/server/job_proxy/environment.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/ytlib/job_proxy/job_spec_helper.h>

#include <yt/yt/ytlib/job_tracker_client/public.h>

#include <yt/yt/ytlib/job_prober_client/job_probe.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/misc/statistics.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobSpecEnvironmentOptions
{
    bool EnablePortoMemoryTracking = false;
    bool EnableCoreDumps = false;
    bool EnableGpuCoreDumps = false;
    NContainers::EEnablePorto EnablePorto = NContainers::EEnablePorto::None;
    i64 ThreadLimit;
};

//! Represents a context for running jobs inside job proxy.
struct IJobHost
    : public virtual TRefCounted
{
    virtual TJobProxyConfigPtr GetConfig() const = 0;
    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment(const TJobSpecEnvironmentOptions& options) const = 0;

    virtual NJobTrackerClient::TOperationId GetOperationId() const = 0;
    virtual NJobTrackerClient::TJobId GetJobId() const = 0;

    virtual const IJobSpecHelperPtr& GetJobSpecHelper() const = 0;

    virtual void SetUserJobMemoryUsage(i64 memoryUsage) = 0;

    virtual void ReleaseNetwork() = 0;

    virtual TString GetJobUserName() const = 0;

    virtual NApi::NNative::IClientPtr GetClient() const = 0;

    virtual void OnPrepared() = 0;

    virtual void PrepareArtifact(
        const TString& artifactName,
        const TString& pipePath) = 0;
    virtual void OnArtifactPreparationFailed(
        const TString& artifactName,
        const TString& artifactPath,
        const TError& error) = 0;

    virtual NChunkClient::IBlockCachePtr GetReaderBlockCache() const = 0;
    virtual NChunkClient::IBlockCachePtr GetWriterBlockCache() const = 0;

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

    virtual IInvokerPtr GetControlInvoker() const = 0;
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

    virtual void PrepareArtifacts() = 0;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const = 0;
    virtual NChunkClient::TInterruptDescriptor GetInterruptDescriptor() const = 0;

    virtual double GetProgress() const = 0;

    virtual i64 GetStderrSize() const = 0;

    virtual std::optional<TString> GetFailContext() = 0;

    virtual std::optional<NJobAgent::TJobProfile> GetProfile() = 0;

    virtual TStatistics GetStatistics() const = 0;

    virtual const NCoreDump::TCoreInfos& GetCoreInfos() const = 0;

    virtual TCpuStatistics GetCpuStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
