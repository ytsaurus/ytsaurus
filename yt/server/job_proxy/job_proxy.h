#pragma once

#include "public.h"
#include "private.h"
#include "config.h"
#include "job.h"
#include "job_satellite_connection.h"
#include "environment.h"

#include <yt/server/exec_agent/public.h>
#include <yt/server/exec_agent/supervisor_service_proxy.h>

#include <yt/server/job_agent/public.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/job_prober_client/job_probe.h>

#include <yt/ytlib/job_tracker_client/public.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/logging/log.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public IJobHost
    , public NJobProberClient::IJobProbe
{
public:
    TJobProxy(
        TJobProxyConfigPtr config,
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId);

    //! Runs the job. Blocks until the job is complete.
    void Run();

    IInvokerPtr GetControlInvoker() const;

    virtual std::vector<NChunkClient::TChunkId> DumpInputContext() override;
    virtual TString GetStderr() override;
    virtual NYson::TYsonString StraceJob() override;
    virtual void SignalJob(const TString& signalName) override;
    virtual NYson::TYsonString PollJobShell(const NYson::TYsonString& parameters) override;
    virtual void Interrupt() override;
    virtual void Fail() override;

    virtual NJobAgent::TJobId GetJobId() const override;

    virtual NRpc::IServerPtr GetRpcServer() const override;

    virtual TString GetPreparationPath() const override;
    virtual TString GetSlotPath() const override;
    virtual TString AdjustPath(const TString& path) const override;

    virtual NChunkClient::TTrafficMeterPtr GetTrafficMeter() const override;

    virtual NConcurrency::IThroughputThrottlerPtr GetInBandwidthThrottler() const override;
    virtual NConcurrency::IThroughputThrottlerPtr GetOutBandwidthThrottler() const override;
    virtual NConcurrency::IThroughputThrottlerPtr GetOutRpsThrottler() const override;

    TDuration GetSpentCpuTime() const;

    void SetCpuLimit(double cpuLimit);

private:
    const TJobProxyConfigPtr Config_;
    const NJobTrackerClient::TOperationId OperationId_;
    const NJobTrackerClient::TJobId JobId_;

    //! Can be null if running in non-porto and non-cgroups environment.
    IJobProxyEnvironmentPtr JobProxyEnvironment_;

    TCpuMonitorPtr CpuMonitor_;

    // Job proxy memory reserve (= memory limit after multiplication by
    // job proxy memory reserve factor) by the scheduler.
    i64 JobProxyMemoryReserve_ = 0;
    // Job proxy peak memory usage.
    std::atomic<i64> JobProxyMaxMemoryUsage_ = {0};
    // If this limit for job proxy memory overcommit is exceeded, the job proxy is terminated.
    std::optional<i64> JobProxyMemoryOvercommitLimit_;

    std::atomic<i64> UserJobCurrentMemoryUsage_ = {0};

    // Job proxy and possibly user job peak memory usage.
    i64 TotalMaxMemoryUsage_ = 0;

    // Memory reserve approved by the node.
    std::atomic<i64> ApprovedMemoryReserve_ = {0};
    std::atomic<i64> RequestedMemoryReserve_ = {0};

    std::atomic<i32> NetworkUsage_ = {0};

    std::atomic<double> CpuLimit_ = {0};

    const NConcurrency::TActionQueuePtr JobThread_;
    const NConcurrency::TActionQueuePtr ControlThread_;

    NLogging::TLogger Logger;

    NNodeTrackerClient::TNodeDescriptor LocalDescriptor_;

    NRpc::IServerPtr RpcServer_;

    std::unique_ptr<NExecAgent::TSupervisorServiceProxy> SupervisorProxy_;

    NApi::NNative::IClientPtr Client_;

    NNodeTrackerClient::TNodeDirectoryPtr InputNodeDirectory_;

    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    NConcurrency::TPeriodicExecutorPtr MemoryWatchdogExecutor_;

    TDuration RefCountedTrackerLogPeriod_;
    TInstant LastRefCountedTrackerLogTime_;

    IJobPtr Job_;

    IJobSpecHelperPtr JobSpecHelper_;

    std::vector<int> Ports_;

    NChunkClient::TTrafficMeterPtr TrafficMeter_;

    NConcurrency::IThroughputThrottlerPtr InBandwidthThrottler_;
    NConcurrency::IThroughputThrottlerPtr OutBandwidthThrottler_;
    NConcurrency::IThroughputThrottlerPtr OutRpsThrottler_;

    void ValidateJobId(NJobTrackerClient::TJobId jobId);

    NJobTrackerClient::NProto::TJobResult DoRun();
    void SendHeartbeat();
    void OnHeartbeatResponse(const TError& error);

    void RetrieveJobSpec();
    void ReportResult(
        const NJobTrackerClient::NProto::TJobResult& result,
        const NYson::TYsonString& statistics,
        TInstant startTime,
        TInstant finishTime);

    NJobTrackerClient::TStatistics GetStatistics() const;

    IJobPtr CreateBuiltinJob();

    void UpdateResourceUsage();

    // IJobHost implementation.
    virtual TJobProxyConfigPtr GetConfig() const override;
    virtual IUserJobEnvironmentPtr CreateUserJobEnvironment() const override;
    virtual NJobAgent::TOperationId GetOperationId() const override;

    virtual const IJobSpecHelperPtr& GetJobSpecHelper() const override;

    virtual void SetUserJobMemoryUsage(i64 memoryUsage) override;
    void OnResourcesUpdated(i64 memoryUsage, const TError& error);

    virtual void ReleaseNetwork() override;

    virtual NApi::NNative::IClientPtr GetClient() const override;

    virtual void OnPrepared() override;

    virtual NChunkClient::IBlockCachePtr GetBlockCache() const override;

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetInputNodeDirectory() const override;

    virtual const NNodeTrackerClient::TNodeDescriptor& LocalDescriptor() const override;

    virtual NLogging::TLogger GetLogger() const override;

    void CheckMemoryUsage();

    void EnsureStderrResult(NJobTrackerClient::NProto::TJobResult* jobResult);

    void Exit(EJobProxyExitCode exitCode);
};

DEFINE_REFCOUNTED_TYPE(TJobProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
