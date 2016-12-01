#pragma once

#include "public.h"
#include "private.h"
#include "config.h"
#include "job.h"

#include <yt/server/exec_agent/public.h>
#include <yt/server/exec_agent/supervisor_service_proxy.h>

#include <yt/server/job_agent/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/job_tracker_client/public.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public IJobHost
{
public:
    TJobProxy(
        TJobProxyConfigPtr config,
        const NJobTrackerClient::TOperationId& operationId,
        const NJobTrackerClient::TJobId& jobId);

    //! Runs the job. Blocks until the job is complete.
    void Run();

    IInvokerPtr GetControlInvoker() const;

    std::vector<NChunkClient::TChunkId> DumpInputContext(const NJobTrackerClient::TJobId& jobId);
    Stroka GetStderr(const NJobTrackerClient::TJobId& jobId);
    NYson::TYsonString Strace(const NJobTrackerClient::TJobId& jobId);
    void SignalJob(const NJobTrackerClient::TJobId& jobId, const Stroka& signalName);
    NYson::TYsonString PollJobShell(const NJobTrackerClient::TJobId& jobId, const NYson::TYsonString& parameters);
    void Interrupt(const NJobTrackerClient::TJobId& jobId);

    virtual NRpc::IServerPtr GetRpcServer() const override;

private:
    const TJobProxyConfigPtr Config_;
    const NJobTrackerClient::TOperationId OperationId_;
    const NJobTrackerClient::TJobId JobId_;

    //! Can be null if running in non-cgroups environment.
    NExecAgent::TCGroupJobEnvironmentConfigPtr CGroupsConfig_;

    // Job proxy memory reserve (= memory limit after multiplication by
    // job proxy memory reserve factor) by the scheduler.
    i64 JobProxyMemoryReserve_ = 0;
    // Job proxy peak memory usage.
    std::atomic<i64> JobProxyMaxMemoryUsage_ = {0};
    // If this limit for job proxy memory overcommit is exceeded, the job proxy is terminated.
    TNullable<i64> JobProxyMemoryOvercommitLimit_;

    std::atomic<i64> UserJobCurrentMemoryUsage_ = {0};

    // Job proxy and possibly user job peak memory usage.
    i64 TotalMaxMemoryUsage_ = 0;

    std::atomic<i32> NetworkUsage_ = {0};

    int CpuLimit_;

    const NConcurrency::TActionQueuePtr JobThread_;
    const NConcurrency::TActionQueuePtr ControlThread_;

    NLogging::TLogger Logger;

    NNodeTrackerClient::TNodeDescriptor LocalDescriptor_;

    NRpc::IServerPtr RpcServer_;

    std::unique_ptr<NExecAgent::TSupervisorServiceProxy> SupervisorProxy_;

    NApi::INativeClientPtr Client_;

    NNodeTrackerClient::TNodeDirectoryPtr InputNodeDirectory_;

    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    NConcurrency::TPeriodicExecutorPtr MemoryWatchdogExecutor_;

    TDuration RefCountedTrackerLogPeriod_;
    TInstant LastRefCountedTrackerLogTime_;

    IJobPtr Job_;

    IJobSpecHelperPtr JobSpecHelper_;

    void ValidateJobId(const NJobTrackerClient::TJobId& jobId);

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

    std::unique_ptr<IUserJobIO> CreateUserJobIO();
    IJobPtr CreateBuiltinJob();

    void UpdateResourceUsage();

    // IJobHost implementation.
    virtual TJobProxyConfigPtr GetConfig() const override;
    virtual NExecAgent::TCGroupJobEnvironmentConfigPtr GetCGroupsConfig() const override;
    virtual const NJobAgent::TOperationId& GetOperationId() const override;
    virtual const NJobAgent::TJobId& GetJobId() const override;

    virtual const IJobSpecHelperPtr& GetJobSpecHelper() const override;

    virtual void SetUserJobMemoryUsage(i64 memoryUsage) override;
    void OnResourcesUpdated(const TError& error);

    virtual void ReleaseNetwork() override;

    virtual NApi::INativeClientPtr GetClient() const override;

    virtual void OnPrepared() override;

    virtual NChunkClient::IBlockCachePtr GetBlockCache() const override;

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetInputNodeDirectory() const override;

    virtual const NNodeTrackerClient::TNodeDescriptor& LocalDescriptor() const override;

    virtual NLogging::TLogger GetLogger() const override;

    void CheckMemoryUsage();

    void CheckResult(const NJobTrackerClient::NProto::TJobResult& jobResult);

    void Exit(EJobProxyExitCode exitCode);
};

DEFINE_REFCOUNTED_TYPE(TJobProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
