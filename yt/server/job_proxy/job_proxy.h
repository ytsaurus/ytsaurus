#pragma once

#include "public.h"
#include "private.h"
#include "config.h"
#include "job.h"

#include <yt/server/exec_agent/supervisor_service_proxy.h>

#include <yt/server/job_agent/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/job_tracker_client/public.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

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
        NYTree::INodePtr configNode,
        const NJobAgent::TJobId& jobId);

    //! Runs the job. Blocks until the job is complete.
    void Run();

    IInvokerPtr GetControlInvoker() const;

    std::vector<NChunkClient::TChunkId> DumpInputContext(const NJobTrackerClient::TJobId& jobId);
    NYson::TYsonString Strace(const NJobTrackerClient::TJobId& jobId);
    void SignalJob(const NJobTrackerClient::TJobId& jobId, const Stroka& signalName);

private:
    const NYTree::INodePtr ConfigNode_;
    const NJobAgent::TJobId JobId_;

    // Job proxy memory limit by the scheduler.
    i64 JobProxyInitialMemoryLimit_;
    // Job proxy memory limit possibly after increases because of memory overcommit.
    i64 JobProxyCurrentMemoryLimit_;
    // If this limit for job proxy memory overcommit is exceeded, the job proxy is terminated.
    TNullable<i64> JobProxyMemoryOvercommitLimit_;

    const NConcurrency::TActionQueuePtr JobThread_;
    const NConcurrency::TActionQueuePtr ControlThread_;

    NLogging::TLogger Logger;

    const TJobProxyConfigPtr Config_ = New<TJobProxyConfig>();

    NRpc::IServerPtr RpcServer_;

    std::unique_ptr<NExecAgent::TSupervisorServiceProxy> SupervisorProxy_;
    
    NApi::IClientPtr Client_;


    NNodeTrackerClient::TNodeDirectoryPtr InputNodeDirectory_;
    NNodeTrackerClient::TNodeDirectoryPtr AuxNodeDirectory_;

    std::atomic<i64> MaxMemoryUsage_ = {0};

    int CpuLimit_;

    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    NConcurrency::TPeriodicExecutorPtr MemoryWatchdogExecutor_;

    IJobPtr Job_;

    NJobTrackerClient::NProto::TJobSpec JobSpec_;
    NNodeTrackerClient::NProto::TNodeResources ResourceUsage_;

    void ValidateJobId(const NJobTrackerClient::TJobId& jobId);

    NJobTrackerClient::NProto::TJobResult DoRun();
    void SendHeartbeat();
    void OnHeartbeatResponse(const TError& error);

    void RetrieveJobSpec();
    void ReportResult(const NJobTrackerClient::NProto::TJobResult& result);

    NJobTrackerClient::TStatistics GetStatistics() const;

    std::unique_ptr<IUserJobIO> CreateUserJobIO();
    IJobPtr CreateBuiltinJob();

    // IJobHost implementation.
    virtual TJobProxyConfigPtr GetConfig() override;
    virtual const NJobTrackerClient::NProto::TJobSpec& GetJobSpec() const override;

    virtual const NNodeTrackerClient::NProto::TNodeResources& GetResourceUsage() const override;
    virtual void SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& usage) override;
    void OnResourcesUpdated(const TError& error);

    virtual void ReleaseNetwork() override;

    virtual NApi::IClientPtr GetClient() const override;

    virtual NChunkClient::IBlockCachePtr GetBlockCache() const override;

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetInputNodeDirectory() const override;
    virtual NNodeTrackerClient::TNodeDirectoryPtr GetAuxNodeDirectory() const override;

    virtual NLogging::TLogger GetLogger() const override;

    void CheckMemoryUsage();

    void Exit(EJobProxyExitCode exitCode);

};

DEFINE_REFCOUNTED_TYPE(TJobProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
