#pragma once

#include "public.h"
#include "private.h"
#include "job.h"

#include <server/job_agent/public.h>

#include <server/exec_agent/supervisor_service_proxy.h>

#include <ytlib/api/public.h>

#include <ytlib/job_tracker_client/public.h>
#include <ytlib/job_tracker_client/statistics.h>

#include <core/concurrency/public.h>

#include <core/logging/log.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public IJobHost
{
public:
    TJobProxy(
        TJobProxyConfigPtr config,
        const NJobAgent::TJobId& jobId);

    //! Runs the job. Blocks until the job is complete.
    void Run();

    IInvokerPtr GetControlInvoker() const;

    virtual std::vector<NChunkClient::TChunkId> DumpInputContext(const NJobTrackerClient::TJobId& jobId) override;
    virtual NYTree::TYsonString Strace(const NJobTrackerClient::TJobId& jobId) override;

private:
    TJobProxyConfigPtr Config_;
    NJobAgent::TJobId JobId_;

    NLogging::TLogger Logger;

    NRpc::IServerPtr RpcServer;

    std::unique_ptr<NExecAgent::TSupervisorServiceProxy> SupervisorProxy_;
    
    NApi::IClientPtr Client_;

    std::atomic<bool> EnableJobProxyMemoryControl_ = { false };

    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    i64 JobProxyMemoryLimit_;
    std::atomic<i64> MaxMemoryUsage_ = { 0 };

    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    NConcurrency::TPeriodicExecutorPtr MemoryWatchdogExecutor_;

    IJobPtr Job_;
    NConcurrency::TActionQueuePtr JobThread_;
    NConcurrency::TActionQueuePtr ControlThread_;

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

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const override;

    virtual NLogging::TLogger GetLogger() const override;

    void CheckMemoryUsage();

    static void Exit(EJobProxyExitCode exitCode);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
