#pragma once

#include "public.h"
#include "private.h"
#include "job.h"

#include <core/concurrency/public.h>

#include <core/logging/log.h>

#include <server/job_agent/public.h>

#include <server/exec_agent/supervisor_service_proxy.h>

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

private:
    TJobProxyConfigPtr Config_;
    NJobAgent::TJobId JobId_;

    NLog::TLogger Logger;

    std::unique_ptr<NExecAgent::TSupervisorServiceProxy> SupervisorProxy_;

    NRpc::IChannelPtr MasterChannel_;

    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    i64 JobProxyMemoryLimit_;

    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    NConcurrency::TPeriodicExecutorPtr MemoryWatchdogExecutor_;

    IJobPtr Job_;
    NConcurrency::TActionQueuePtr JobThread_;

    NJobTrackerClient::NProto::TJobSpec JobSpec_;
    NNodeTrackerClient::NProto::TNodeResources ResourceUsage_;


    NJobTrackerClient::NProto::TJobResult DoRun();
    void SendHeartbeat();
    void OnHeartbeatResponse(NExecAgent::TSupervisorServiceProxy::TRspOnJobProgressPtr rsp);

    void RetrieveJobSpec();
    void ReportResult(const NJobTrackerClient::NProto::TJobResult& result);

    std::unique_ptr<IUserJobIO> CreateUserJobIO();
    IJobPtr CreateBuiltinJob();

    // IJobHost implementation.
    virtual TJobProxyConfigPtr GetConfig() override;
    virtual const NJobTrackerClient::NProto::TJobSpec& GetJobSpec() const override;

    virtual const NNodeTrackerClient::NProto::TNodeResources& GetResourceUsage() const override;
    virtual void SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& usage) override;
    void OnResourcesUpdated(NExecAgent::TSupervisorServiceProxy::TRspUpdateResourceUsagePtr rsp);

    virtual void ReleaseNetwork() override;

    virtual NRpc::IChannelPtr GetMasterChannel() const override;

    virtual NChunkClient::IBlockCachePtr GetCompressedBlockCache() const override;
    virtual NChunkClient::IBlockCachePtr GetUncompressedBlockCache() const override;

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const override;

    virtual NLog::TLogger GetLogger() const override;

    void CheckMemoryUsage();

    static void Exit(EJobProxyExitCode exitCode);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
