#pragma once

#include "public.h"
#include "private.h"
#include "pipes.h"
#include "job.h"

#include <ytlib/scheduler/public.h>

#include <ytlib/misc/periodic_invoker.h>

#include <ytlib/logging/tagged_logger.h>

#include <server/exec_agent/public.h>
#include <server/exec_agent/supervisor_service_proxy.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public TRefCounted
    , public IJobHost
{
public:
    TJobProxy(
        TJobProxyConfigPtr config,
        const NScheduler::TJobId& jobId);

    //! Runs the job. Blocks until the job is complete.
    void Run();

private:
    void SendHeartbeat();
    void OnHeartbeatResponse(NExecAgent::TSupervisorServiceProxy::TRspOnJobProgressPtr rsp);

    void RetrieveJobSpec();
    void ReportResult(const NScheduler::NProto::TJobResult& result);

    TJobProxyConfigPtr Config;
    NScheduler::TJobId JobId;
    
    NLog::TTaggedLogger Logger;

    THolder<NExecAgent::TSupervisorServiceProxy> SupervisorProxy;

    NRpc::IChannelPtr MasterChannel;

    NChunkClient::IBlockCachePtr BlockCache;
    
    TJobPtr Job;

    TPeriodicInvokerPtr HeartbeatInvoker;

    NScheduler::NProto::TJobSpec JobSpec;
    NScheduler::NProto::TNodeResources ResourceUsage;

    // IJobHost implementation.
    virtual TJobProxyConfigPtr GetConfig() override;
    virtual const NScheduler::NProto::TJobSpec& GetJobSpec() const override;

    virtual const NScheduler::NProto::TNodeResources& GetResourceUsage() const override;
    virtual void SetResourceUsage(const NScheduler::NProto::TNodeResources& usage) override;
    void OnResourcesUpdated(NExecAgent::TSupervisorServiceProxy::TRspUpdateResourceUsagePtr rsp);

    virtual void ReleaseNetwork() override;

    virtual NRpc::IChannelPtr GetMasterChannel() const override;

    virtual NChunkClient::IBlockCachePtr GetBlockCache() const override;

    TFuture<void> GetFailedChunks(std::vector<NChunkClient::TChunkId>* failedChunks);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
