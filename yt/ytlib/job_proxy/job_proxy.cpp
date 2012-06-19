#include "stdafx.h"

#include "config.h"
#include "job_proxy.h"
#include "user_job.h"
#include "sorted_merge_job.h"
#include "ordered_merge_job.h"
#include "sort_job.h"
#include "partition_job.h"

#include <ytlib/logging/log_manager.h>
#include <ytlib/scheduler/public.h>
#include <ytlib/bus/tcp_client.h>
#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NExecAgent;
using namespace NBus;
using namespace NRpc;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfigPtr config,
    const TJobId& jobId)
    : Config(config)
    , JobId(jobId)
    , Logger(JobProxyLogger)
{
    Logger.AddTag(Sprintf("JobId: %s", ~JobId.ToString()));

    auto client = CreateTcpBusClient(Config->SupervisorConnection);
    auto channel = CreateBusChannel(client, Config->SupervisorRpcTimeout);
    SupervisorProxy.Reset(new TSupervisorServiceProxy(channel));
}

void TJobProxy::SendHeartbeat()
{
    HeartbeatInvoker->ScheduleNext();

    auto req = SupervisorProxy->OnJobProgress();
    *req->mutable_job_id() = JobId.ToProto();
    req->Invoke().Subscribe(BIND(&TJobProxy::OnHeartbeatResponse, MakeWeak(this)));

    LOG_DEBUG("Supervisor heartbeat sent");
}

void TJobProxy::OnHeartbeatResponse(TSupervisorServiceProxy::TRspOnJobProgressPtr rsp)
{
    if (!rsp->IsOK()) {
        // NB: user process is not killed here.
        // Good user processes are supposed to die themselves 
        // when io pipes are closed.
        // Bad processes will die at container shutdown.
        LOG_ERROR("Error sending heartbeat to supervisor\n%s",
            ~rsp->GetError().ToString());

        NLog::TLogManager::Get()->Shutdown();
        // TODO(babenko): extract error code constant
        _exit(122);
    }

    LOG_DEBUG("Successfully reported heartbeat to supervisor")
}

TJobSpec TJobProxy::GetJobSpec()
{
    LOG_INFO("Requesting job spec");
    auto req = SupervisorProxy->GetJobSpec();
    *req->mutable_job_id() = JobId.ToProto();

    auto rsp = req->Invoke().Get();
    if (!rsp->IsOK()) {
        ythrow yexception() << Sprintf("Failed to get job spec\n%s",
            ~rsp->GetError().ToString());
    }

    LOG_INFO("Job spec received\n%s", ~rsp->job_spec().DebugString());

    return rsp->job_spec();
}

void TJobProxy::Run()
{
    HeartbeatInvoker = New<TPeriodicInvoker>(
        TSyncInvoker::Get(),
        BIND(&TJobProxy::SendHeartbeat, MakeWeak(this)), 
        Config->HeartbeatPeriod);
    HeartbeatInvoker->Start();

    try {
        auto jobSpec = GetJobSpec();

        switch (jobSpec.type()) {
            case EJobType::Map: {
                const auto& jobSpecExt = jobSpec.GetExtension(TMapJobSpecExt::map_job_spec_ext);
                Job = new TUserJob(Config, jobSpec, jobSpecExt.mapper_spec());
                break;
            }

            case EJobType::OrderedMerge:
                Job = new TOrderedMergeJob(Config, jobSpec);
                break;

            case EJobType::SortedMerge:
                Job = new TSortedMergeJob(Config, jobSpec);
                break;

            case EJobType::PartitionSort:
            case EJobType::SimpleSort:
                Job = new TSortJob(Config, jobSpec);
                break;

            case EJobType::Partition:
                Job = new TPartitionJob(Config, jobSpec);
                break;

            default:
                YUNREACHABLE();
        }

        auto result = Job->Run();
        ReportResult(result);

    } catch (const std::exception& ex) {
        LOG_ERROR("Job failed\n%s", ex.what());

        TJobResult result;
        result.mutable_error()->set_code(TError::Fail);
        result.mutable_error()->set_message(ex.what());
        ReportResult(result);
    }
}

void TJobProxy::ReportResult(const NScheduler::NProto::TJobResult& result)
{
    HeartbeatInvoker->Stop();

    auto req = SupervisorProxy->OnJobFinished();
    *req->mutable_result() = result;
    *req->mutable_job_id() = JobId.ToProto();

    auto rsp = req->Invoke().Get();
    if (!rsp->IsOK()) {
        LOG_ERROR("Failed to report job result");

        NLog::TLogManager::Get()->Shutdown();
        // TODO(babenko): extract error code constant
        _exit(123);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
