#include "stdafx.h"

#include "config.h"
#include "job_proxy.h"
#include "user_job.h"
#include "sorted_merge_job.h"
#include "ordered_merge_job.h"
#include "sort_job.h"
#include "partition_job.h"

#include <ytlib/rpc/channel.h>
#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NExecAgent;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfigPtr config,
    const TJobId& jobId)
    : Config(config)
    , Proxy(NRpc::CreateBusChannel(config->ExecAgentAddress))
    , JobId(jobId)
    , Logger(JobProxyLogger)
{
    Logger.AddTag(Sprintf("JobId: %s", ~JobId.ToString()));
    Proxy.SetDefaultTimeout(config->RpcTimeout);
}

void TJobProxy::SendHeartbeat()
{
    HeartbeatInvoker->ScheduleNext();

    auto req = Proxy.OnJobProgress();
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

        // TODO(babenko): extract error code constant
        _exit(122);
    }

    LOG_DEBUG("Successfully reported heartbeat to supervisor")
}

TJobSpec TJobProxy::GetJobSpec()
{
    LOG_INFO("Requesting job spec");
    auto req = Proxy.GetJobSpec();
    *req->mutable_job_id() = JobId.ToProto();

    auto rsp = req->Invoke().Get();

    if (!rsp->IsOK()) {
        ythrow yexception() << Sprintf("Failed to get job spec\n%s",
            ~rsp->GetError().ToString());
    }

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
            case EJobType::Map:
                YASSERT(jobSpec.HasExtension(TUserJobSpec::user_job_spec));
                Job = new TUserJob(Config, jobSpec);
                break;

            case EJobType::OrderedMerge:
                Job = new TOrderedMergeJob(
                    Config->JobIO, 
                    Config->Masters, 
                    jobSpec.GetExtension(TMergeJobSpec::merge_job_spec));
                break;

            case EJobType::SortedMerge:
                Job = new TSortedMergeJob(
                    Config->JobIO, 
                    Config->Masters, 
                    jobSpec.GetExtension(TMergeJobSpec::merge_job_spec));
                break;

            case EJobType::Sort:
                Job = new TSortJob(
                    Config->JobIO, 
                    Config->Masters, 
                    jobSpec.GetExtension(TSortJobSpec::sort_job_spec));
                break;

            case EJobType::Partition:
                Job = new TPartitionJob(
                    Config->JobIO, 
                    Config->Masters, 
                    jobSpec.GetExtension(TPartitionJobSpec::partition_job_spec));
                break;

            default:
                YUNREACHABLE();
        }

        auto result = Job->Run();
        ReportResult(result);

    } catch (const std::exception& ex) {
        LOG_WARNING("Job failed\n%s", ex.what());

        TJobResult result;
        result.mutable_error()->set_code(TError::Fail);
        result.mutable_error()->set_message(ex.what());
        ReportResult(result);
    }
}

void TJobProxy::ReportResult(const NScheduler::NProto::TJobResult& result)
{
    HeartbeatInvoker->Stop();

    auto req = Proxy.OnJobFinished();
    *req->mutable_result() = result;
    *req->mutable_job_id() = JobId.ToProto();

    auto rsp = req->Invoke().Get();
    if (!rsp->IsOK()) {
        LOG_ERROR("Failed to report job result");

        // TODO(babenko): extract error code constant
        _exit(123);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
