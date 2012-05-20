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
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfigPtr config,
    const TJobId& jobId)
    : Config(config)
    , Proxy(NRpc::CreateBusChannel(config->ExecAgentAddress))
    , JobId(jobId)
{
    Proxy.SetDefaultTimeout(config->RpcTimeout);
}

void TJobProxy::SendHeartbeat()
{
    HeartbeatInvoker->ScheduleNext();

    auto req = Proxy.OnJobProgress();
    *req->mutable_job_id() = JobId.ToProto();

    auto rsp = req->Invoke().Get();

    if (!rsp->IsOK()) {
        // NB: user process is not killed here.
        // Good user processes are supposed to die themselves 
        // when io pipes are closed.
        // Bad processes will die at container shutdown.
        LOG_ERROR("Failed to report progress for job %s", ~JobId.ToString());
        // TODO(babenko): extract error code constant
        _exit(122);
    }
}

TJobSpec TJobProxy::GetJobSpec()
{
    LOG_DEBUG("Requesting spec for job %s", ~JobId.ToString());
    auto req = Proxy.GetJobSpec();
    *req->mutable_job_id() = JobId.ToProto();

    auto rsp = req->Invoke().Get();

    if (!rsp->IsOK()) {
        ythrow yexception() << Sprintf("Failed to get job spec (JobId: %s)\n%s",
            ~JobId.ToString(),
            ~rsp->GetError().ToString());
    }

    return rsp->job_spec();
}

void TJobProxy::Start()
{
    HeartbeatInvoker = New<TPeriodicInvoker>(
        TSyncInvoker::Get(),
        BIND(&TJobProxy::SendHeartbeat, this), 
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
        LOG_DEBUG(
            "Job failed (JobId: %s, error: %s)", 
            ~JobId.ToString(),
            ex.what());

        TJobResult result;
        result.mutable_error()->set_code(TError::Fail);
        result.mutable_error()->set_message(ex.what());
        ReportResult(result);
    }
}

void TJobProxy::ReportResult(
    const NScheduler::NProto::TJobResult& result)
{
    HeartbeatInvoker->Stop();

    auto req = Proxy.OnJobFinished();
    *(req->mutable_result()) = result;
    *(req->mutable_job_id()) = JobId.ToProto();

    auto rsp = req->Invoke().Get();
    if (!rsp->IsOK()) {
        LOG_ERROR("Failed to report result for job %s", ~JobId.ToString());
        // TODO(babenko): extract error code constant
        _exit(123);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
