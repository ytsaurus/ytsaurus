#include "stdafx.h"

#include "config.h"
#include "job_proxy.h"
#include "user_job.h"
#include "sorted_merge_job.h"
#include "ordered_merge_job.h"
#include "simple_sort_job.h"
#include "partition_sort_job.h"
#include "partition_job.h"
#include "map_job_io.h"
#include "reduce_job_io.h"

#include <ytlib/actions/invoker_util.h>
#include <ytlib/logging/log_manager.h>
#include <ytlib/scheduler/public.h>
#include <ytlib/bus/tcp_client.h>
#include <ytlib/rpc/bus_channel.h>

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
    *req->mutable_progress() = Job->GetProgress();
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

void TJobProxy::RetrieveJobSpec()
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

    JobSpec = rsp->job_spec();
    ResourceUtilization = JobSpec.resource_utilization();
}

void TJobProxy::Run()
{
    HeartbeatInvoker = New<TPeriodicInvoker>(
        TSyncInvoker::Get(),
        BIND(&TJobProxy::SendHeartbeat, MakeWeak(this)), 
        Config->HeartbeatPeriod);

    try {
        RetrieveJobSpec();

        const auto& jobSpec = GetJobSpec();
        auto jobType = EJobType(jobSpec.type());
        NYT::NThread::SetCurrentThreadName(~jobType.ToString());

        switch (jobType) {
            case EJobType::Map: {
                const auto& jobSpecExt = jobSpec.GetExtension(TMapJobSpecExt::map_job_spec_ext);
                TAutoPtr<TUserJobIO> userJobIO = new TMapJobIO(Config->JobIO, Config->Masters, jobSpec);
                Job = CreateUserJob(this, jobSpecExt.mapper_spec(), userJobIO);
                break;
            }

            case EJobType::Reduce: {
                const auto& jobSpecExt = jobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                TAutoPtr<TUserJobIO> userJobIO = new TReduceJobIO(Config->JobIO, Config->Masters, jobSpec);
                Job = CreateUserJob(this, jobSpecExt.reducer_spec(), userJobIO);
                break;
            }

            case EJobType::OrderedMerge:
            case EJobType::UnorderedMerge:
                Job = CreateOrderedMergeJob(this);
                break;

            case EJobType::SortedMerge:
                Job = CreateSortedMergeJob(this);
                break;

            case EJobType::PartitionSort:
                Job = CreatePartitionSortJob(this);
                break;

            case EJobType::SimpleSort:
                Job = CreateSimpleSortJob(this);
                break;

            case EJobType::Partition:
                Job = CreatePartitionJob(this);
                break;

            default:
                YUNREACHABLE();
        }

        HeartbeatInvoker->Start();
        auto result = Job->Run();
        HeartbeatInvoker->Stop();

        ReportResult(result);
    } catch (const std::exception& ex) {
        LOG_ERROR("Job failed\n%s", ex.what());

        TJobResult result;
        result.mutable_error()->set_code(TError::Fail);
        result.mutable_error()->set_message(ex.what());
        ReportResult(result);
    }
}

void TJobProxy::ReportResult(const TJobResult& result)
{
    HeartbeatInvoker->Stop();

    auto req = SupervisorProxy->OnJobFinished();
    *req->mutable_job_id() = JobId.ToProto();
    *req->mutable_result() = result;

    auto rsp = req->Invoke().Get();
    if (!rsp->IsOK()) {
        LOG_ERROR("Failed to report job result");

        NLog::TLogManager::Get()->Shutdown();
        // TODO(babenko): extract error code constant
        _exit(123);
    }
}

TJobProxyConfigPtr TJobProxy::GetConfig()
{
    return Config;
}

const TJobSpec& TJobProxy::GetJobSpec()
{
    return JobSpec;
}

TNodeResources TJobProxy::GetResourceUtilization()
{
    return ResourceUtilization;
}

void TJobProxy::SetResourceUtilization(const TNodeResources& utilization) 
{
    ResourceUtilization = utilization;

    // Fire-and-forget.
    auto req = SupervisorProxy->OnResourceUtilizationSet();
    *req->mutable_job_id() = JobId.ToProto();
    *req->mutable_utilization() = ResourceUtilization;
    req->Invoke();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
