#include "supervisor_service.h"
#include "private.h"
#include "job.h"
#include "supervisor_service_proxy.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/job_agent/job_controller.h>
#include <yt/server/job_agent/public.h>

#include <yt/server/job_proxy/job_bandwidth_throttler.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/client/misc/workload.h>

#include <yt/core/concurrency/scheduler-inl.h>

namespace NYT {
namespace NExecAgent {

using namespace NJobAgent;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NYson;
using namespace NConcurrency;
using namespace NJobProxy;

////////////////////////////////////////////////////////////////////////////////

TSupervisorService::TSupervisorService(TBootstrap* bootstrap)
    : NRpc::TServiceBase(
        bootstrap->GetControlInvoker(),
        TSupervisorServiceProxy::GetDescriptor(),
        ExecAgentLogger)
    , Bootstrap(bootstrap)
{
    RegisterMethod(
        RPC_SERVICE_METHOD_DESC(GetJobSpec)
        .SetResponseCodec(NCompression::ECodec::Lz4)
        .SetHeavy(true));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobFinished));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobProgress));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobPrepared));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateResourceUsage));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(ThrottleBandwidth));
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, GetJobSpec)
{
    auto jobId = FromProto<TJobId>(request->job_id());
    context->SetRequestInfo("JobId: %v", jobId);

    auto jobController = Bootstrap->GetJobController();
    auto job = jobController->GetJobOrThrow(jobId);

    *response->mutable_job_spec() = job->GetSpec();
    auto resources = job->GetResourceUsage();

    auto* jobProxyResources = response->mutable_resource_usage();
    jobProxyResources->set_cpu(resources.cpu());
    jobProxyResources->set_memory(resources.user_memory());
    jobProxyResources->set_network(resources.network());

    ToProto(response->mutable_ports(), job->GetPorts());

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, OnJobFinished)
{
    auto jobId = FromProto<TJobId>(request->job_id());
    const auto& result = request->result();
    auto error = FromProto<TError>(result.error());
    context->SetRequestInfo("JobId: %v, Error: %v",
        jobId,
        error);

    auto jobController = Bootstrap->GetJobController();
    auto job = jobController->GetJobOrThrow(jobId);

    job->SetResult(result);

    auto statistics = TJobStatistics().Error(error);
    if (request->has_statistics()) {
        auto ysonStatistics = TYsonString(request->statistics());
        job->SetStatistics(ysonStatistics);
        statistics.SetStatistics(ysonStatistics);
    }
    // TODO(ignat): migrate to new fields (node_start_time, node_finish_time)
    if (request->has_start_time()) {
        statistics.SetStartTime(FromProto<TInstant>(request->start_time()));
    }
    if (request->has_finish_time()) {
        statistics.SetFinishTime(FromProto<TInstant>(request->finish_time()));
    }
    job->ReportStatistics(std::move(statistics));

    if (request->has_stderr()) {
        job->SetStderr(request->stderr());
    }

    if (request->has_fail_context()) {
        job->SetFailContext(request->fail_context());
    }

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, OnJobProgress)
{
    auto jobId = FromProto<TJobId>(request->job_id());
    double progress = request->progress();
    auto statistics = TYsonString(request->statistics());
    auto stderrSize = request->stderr_size();

    context->SetRequestInfo("JobId: %v, Progress: %lf, Statistics: %v, StderrSize: %v",
        jobId,
        progress,
        NYTree::ConvertToYsonString(statistics, EYsonFormat::Text).GetData(),
        stderrSize);

    auto jobController = Bootstrap->GetJobController();
    auto job = jobController->GetJobOrThrow(jobId);

    job->SetProgress(progress);
    job->SetStatistics(statistics);
    job->SetStderrSize(stderrSize);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, OnJobPrepared)
{
    auto jobId = FromProto<TJobId>(request->job_id());

    context->SetRequestInfo("JobId: %v", jobId);

    auto jobController = Bootstrap->GetJobController();
    auto job = jobController->GetJobOrThrow(jobId);

    job->OnJobPrepared();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, UpdateResourceUsage)
{
    auto jobId = FromProto<TJobId>(request->job_id());
    const auto& jobProxyResourceUsage = request->resource_usage();

    context->SetRequestInfo("JobId: %v, JobProxyResourceUsage: {Cpu: %v, Memory %v, Network: %v}",
        jobId,
        jobProxyResourceUsage.cpu(),
        jobProxyResourceUsage.memory(),
        jobProxyResourceUsage.network());

    auto jobController = Bootstrap->GetJobController();
    auto job = jobController->GetJobOrThrow(jobId);

    auto resourceUsage = job->GetResourceUsage();
    resourceUsage.set_user_memory(jobProxyResourceUsage.memory());
    resourceUsage.set_cpu(jobProxyResourceUsage.cpu());
    resourceUsage.set_network(jobProxyResourceUsage.network());

    job->SetResourceUsage(resourceUsage);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, ThrottleBandwidth)
{
    auto direction = static_cast<EJobBandwidthDirection>(request->direction());
    auto byteCount = request->byte_count();
    auto descriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());
    auto jobId = FromProto<TJobId>(request->job_id());

    context->SetRequestInfo("Direction: %v, Count: %v, JobId: %v, WorkloadDescriptor: %v",
        direction,
        byteCount,
        jobId,
        descriptor);

    IThroughputThrottlerPtr throttler;
    switch (direction) {
        case EJobBandwidthDirection::In:
            throttler = Bootstrap->GetInThrottler(descriptor);
            break;
        case EJobBandwidthDirection::Out:
            throttler = Bootstrap->GetOutThrottler(descriptor);
            break;
        default:
            Y_UNREACHABLE();
    }

    WaitFor(throttler->Throttle(byteCount))
        .ThrowOnError();

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
