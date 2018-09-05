#include "supervisor_service.h"
#include "private.h"
#include "job.h"
#include "supervisor_service_proxy.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/job_agent/job_controller.h>
#include <yt/server/job_agent/public.h>

#include <yt/server/job_proxy/job_throttler.h>
#include <yt/server/job_proxy/config.h>

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
    RegisterMethod(RPC_SERVICE_METHOD_DESC(ThrottleJob)
       .SetMaxQueueSize(5000)
       .SetMaxConcurrency(5000));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(PollThrottlingRequest)
       .SetMaxQueueSize(5000)
       .SetMaxConcurrency(5000));
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

    if (request->has_job_stderr()) {
        job->SetStderr(request->job_stderr());
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

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, ThrottleJob)
{
    auto throttlerType = static_cast<EJobThrottlerType>(request->throttler_type());
    auto count = request->count();
    auto descriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());
    auto jobId = FromProto<TJobId>(request->job_id());

    context->SetRequestInfo("ThrottlerType: %v, Count: %v, JobId: %v, WorkloadDescriptor: %v",
        throttlerType,
        count,
        jobId,
        descriptor);

    IThroughputThrottlerPtr throttler;
    switch (throttlerType) {
        case EJobThrottlerType::InBandwidth:
            throttler = Bootstrap->GetInThrottler(descriptor);
            break;
        case EJobThrottlerType::OutBandwidth:
            throttler = Bootstrap->GetOutThrottler(descriptor);
            break;
        case EJobThrottlerType::OutRps:
            throttler = Bootstrap->GetReadRpsOutThrottler();
            break;
        default:
            Y_UNREACHABLE();
    }

    auto future = throttler->Throttle(count);
    if (!future.IsSet()) {
        auto throttlingRequestId = TGuid::Create();
        OutstandingThrottlingRequests_.insert(std::make_pair(throttlingRequestId, future));

        // Remove future from outstanding requests after it was set + timeout.
        future.Subscribe(BIND([throttlingRequestId, this, this_ = MakeStrong(this)] (const TError& /* error */) {
            TDelayedExecutor::Submit(BIND([throttlingRequestId, this_] () {
                    const auto& Logger = ExecAgentLogger;

                    LOG_DEBUG("Evict outstanding throttling request (ThrottlingRequestId: %v)", throttlingRequestId);
                    YCHECK(this_->OutstandingThrottlingRequests_.erase(throttlingRequestId) == 1);
                }).Via(this_->Bootstrap->GetControlInvoker()),
                Bootstrap->GetConfig()->JobThrottler->MaxBackoffTime * 2);
        }));

        ToProto(response->mutable_throttling_request_id(), throttlingRequestId);
        context->SetResponseInfo("ThrottlingRequestId: %v", throttlingRequestId);
    } else {
        future
            .Get()
            .ThrowOnError();
    }

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, PollThrottlingRequest)
{
    auto throttlingRequestId = FromProto<TGuid>(request->throttling_request_id());

    context->SetRequestInfo("ThrottlingRequestId: %v", throttlingRequestId);

    auto it = OutstandingThrottlingRequests_.find(throttlingRequestId);
    if (it == OutstandingThrottlingRequests_.end()) {
        context->Reply(TError("Unknown throttling request")
            << TErrorAttribute("throttling_request_id", throttlingRequestId));
    } else {
        response->set_completed(it->second.IsSet());
        context->SetResponseInfo("Completed: %v", response->completed());
        if (response->completed()) {
            it->second
                .Get()
                .ThrowOnError();
        }
        context->Reply();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
