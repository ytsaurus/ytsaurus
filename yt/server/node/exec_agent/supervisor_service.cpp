#include "supervisor_service.h"
#include "private.h"
#include "job.h"

#include <yt/server/lib/exec_agent/supervisor_service_proxy.h>

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/server/node/job_agent/job_controller.h>
#include <yt/server/node/job_agent/public.h>

#include <yt/server/lib/job_proxy/config.h>

#include <yt/server/job_proxy/job_throttler.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/client/misc/workload.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT::NExecAgent {

using namespace NJobAgent;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NYson;
using namespace NConcurrency;
using namespace NJobProxy;

////////////////////////////////////////////////////////////////////////////////

class TSupervisorService
    : public NRpc::TServiceBase
{
public:
    explicit TSupervisorService(NCellNode::TBootstrap* bootstrap)
        : NRpc::TServiceBase(
            bootstrap->GetControlInvoker(),
            TSupervisorServiceProxy::GetDescriptor(),
            ExecAgentLogger)
        , Bootstrap_(bootstrap)
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
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000)
            .SetInvoker(Bootstrap_->GetJobThrottlerInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollThrottlingRequest)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000)
            .SetInvoker(Bootstrap_->GetJobThrottlerInvoker()));
    }

private:
    NCellNode::TBootstrap* const Bootstrap_;

    THashMap<TGuid, TFuture<void>> OutstandingThrottlingRequests_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThrottlerThread);


    TGuid RegisterThrottlingRequest(TFuture<void> future)
    {
        VERIFY_THREAD_AFFINITY(JobThrottlerThread);
        auto id = TGuid::Create();
        YCHECK(OutstandingThrottlingRequests_.insert(std::make_pair(id, future)).second);
        // Remove future from outstanding requests after it was set + timeout.
        future.Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& /* error */) {
            TDelayedExecutor::Submit(
                BIND(&TSupervisorService::EvictThrottlingRequest, this_, id).Via(Bootstrap_->GetJobThrottlerInvoker()),
                Bootstrap_->GetConfig()->JobThrottler->MaxBackoffTime * 2);
        }));
        return id;
    }

    void EvictThrottlingRequest(TGuid id)
    {
        VERIFY_THREAD_AFFINITY(JobThrottlerThread);
        YT_LOG_DEBUG("Outstanding throttling request evicted (ThrottlingRequestId: %v)",
            id);
        YCHECK(OutstandingThrottlingRequests_.erase(id) == 1);
    }

    TFuture<void> FindThrottlingRequest(TGuid id)
    {
        VERIFY_THREAD_AFFINITY(JobThrottlerThread);
        auto it = OutstandingThrottlingRequests_.find(id);
        return it == OutstandingThrottlingRequests_.end() ? TFuture<void>() : it->second;
    }

    TFuture<void> GetThrottlingRequestOrThrow(TGuid id)
    {
        VERIFY_THREAD_AFFINITY(JobThrottlerThread);
        auto future = FindThrottlingRequest(id);
        if (!future) {
            THROW_ERROR_EXCEPTION("Unknown throttling request %v", id);
        }
        return future;
    }


    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpec)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        auto jobPhase = job->GetPhase();
        if (jobPhase != EJobPhase::PreparingProxy) {
            THROW_ERROR_EXCEPTION("Cannot fetch job spec; job is in wrong phase")
                  << TErrorAttribute("expected_phase", EJobPhase::PreparingProxy)
                  << TErrorAttribute("actual_phase", jobPhase);
        }

        *response->mutable_job_spec() = job->GetSpec();
        auto resources = job->GetResourceUsage();

        auto* jobProxyResources = response->mutable_resource_usage();
        jobProxyResources->set_cpu(resources.cpu());
        jobProxyResources->set_memory(resources.user_memory());
        jobProxyResources->set_network(resources.network());

        ToProto(response->mutable_ports(), job->GetPorts());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobFinished)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        const auto& result = request->result();
        auto error = FromProto<TError>(result.error());
        context->SetRequestInfo("JobId: %v, Error: %v",
            jobId,
            error);

        auto jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        job->SetResult(result);

        auto statistics = TJobStatistics().Error(error);
        if (request->has_statistics()) {
            auto ysonStatistics = TYsonString(request->statistics());
            job->SetStatistics(ysonStatistics);
            statistics.SetStatistics(ysonStatistics);
        }
        // COMPAT(ignat): migrate to new fields (node_start_time, node_finish_time)
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

        if (request->has_profile_type() && request->has_profile_blob()) {
            job->SetProfile({request->profile_type(), request->profile_blob()});
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobPrepared)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        auto jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        job->OnJobPrepared();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobProgress)
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

        auto jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        job->SetProgress(progress);
        job->SetStatistics(statistics);
        job->SetStderrSize(stderrSize);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, UpdateResourceUsage)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        const auto& jobProxyResourceUsage = request->resource_usage();

        context->SetRequestInfo("JobId: %v, JobProxyResourceUsage: {Cpu: %v, Memory %v, Network: %v}",
            jobId,
            jobProxyResourceUsage.cpu(),
            jobProxyResourceUsage.memory(),
            jobProxyResourceUsage.network());

        auto jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        auto resourceUsage = job->GetResourceUsage();
        resourceUsage.set_user_memory(jobProxyResourceUsage.memory());
        resourceUsage.set_cpu(jobProxyResourceUsage.cpu());
        resourceUsage.set_network(jobProxyResourceUsage.network());

        job->SetResourceUsage(resourceUsage);

        if (job->GetPhase() >= EJobPhase::WaitingAbort) {
            THROW_ERROR_EXCEPTION("Cannot update resource usage for job in %Qlv phase", job->GetPhase());
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ThrottleJob)
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
                throttler = Bootstrap_->GetInThrottler(descriptor);
                break;
            case EJobThrottlerType::OutBandwidth:
                throttler = Bootstrap_->GetOutThrottler(descriptor);
                break;
            case EJobThrottlerType::OutRps:
                throttler = Bootstrap_->GetReadRpsOutThrottler();
                break;
            default:
                Y_UNREACHABLE();
        }

        auto future = throttler->Throttle(count);
        if (!future.IsSet()) {
            auto throttlingRequestId = RegisterThrottlingRequest(future);

            ToProto(response->mutable_throttling_request_id(), throttlingRequestId);
            context->SetResponseInfo("ThrottlingRequestId: %v", throttlingRequestId);
        } else {
            future
                .Get()
                .ThrowOnError();
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PollThrottlingRequest)
    {
        auto throttlingRequestId = FromProto<TGuid>(request->throttling_request_id());

        context->SetRequestInfo("ThrottlingRequestId: %v", throttlingRequestId);

        auto future = GetThrottlingRequestOrThrow(throttlingRequestId);
        response->set_completed(future.IsSet());
        context->SetResponseInfo("Completed: %v", response->completed());
        if (response->completed()) {
            future
                .Get()
                .ThrowOnError();
        }
        context->Reply();
    }
};

NRpc::IServicePtr CreateSupervisorService(NCellNode::TBootstrap* bootstrap)
{
    return New<TSupervisorService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
