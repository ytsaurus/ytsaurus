#include "supervisor_service.h"

#include "bootstrap.h"
#include "private.h"
#include "job.h"

#include <yt/yt/server/lib/exec_agent/supervisor_service_proxy.h>

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/data_node/bootstrap.h>

#include <yt/yt/server/node/job_agent/job_controller.h>
#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/server/lib/job_proxy/config.h>
#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NExecAgent {

using namespace NJobAgent;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NYson;
using namespace NConcurrency;
using namespace NJobProxy;
using namespace NCoreDump;

////////////////////////////////////////////////////////////////////////////////

class TSupervisorService
    : public NRpc::TServiceBase
{
public:
    explicit TSupervisorService(IBootstrap* bootstrap)
        : NRpc::TServiceBase(
            bootstrap->GetJobInvoker(),
            TSupervisorServiceProxy::GetDescriptor(),
            ExecAgentLogger)
        , Bootstrap_(bootstrap)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(GetJobSpec)
                .SetResponseCodec(NCompression::ECodec::Lz4)
                .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobProxySpawned));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PrepareArtifact));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(OnArtifactPreparationFailed));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(OnArtifactsPrepared));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobFinished));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobProgress));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobPrepared));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateResourceUsage));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ThrottleJob)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollThrottlingRequest)
            .SetQueueSizeLimit(5000)
            .SetConcurrencyLimit(5000));
    }

private:
    IBootstrap* const Bootstrap_;

    THashMap<TGuid, TFuture<void>> OutstandingThrottlingRequests_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);


    TGuid RegisterThrottlingRequest(TFuture<void> future)
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        auto id = TGuid::Create();
        YT_VERIFY(OutstandingThrottlingRequests_.emplace(id, future).second);
        // Remove future from outstanding requests after it was set + timeout.
        future.Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& /* error */) {
            TDelayedExecutor::Submit(
                BIND(&TSupervisorService::EvictThrottlingRequest, this_, id).Via(Bootstrap_->GetJobInvoker()),
                Bootstrap_->GetConfig()->JobThrottler->MaxBackoffTime * 2);
        }));
        return id;
    }

    void EvictThrottlingRequest(TGuid id)
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        YT_LOG_DEBUG("Outstanding throttling request evicted (ThrottlingRequestId: %v)",
            id);
        YT_VERIFY(OutstandingThrottlingRequests_.erase(id) == 1);
    }

    TFuture<void> FindThrottlingRequest(TGuid id)
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        auto it = OutstandingThrottlingRequests_.find(id);
        return it == OutstandingThrottlingRequests_.end() ? TFuture<void>() : it->second;
    }

    TFuture<void> GetThrottlingRequestOrThrow(TGuid id)
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        auto future = FindThrottlingRequest(id);
        if (!future) {
            THROW_ERROR_EXCEPTION("Unknown throttling request %v", id);
        }
        return future;
    }

    const IThroughputThrottlerPtr& GetJobThrottler(EJobThrottlerType throttlerType)
    {

        switch (throttlerType) {
            case EJobThrottlerType::InBandwidth:
                return Bootstrap_->GetThrottler(EExecNodeThrottlerKind::JobIn);
            case EJobThrottlerType::OutBandwidth:
                return Bootstrap_->GetThrottler(EExecNodeThrottlerKind::JobOut);
            case EJobThrottlerType::OutRps:
                return Bootstrap_->GetReadRpsOutThrottler();
            default:
                THROW_ERROR_EXCEPTION("Unknown throttler type %Qlv", throttlerType);
        }
    }


    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpec)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        const auto& jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        auto jobPhase = job->GetPhase();
        if (jobPhase != EJobPhase::SpawningJobProxy) {
            THROW_ERROR_EXCEPTION("Cannot fetch job spec; job is in wrong phase")
                << TErrorAttribute("expected_phase", EJobPhase::SpawningJobProxy)
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

    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobProxySpawned)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        const auto& jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);
        job->OnJobProxySpawned();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PrepareArtifact)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto artifactName = request->artifact_name();
        auto pipePath = request->pipe_path();

        context->SetRequestInfo("JobId: %v, ArtifactName: %v",
            jobId,
            artifactName);

        const auto& jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        job->PrepareArtifact(artifactName, pipePath);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, OnArtifactPreparationFailed)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto artifactName = request->artifact_name();
        auto artifactPath = request->artifact_path();
        auto error = FromProto<TError>(request->error());

        context->SetRequestInfo("JobId: %v, ArtifactName: %v",
            jobId,
            artifactName);

        const auto& jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        job->OnArtifactPreparationFailed(artifactName, artifactPath, error);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, OnArtifactsPrepared)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        const auto& jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);
        job->OnArtifactsPrepared();

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

        const auto& jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        job->SetResult(result);

        auto jobReport = TNodeJobReport().Error(error);
        if (request->has_statistics()) {
            auto ysonStatistics = TYsonString(request->statistics());
            job->SetStatistics(ysonStatistics);
            jobReport.SetStatistics(ysonStatistics);
        }
        // COMPAT(ignat): migrate to new fields (node_start_time, node_finish_time)
        if (request->has_start_time()) {
            jobReport.SetStartTime(FromProto<TInstant>(request->start_time()));
        }
        if (request->has_finish_time()) {
            jobReport.SetFinishTime(FromProto<TInstant>(request->finish_time()));
        }
        job->SetCoreInfos(FromProto<TCoreInfos>(request->core_infos()));
        job->HandleJobReport(std::move(jobReport));

        if (request->has_job_stderr()) {
            job->SetStderr(request->job_stderr());
        }

        if (request->has_fail_context()) {
            job->SetFailContext(request->fail_context());
        }

        if (request->has_profile_type() && request->has_profile_blob() && request->has_profiling_probability()) {
            job->SetProfile({request->profile_type(), request->profile_blob(), request->profiling_probability()});
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobPrepared)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        const auto& jobController = Bootstrap_->GetJobController();
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
            NYTree::ConvertToYsonString(statistics, EYsonFormat::Text).AsStringBuf(),
            stderrSize);

        const auto& jobController = Bootstrap_->GetJobController();
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

        const auto& jobController = Bootstrap_->GetJobController();
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
        auto throttlerType = CheckedEnumCast<EJobThrottlerType>(request->throttler_type());
        auto count = request->count();
        auto descriptor = FromProto<TWorkloadDescriptor>(request->workload_descriptor());
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("ThrottlerType: %v, Count: %v, JobId: %v, WorkloadDescriptor: %v",
            throttlerType,
            count,
            jobId,
            descriptor);

        const auto& throttler = GetJobThrottler(throttlerType);
        auto future = throttler->Throttle(count);
        if (auto optionalResult = future.TryGet()) {
            optionalResult->ThrowOnError();
        } else {
            auto throttlingRequestId = RegisterThrottlingRequest(future);

            ToProto(response->mutable_throttling_request_id(), throttlingRequestId);
            context->SetResponseInfo("ThrottlingRequestId: %v", throttlingRequestId);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PollThrottlingRequest)
    {
        auto throttlingRequestId = FromProto<TGuid>(request->throttling_request_id());

        context->SetRequestInfo("ThrottlingRequestId: %v", throttlingRequestId);

        auto future = GetThrottlingRequestOrThrow(throttlingRequestId);
        auto optionalResult = future.TryGet();
        if (optionalResult) {
            optionalResult->ThrowOnError();
        }
        response->set_completed(optionalResult.has_value());
        context->SetResponseInfo("Completed: %v", response->completed());
        context->Reply();
    }
};

NRpc::IServicePtr CreateSupervisorService(IBootstrap* bootstrap)
{
    return New<TSupervisorService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
