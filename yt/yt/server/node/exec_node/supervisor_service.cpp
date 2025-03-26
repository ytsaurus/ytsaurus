#include "supervisor_service.h"

#include "bootstrap.h"
#include "job.h"
#include "job_controller.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/supervisor_service_proxy.h>

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/data_node/bootstrap.h>

#include <yt/yt/server/node/job_agent/job_resource_manager.h>
#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/server/lib/job_proxy/config.h>
#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NExecNode {

using namespace NJobAgent;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NYson;
using namespace NConcurrency;
using namespace NJobProxy;
using namespace NCoreDump;
using namespace NObjectClient;
using NChunkClient::NProto::TDataStatistics;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TSupervisorService
    : public NRpc::TServiceBase
{
public:
    explicit TSupervisorService(IBootstrap* bootstrap)
        : NRpc::TServiceBase(
            bootstrap->GetJobInvoker(),
            TSupervisorServiceProxy::GetDescriptor(),
            ExecNodeLogger(),
            NRpc::TServiceOptions{
                .Authenticator = bootstrap->GetNativeAuthenticator(),
            })
        , Bootstrap_(bootstrap)
    {
        YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(GetJobSpec)
                .SetResponseCodec(NCompression::ECodec::Lz4)
                .SetInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobMemoryThrashing));
    }

private:
    IBootstrap* const Bootstrap_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    IThroughputThrottlerPtr GetJobThrottler(EJobThrottlerType throttlerType, std::optional<TClusterName> remoteClusterName)
    {
        switch (throttlerType) {
            case EJobThrottlerType::InBandwidth:
                return Bootstrap_->GetThrottler(EExecNodeThrottlerKind::JobIn, EThrottlerTrafficType::Bandwidth, std::move(remoteClusterName));
            case EJobThrottlerType::OutBandwidth:
                return Bootstrap_->GetThrottler(EExecNodeThrottlerKind::JobOut);
            case EJobThrottlerType::OutRps:
                return Bootstrap_->GetReadRpsOutThrottler();
            case EJobThrottlerType::ContainerCreation:
                return Bootstrap_->GetUserJobContainerCreationThrottler();
            default:
                THROW_ERROR_EXCEPTION("Unknown throttler type %Qlv", throttlerType);
        }
    }

    TJobPtr GetSchedulerJobOrThrow(TJobId jobId) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpec)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto validateJobPhase = [] (EJobPhase jobPhase) {
            if (jobPhase != EJobPhase::SpawningJobProxy) {
                THROW_ERROR_EXCEPTION("Cannot fetch job spec; job is in wrong phase")
                    << TErrorAttribute("expected_phase", EJobPhase::SpawningJobProxy)
                    << TErrorAttribute("actual_phase", jobPhase);
            }
        };

        auto [job, jobPhase, resourceUsage, ports] = WaitFor(BIND([bootstrap = Bootstrap_, jobId] {
                auto job = bootstrap->GetJobController()->GetJobOrThrow(jobId);

                auto jobPhase = job->GetPhase();

                auto resourceUsage = job->GetResourceUsage();
                auto ports = job->GetPorts();

                return std::make_tuple(job, jobPhase, resourceUsage, ports);
            })
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run())
            .ValueOrThrow();

        validateJobPhase(jobPhase);

        *response->mutable_job_spec() = job->GetSpec();

        // Job spec may become empty, we don't want to send empty spec to job proxy.
        auto newJobPhase = WaitFor(BIND([job] {
                return job->GetPhase();
            })
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run())
            .ValueOrThrow();

        validateJobPhase(newJobPhase);

        auto* resourceUsageProto = response->mutable_resource_usage();
        resourceUsageProto->set_cpu(resourceUsage.Cpu);
        resourceUsageProto->set_memory(resourceUsage.UserMemory);
        resourceUsageProto->set_network(resourceUsage.Network);

        ToProto(response->mutable_ports(), ports);

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
        auto result = std::move(*request->mutable_result());
        auto error = FromProto<TError>(result.error());
        context->SetRequestInfo("JobId: %v, Error: %v, ResultSize: %v, HasStatistics: %v, HasStderr: %v, HasFailedContext: %v",
            jobId,
            error,
            result.ByteSizeLong(),
            request->has_statistics(),
            request->has_job_stderr(),
            request->has_fail_context());

        auto job = GetSchedulerJobOrThrow(jobId);

        auto jobReport = TNodeJobReport().Error(error);
        if (request->has_statistics()) {
            auto ysonStatistics = TYsonString(request->statistics());
            job->SetStatistics(ysonStatistics);
            jobReport.SetStatistics(job->GetStatistics());
        }

        job->SetTotalInputDataStatistics(request->total_input_data_statistics());
        job->SetOutputDataStatistics(FromProto<std::vector<TDataStatistics>>(request->output_data_statistics()));
        // COMPAT(ignat): migrate to new fields (node_start_time, node_finish_time)
        if (request->has_start_time()) {
            jobReport.SetStartTime(FromProto<TInstant>(request->start_time()));
        }
        if (request->has_finish_time()) {
            jobReport.SetFinishTime(FromProto<TInstant>(request->finish_time()));
        }
        job->SetCoreInfos(FromProto<NControllerAgent::TCoreInfos>(request->core_infos()));
        job->HandleJobReport(std::move(jobReport));

        if (request->has_job_stderr()) {
            job->SetStderr(request->job_stderr());
        }

        if (request->has_fail_context()) {
            job->SetFailContext(request->fail_context());
        }

        for (const auto& profile : request->profiles()) {
            job->AddProfile({profile.type(), profile.blob(), profile.profiling_probability()});
        }

        job->OnResultReceived(std::move(result));

        YT_VERIFY(job->GetPhase() >= EJobPhase::FinalizingJobProxy);

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
        auto hasJobTrace = request->has_job_trace();

        context->SetRequestInfo("JobId: %v, Progress: %lf, Statistics: %v, StderrSize: %v, HasJobTrace: %v, HeartbeatEpoch: %v",
            jobId,
            progress,
            NYson::ConvertToYsonString(statistics, EYsonFormat::Text).AsStringBuf(),
            stderrSize,
            hasJobTrace,
            request->epoch());

        const auto& jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        if (job->GetPhase() >= EJobPhase::FinalizingJobProxy) {
            YT_LOG_DEBUG("Job is already not running, skipping progress (JobPhase: %v)", job->GetPhase());
        } else if (!job->UpdateJobProxyHearbeatEpoch(request->epoch())) {
            YT_LOG_DEBUG(
                "Received message with outdated epoch, skipping progress (ReceivedEpoch: %v, StoredEpoch: %v)",
                request->epoch(),
                job->GetJobProxyHeartbeatEpoch());
        } else {
            job->SetProgress(progress);
            job->SetStatistics(statistics);
            job->SetTotalInputDataStatistics(request->total_input_data_statistics());
            job->SetOutputDataStatistics(FromProto<std::vector<TDataStatistics>>(request->output_data_statistics()));
            job->SetStderrSize(stderrSize);
            job->SetHasJobTrace(hasJobTrace);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, UpdateResourceUsage)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        const auto& reportedResourceUsage = request->resource_usage();

        context->SetRequestInfo("JobId: %v, ReportedResourceUsage: {Cpu: %v, Memory %v, Network: %v}",
            jobId,
            reportedResourceUsage.cpu(),
            reportedResourceUsage.memory(),
            reportedResourceUsage.network());

        const auto& jobController = Bootstrap_->GetJobController();
        auto job = jobController->GetJobOrThrow(jobId);

        const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();
        auto resourceUsage = job->GetResourceUsage();
        resourceUsage.UserMemory = reportedResourceUsage.memory();
        resourceUsage.Cpu = reportedResourceUsage.cpu();
        resourceUsage.Network = reportedResourceUsage.network();
        resourceUsage.VCpu = resourceUsage.Cpu * jobResourceManager->GetCpuToVCpuFactor();

        job->SetResourceUsage(resourceUsage);

        if (job->GetPhase() >= EJobPhase::WaitingForCleanup) {
            THROW_ERROR_EXCEPTION("Cannot update resource usage for job in %Qlv phase", job->GetPhase());
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ThrottleJob)
    {
        auto throttlerType = FromProto<EJobThrottlerType>(request->throttler_type());
        auto amount = request->amount();
        auto workloadDescriptor = GetRequestWorkloadDescriptor(context);
        auto jobId = FromProto<TJobId>(request->job_id());
        auto remoteClusterName = YT_OPTIONAL_FROM_PROTO(*request, remote_cluster_name, TClusterName);

        context->SetRequestInfo("ThrottlerType: %v, Amount: %v, JobId: %v, WorkloadDescriptor: %v, RemoteClusterName: %v",
            throttlerType,
            amount,
            jobId,
            workloadDescriptor,
            remoteClusterName);

        const auto& throttler = GetJobThrottler(throttlerType, std::move(remoteClusterName));
        auto future = throttler->Throttle(amount);
        if (auto optionalResult = future.TryGet()) {
            optionalResult->ThrowOnError();
        } else {
            auto throttlingRequestId = Bootstrap_->GetJobController()->RegisterThrottlingRequest(future);

            ToProto(response->mutable_throttling_request_id(), throttlingRequestId);
            context->SetResponseInfo("ThrottlingRequestId: %v", throttlingRequestId);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PollThrottlingRequest)
    {
        auto throttlingRequestId = FromProto<TGuid>(request->throttling_request_id());

        context->SetRequestInfo("ThrottlingRequestId: %v", throttlingRequestId);

        auto future = Bootstrap_->GetJobController()->GetThrottlingRequestOrThrow(throttlingRequestId);
        auto optionalResult = future.TryGet();
        if (optionalResult) {
            optionalResult->ThrowOnError();
        }
        response->set_completed(optionalResult.has_value());
        context->SetResponseInfo("Completed: %v", response->completed());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobMemoryThrashing)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        Bootstrap_->GetJobController()->OnJobMemoryThrashing(jobId);

        context->Reply();
    }
};

NRpc::IServicePtr CreateSupervisorService(IBootstrap* bootstrap)
{
    return New<TSupervisorService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
