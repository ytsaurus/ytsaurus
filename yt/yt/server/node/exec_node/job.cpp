#include "job.h"

#include "bootstrap.h"
#include "chunk_cache.h"
#include "controller_agent_connector.h"
#include "job_controller.h"
#include "job_gpu_checker.h"
#include "job_workspace_builder.h"
#include "gpu_manager.h"
#include "private.h"
#include "slot.h"
#include "slot_manager.h"
#include "volume_manager.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/artifact.h>
#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/chunk.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/containers/cri/config.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/statistics.h>

#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/server/lib/job_agent/structs.h>

#include <yt/yt/server/lib/job_proxy/job_probe.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/server/lib/nbd/profiler.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/job_prober_client/public.h>

#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/job_proxy/config.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/system/env.h>

namespace NYT::NExecNode {

using namespace NRpc;
using namespace NJobProxy;
using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NFileClient;
using namespace NClusterNode;
using namespace NDataNode;
using namespace NClusterNode;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NIO;
using namespace NJobAgent;
using namespace NJobProberClient;
using namespace NChunkServer;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NConcurrency;
using namespace NApi;
using namespace NCoreDump;
using namespace NNet;
using namespace NProfiling;
using namespace NContainers;
using namespace NTracing;
using namespace NTransactionClient;

using NNodeTrackerClient::TNodeDirectory;
using NChunkClient::TDataSliceDescriptor;

using NObjectClient::TypeFromId;
using NCypressClient::EObjectType;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto DisableSandboxCleanupEnv = "YT_DISABLE_SANDBOX_CLEANUP";

static const TString SlotIndexPattern("\%slot_index\%");

static constexpr TStringBuf DockerAuthEnvPrefix("YT_SECURE_VAULT_docker_auth=");

////////////////////////////////////////////////////////////////////////////////

namespace {

TGuid MakeNbdExportId(TJobId jobId, int nbdExportIndex)
{
    auto nbdExportId = jobId.Underlying();
    nbdExportId.Parts32[0] = nbdExportIndex;
    return nbdExportId;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    TJobId jobId,
    TOperationId operationId,
    const NClusterNode::TJobResources& resourceUsage,
    const NClusterNode::TJobResourceAttributes& resourceAttributes,
    TJobSpec&& jobSpec,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap,
    const TJobCommonConfigPtr& commonConfig)
    : TResourceHolder(
        bootstrap->GetJobResourceManager().Get(),
        EResourcesConsumerType::SchedulerJob,
        ExecNodeLogger.WithTag(
            "JobId: %v, OperationId: %v, JobType: %v",
            jobId,
            operationId,
            CheckedEnumCast<EJobType>(jobSpec.type())),
        resourceUsage,
        resourceAttributes,
        jobSpec.GetExtension(TJobSpecExt::job_spec_ext).user_job_spec().port_count())
    , Id_(jobId)
    , OperationId_(operationId)
    , Bootstrap_(bootstrap)
    , ControllerAgentDescriptor_(std::move(agentDescriptor))
    , ControllerAgentConnector_(
        Bootstrap_->GetControllerAgentConnectorPool()->GetControllerAgentConnector(this))
    , CommonConfig_(commonConfig)
    , Invoker_(Bootstrap_->GetJobInvoker())
    , CreationTime_(TInstant::Now())
    , TrafficMeter_(New<TTrafficMeter>(
        Bootstrap_->GetLocalDescriptor().GetDataCenter()))
    , JobSpec_(std::move(jobSpec))
    , JobSpecExt_(&JobSpec_.GetExtension(TJobSpecExt::job_spec_ext))
    , UserJobSpec_(JobSpecExt_ && JobSpecExt_->has_user_job_spec() ? &JobSpecExt_->user_job_spec() : nullptr)
    , JobTestingOptions_(JobSpecExt_ && JobSpecExt_->has_testing_options()
        ? ConvertTo<TJobTestingOptionsPtr>(TYsonString(JobSpecExt_->testing_options()))
        : New<TJobTestingOptions>())
    , Interruptible_(JobSpecExt_->interruptible())
    , AbortJobIfAccountLimitExceeded_(JobSpecExt_->abort_job_if_account_limit_exceeded())
    , IsGpuRequested_(resourceUsage.Gpu > 0)
    , RequestedCpu_(resourceUsage.Cpu)
    , RequestedMemory_(resourceUsage.UserMemory)
    , TraceContext_(CreateTraceContextFromCurrent("Job"))
    , FinishGuard_(TraceContext_)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    PackBaggageFromJobSpec(TraceContext_, JobSpec_, OperationId_, Id_);

    SupportedMonitoringSensors_ = CommonConfig_->UserJobMonitoring->Sensors;

    TrafficMeter_->Start();

    AddJobEvent(JobState_, JobPhase_);

    HandleJobReport(MakeDefaultJobReport()
        .TreeId(JobSpecExt_->tree_id()));
}

TJob::~TJob()
{
    // Offload job spec destruction to a large thread pool.
    auto jobSpec = std::make_unique<TJobSpec>(std::move(JobSpec_));
    NRpc::TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(
        BIND([jobSpec = std::move(jobSpec)] () mutable { jobSpec.reset(); }));
}

void TJob::DoStart(TErrorOr<std::vector<TNameWithAddress>>&& resolvedNodeAddresses)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "DoStart",
        [&] () {
            auto now = TInstant::Now();
            PrepareStartTime_ = now;

            if (!resolvedNodeAddresses.IsOK()) {
                THROW_ERROR TError("Failed to resolve node addresses") << std::move(resolvedNodeAddresses);
            }

            ResolvedNodeAddresses_ = std::move(resolvedNodeAddresses.Value());

            StartUserJobMonitoring();

            InitializeArtifacts();

            if (UserJobSpec_) {
                if (UserJobSpec_->has_prepare_time_limit()) {
                    auto prepareTimeLimit = FromProto<TDuration>(UserJobSpec_->prepare_time_limit());
                    TDelayedExecutor::Submit(
                        BIND(&TJob::OnJobPreparationTimeout, MakeWeak(this), prepareTimeLimit, /*fatal*/ false)
                            .Via(Invoker_),
                        prepareTimeLimit);
                }

                if (auto prepareTimeLimit = CommonConfig_->JobPrepareTimeLimit) {
                    TDelayedExecutor::Submit(
                        BIND(&TJob::OnJobPreparationTimeout, MakeWeak(this), *prepareTimeLimit, /*fatal*/ true)
                            .Via(Invoker_),
                        *prepareTimeLimit);
                }

                if (UserJobSpec_->has_network_project_id()) {
                    NetworkProjectId_ = UserJobSpec_->network_project_id();
                }
            }

            if (NeedGpu()) {
                GpuStatistics_.resize(GpuSlots_.size());
            }

            SetJobPhase(EJobPhase::PreparingNodeDirectory);

            // This is a heavy part of preparation, offload it to compression invoker.
            // TODO(babenko): get rid of MakeWeak
            BIND([weakThis = MakeWeak(this)] {
                auto strongThis = weakThis.Lock();
                if (!strongThis) {
                    return std::unique_ptr<NNodeTrackerClient::NProto::TNodeDirectory>();
                }
                return strongThis->PrepareNodeDirectory();
            })
                .AsyncVia(NRpc::TDispatcher::Get()->GetCompressionPoolInvoker())
                .Run()
                .SubscribeUnique(
                    BIND(&TJob::OnNodeDirectoryPrepared, MakeWeak(this))
                        .Via(Invoker_));
        });
}

bool TJob::IsStarted() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return Started_;
}

void TJob::OnResourcesAcquired() noexcept
{
    VERIFY_THREAD_AFFINITY(JobThread);

    Start();
}

void TJob::Start() noexcept
{
    VERIFY_THREAD_AFFINITY(JobThread);

    TCurrentTraceContextGuard guard(TraceContext_);

    YT_VERIFY(!std::exchange(Started_, true));

    if (JobPhase_ != EJobPhase::Created) {
        YT_LOG_FATAL("Cannot start job, unexpected job phase (JobState: %v, JobPhase: %v)",
            JobState_,
            JobPhase_);
        return;
    }

    YT_LOG_INFO("Start job");

    SetJobState(EJobState::Running);

    GetUserSlot()->SetAllocationId(GetAllocationId());

    TFuture<std::vector<TNameWithAddress>> resolveFuture;

    if (UserJobSpec_ && UserJobSpec_->has_network_project_id()) {
        std::vector<TFuture<TNameWithAddress>> nodeAddressFutures;

        auto addresses = Bootstrap_->GetConfig()->Addresses;
        ResolvedNodeAddresses_.reserve(std::size(addresses));

        auto* resolver = TAddressResolver::Get();

        for (auto& [addressName, address] : addresses) {
            nodeAddressFutures.push_back(
                resolver->Resolve(address)
                    .Apply(BIND(
                        [
                            this,
                            this_ = MakeStrong(this),
                            address = std::move(address),
                            addressName = std::move(addressName)
                        ] (const TErrorOr<TNetworkAddress>& resolvedAddressOrError) mutable {
                            if (!resolvedAddressOrError.IsOK()) {
                                YT_LOG_WARNING(
                                    resolvedAddressOrError,
                                    "Failed to resolve node address (AddressName: %v, Address: %v)",
                                    addressName,
                                    address);
                                THROW_ERROR resolvedAddressOrError;
                            }

                            const auto& resolvedAddress = resolvedAddressOrError.Value();
                            YT_VERIFY(resolvedAddress.IsIP6());

                            return TNameWithAddress{
                                .Name = std::move(addressName),
                                .Address = resolvedAddress.ToIP6Address(),
                            };
                        })));
        }

        resolveFuture = AllSucceeded(std::move(nodeAddressFutures));
    } else {
        resolveFuture = MakeFuture(std::vector<TNameWithAddress>());
    }

    resolveFuture.SubscribeUnique(
        BIND(&TJob::DoStart, MakeStrong(this))
            .Via(Bootstrap_->GetJobInvoker()));
}

void TJob::Abort(TError error, bool graceful)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO(
        error,
        "Job abort requested (Phase: %v, State: %v)",
        JobPhase_,
        JobState_);

    if (graceful) {
        RequestGracefulAbort(std::move(error));
        return;
    }

    Terminate(EJobState::Aborted, std::move(error));
}

void TJob::OnJobProxySpawned()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "OnJobProxySpawned",
        [&] {
            YT_LOG_INFO("Job proxy spawned");

            ValidateJobPhase(EJobPhase::SpawningJobProxy);
            SetJobPhase(EJobPhase::PreparingArtifacts);

            if (!Bootstrap_->GetJobController()->IsJobProxyProfilingDisabled()) {
                Bootstrap_->GetJobProxySolomonExporter()->AttachRemoteProcess(BIND(&TJob::DumpSensors, MakeStrong(this)));
            }
        });
}

void TJob::PrepareArtifact(
    const TString& artifactName,
    const TString& pipePath)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "PrepareArtifact",
        [&] {
            YT_LOG_DEBUG("Prepare job artifact (ArtifactName: %v, PipePath: %v)",
                artifactName,
                pipePath);

            // NB: Open pipe for writing before reply.
            auto pipeFd = HandleEintr(::open, pipePath.c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC);
            TFile pipe(pipeFd);

            auto fcntlResult = HandleEintr(::fcntl, pipeFd, F_SETFL, O_WRONLY | O_CLOEXEC);
            if (fcntlResult < 0) {
                THROW_ERROR_EXCEPTION("Failed to disable O_NONBLOCK for artifact pipe")
                    << TError::FromSystem();
            }

            ValidateJobPhase(EJobPhase::PreparingArtifacts);

            int artifactIndex = GetOrCrash(UserArtifactNameToIndex_, artifactName);
            const auto& artifact = Artifacts_[artifactIndex];

            YT_VERIFY(artifact.BypassArtifactCache || artifact.CopyFile);

            auto traceContext = CreateTraceContextFromCurrent("ArtifactPrepare");
            TTraceContextGuard guard(traceContext);
            auto baggage = traceContext->UnpackOrCreateBaggage();
            const char* jobIOKind = artifact.BypassArtifactCache ? "artifact_bypass_cache" : "artifact_copy";
            AddTagToBaggage(baggage, EAggregateIOTag::JobIoKind, jobIOKind);
            AddTagsFromDataSource(baggage, FromProto<NChunkClient::TDataSource>(artifact.Key.data_source()));
            traceContext->PackBaggage(std::move(baggage));

            if (artifact.BypassArtifactCache) {
                YT_LOG_INFO("Download artifact with cache bypass (FileName: %v, Executable: %v, SandboxKind: %v, CompressedDataSize: %v)",
                    artifact.Name,
                    artifact.Executable,
                    artifact.SandboxKind,
                    artifact.Key.GetCompressedDataSize());

                const auto& chunkCache = Bootstrap_->GetChunkCache();
                auto downloadOptions = MakeArtifactDownloadOptions();
                auto producer = chunkCache->MakeArtifactDownloadProducer(artifact.Key, downloadOptions);

                ArtifactPrepareFutures_.push_back(
                    GetUserSlot()->MakeFile(
                        Id_,
                        artifact.Name,
                        artifact.SandboxKind,
                        producer,
                        pipe));
            } else if (artifact.CopyFile) {
                YT_VERIFY(artifact.Chunk);

                YT_LOG_INFO("Copy artifact (FileName: %v, Executable: %v, SandboxKind: %v, CompressedDataSize: %v)",
                    artifact.Name,
                    artifact.Executable,
                    artifact.SandboxKind,
                    artifact.Key.GetCompressedDataSize());

                ArtifactPrepareFutures_.push_back(
                    GetUserSlot()->MakeCopy(
                        Id_,
                        artifact.Name,
                        artifact.SandboxKind,
                        artifact.Chunk->GetFileName(),
                        pipe,
                        artifact.Chunk->GetLocation()));
            }
        });
}

void TJob::OnArtifactPreparationFailed(
    const TString& artifactName,
    const TString& artifactPath,
    const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    Y_UNUSED(artifactName);

    GuardedAction(
        "OnArtifactPreparationFailed",
        [&] {
            ValidateJobPhase(EJobPhase::PreparingArtifacts);

            GetUserSlot()->OnArtifactPreparationFailed(
                Id_,
                artifactName,
                ESandboxKind::User,
                artifactPath,
                error);
    });
}

void TJob::OnArtifactsPrepared()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    // Wait for possible errors during node-side artifact preparation.
    WaitFor(AllSucceeded(ArtifactPrepareFutures_))
        .ThrowOnError();

    GuardedAction(
        "OnArtifactsPrepared",
        [&] {
            YT_LOG_INFO("Artifacts prepared");

            ValidateJobPhase(EJobPhase::PreparingArtifacts);
            SetJobPhase(EJobPhase::PreparingJob);
        });
}

void TJob::OnJobPrepared()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "OnJobPrepared",
        [&] {
            JobPrepared_.Fire(MakeStrong(this));

            YT_LOG_INFO("Job prepared");

            ValidateJobPhase(EJobPhase::PreparingJob);
            SetJobPhase(EJobPhase::Running);
        });
}

void TJob::Terminate(EJobState finalState, TError error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto doTerminate = [&] {
        auto timeout = CommonConfig_->WaitingForJobCleanupTimeout;

        SetJobPhase(EJobPhase::WaitingForCleanup);
        Finalize(finalState, std::move(error));
        YT_LOG_INFO("Waiting for job cleanup (Timeout: %v)", timeout);
        TDelayedExecutor::Submit(
            BIND(&TJob::OnWaitingForCleanupTimeout, MakeStrong(this))
                .Via(Invoker_),
            timeout);
        ArtifactsFuture_.Cancel(TError("Job terminated"));
        WorkspaceBuildingFuture_.Cancel(TError("Job preparation canceled"));

        if (const auto& slot = GetUserSlot()) {
            slot->CancelPreparation();
        }
    };

    switch (JobPhase_) {
        case EJobPhase::Created:
            doTerminate();
            Cleanup();
            break;

        case EJobPhase::PreparingNodeDirectory:
        case EJobPhase::DownloadingArtifacts:
        case EJobPhase::PreparingSandboxDirectories:
        case EJobPhase::PreparingRootVolume:
        case EJobPhase::RunningSetupCommands:
        case EJobPhase::RunningGpuCheckCommand:
        case EJobPhase::SpawningJobProxy:
        case EJobPhase::PreparingArtifacts:
        case EJobPhase::PreparingJob:
        case EJobPhase::Running:
        case EJobPhase::RunningExtraGpuCheckCommand:
            doTerminate();
            YT_UNUSED_FUTURE(StopJobProxy());
            break;

        case EJobPhase::FinalizingJobProxy:
            YT_LOG_INFO(
                "Cannot terminate job (JobState: %v, JobPhase: %v, JobError: %v)",
                JobState_,
                JobPhase_,
                Error_);
            break;

        default:
            YT_LOG_INFO(
                "Cannot terminate job (JobState: %v, JobPhase: %v)",
                JobState_,
                JobPhase_);

            YT_VERIFY(IsFinished());
            break;
    }
}

void TJob::Finalize(TError error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    TForbidContextSwitchGuard guard;

    if (!Finalize(
        /*finalJobState*/ std::nullopt,
        std::move(error),
        /*jobResultExtension*/ std::nullopt,
        /*byJobProxyCompletion*/ false))
    {
        return;
    }

    YT_VERIFY(Error_);
    auto& currentError = *Error_;

    // NB: we should disable slot here to give scheduler information about job failure.
    if (currentError.FindMatching(NExecNode::EErrorCode::GpuCheckCommandFailed) &&
        !currentError.FindMatching(NExecNode::EErrorCode::GpuCheckCommandIncorrect))
    {
        Bootstrap_->GetSlotManager()->OnGpuCheckCommandFailed(currentError);
    }
}

bool TJob::Finalize(
    std::optional<EJobState> finalJobState,
    TError error,
    std::optional<NControllerAgent::NProto::TJobResultExt> jobResultExtension,
    bool byJobProxyCompletion)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    TForbidContextSwitchGuard guard;

    if (IsFinished()) {
        YT_LOG_DEBUG("Job already finalized");
        return false;
    }

    YT_LOG_INFO("Finalizing job (FinalState: %v)", finalJobState);

    DoSetResult(std::move(error), std::move(jobResultExtension), byJobProxyCompletion);

    YT_VERIFY(Error_);
    auto& currentError = *Error_;

    if (!finalJobState) {
        DeduceAndSetFinishedJobState();
    } else {
        if (*finalJobState == EJobState::Aborted) {
            if (auto deducedAbortReason = DeduceAbortReason()) {
                currentError.MutableAttributes()->Set("abort_reason", deducedAbortReason);

                YT_LOG_DEBUG(
                    "Deduced abort reason set to error (AbortReason: %v, Error: %v)",
                    deducedAbortReason,
                    currentError);
            }
        }

        SetJobState(*finalJobState);
    }

    OnJobFinalized();

    return true;
}

void TJob::Finalize(EJobState finalState, TError error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(finalState == EJobState::Aborted || finalState == EJobState::Failed);

    Finalize(
        finalState,
        std::move(error),
        /*jobResultExtension*/ std::nullopt,
        /*byJobProxyCompletion*/ false);
}

void TJob::OnJobFinalized()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);
    auto& currentError = *Error_;

    YT_LOG_INFO(
        currentError,
        "Job finalized (JobState: %v, ResourceUsage: %v)",
        GetState(),
        FormatResources(GetResourceUsage()));

    YT_VERIFY(IsFinished());

    FinishTime_ = TInstant::Now();
    // Copy info from traffic meter to statistics.
    auto statistics = ConvertTo<TStatistics>(StatisticsYson_);
    FillTrafficStatistics(ExecAgentTrafficStatisticsPrefix, statistics, TrafficMeter_);
    StatisticsYson_ = ConvertToYsonString(statistics);

    JobFinished_.Fire(MakeStrong(this));

    if (!currentError.IsOK()) {
        // NB: it is required to report error that occurred in some place different
        // from OnJobFinished method.
        HandleJobReport(MakeDefaultJobReport().Error(currentError));
    }
}

void TJob::DeduceAndSetFinishedJobState()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);
    auto& currentError = *Error_;

    if (currentError.IsOK()) {
        SetJobState(EJobState::Completed);
    } else if (IsFatalError(currentError)) {
        currentError.MutableAttributes()->Set("fatal", true);
        SetJobState(EJobState::Failed);
    } else {
        auto deducedAbortReason = DeduceAbortReason();
        if (deducedAbortReason) {
            currentError.MutableAttributes()->Set("abort_reason", deducedAbortReason);
            SetJobState(EJobState::Aborted);
        } else {
            SetJobState(EJobState::Failed);
        }
    }
}

void TJob::OnResultReceived(TJobResult jobResult)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "OnResultReceived",
        [&] {
            SetJobPhase(EJobPhase::FinalizingJobProxy);

            std::optional<NControllerAgent::NProto::TJobResultExt> jobResultExtension;
            if (jobResult.HasExtension(NControllerAgent::NProto::TJobResultExt::job_result_ext)) {
                jobResultExtension = std::move(
                    *jobResult.ReleaseExtension(NControllerAgent::NProto::TJobResultExt::job_result_ext));
            }

            if (auto error = FromProto<TError>(jobResult.error());
                error.IsOK() || !NeedsGpuCheck())
            {
                Finalize(
                    /*finalJobState*/ std::nullopt,
                    std::move(error),
                    std::move(jobResultExtension),
                    /*byJobProxyCompletion*/ true);
            } else {
                DoSetResult(
                    std::move(error),
                    /*jobResultExtension*/ std::nullopt,
                    /*receivedFromJobProxy*/ true);
            }
        });
}

TJobId TJob::GetId() const noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

TGuid TJob::GetIdAsGuid() const noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_.Underlying();
}

TAllocationId TJob::GetAllocationId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return TAllocationId(GetIdAsGuid());
}

TOperationId TJob::GetOperationId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return OperationId_;
}

IInvokerPtr TJob::GetInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Invoker_;
}

const TControllerAgentDescriptor& TJob::GetControllerAgentDescriptor() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return ControllerAgentDescriptor_;
}

void TJob::UpdateControllerAgentDescriptor(TControllerAgentDescriptor agentDescriptor)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (ControllerAgentDescriptor_ == agentDescriptor) {
        return;
    }

    YT_LOG_DEBUG(
        "Update controller agent (ControllerAgentAddress: %v -> %v, ControllerAgentIncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        agentDescriptor.Address,
        agentDescriptor.IncarnationId);

    ControllerAgentDescriptor_ = std::move(agentDescriptor);
    ControllerAgentConnector_ = Bootstrap_
        ->GetControllerAgentConnectorPool()
        ->GetControllerAgentConnector(this);
}

EJobType TJob::GetType() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CheckedEnumCast<EJobType>(JobSpec_.type());
}

const TJobSpec& TJob::GetSpec() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JobSpec_;
}

EJobState TJob::GetState() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return JobState_;
}

TInstant TJob::GetCreationTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CreationTime_;
}

NJobAgent::TTimeStatistics TJob::GetTimeStatistics() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto getPrepareDuration = [&] () -> std::optional<TDuration> {
        // TODO(arkady-e1ppa): Fix PrepareStartTime semantics.
        if (!StartTime_) {
            return std::nullopt;
        }
        if (!PrepareStartTime_) {
            return std::nullopt;
        } else if (!ExecStartTime_) {
            return TInstant::Now() - *PrepareStartTime_;
        } else {
            return *ExecStartTime_ - *PrepareStartTime_;
        }
    };
    auto getPrepareRootFSDuration = [&] () -> std::optional<TDuration> {
        if (!StartPrepareVolumeTime_) {
            return std::nullopt;
        } else if (!FinishPrepareVolumeTime_) {
            return TInstant::Now() - *StartPrepareVolumeTime_;
        } else {
            return *FinishPrepareVolumeTime_ - *StartPrepareVolumeTime_;
        }
    };
    auto getArtifactsDownloadDuration = [&] () -> std::optional<TDuration> {
        if (!PrepareStartTime_) {
            return std::nullopt;
        } else if (!CopyFinishTime_) {
            return TInstant::Now() - *PrepareStartTime_;
        } else {
            return *CopyFinishTime_ - *PrepareStartTime_;
        }
    };
    auto getExecDuration = [&] () -> std::optional<TDuration> {
        if (!ExecStartTime_) {
            return std::nullopt;
        } else if (!FinishTime_) {
            return TInstant::Now() - *ExecStartTime_;
        } else {
            return *FinishTime_ - *ExecStartTime_;
        }
    };
    auto getPreliminaryGpuCheckDuration = [&] () -> std::optional<TDuration> {
        if (!PreliminaryGpuCheckStartTime_) {
            return std::nullopt;
        } else if (!PreliminaryGpuCheckFinishTime_) {
            return TInstant::Now() - *PreliminaryGpuCheckStartTime_;
        } else {
            return *PreliminaryGpuCheckFinishTime_ - *PreliminaryGpuCheckStartTime_;
        }
    };
    auto getExtraGpuCheckDuration = [&] () -> std::optional<TDuration> {
        if (!ExtraGpuCheckStartTime_) {
            return std::nullopt;
        } else if (!ExtraGpuCheckFinishTime_) {
            return TInstant::Now() - *ExtraGpuCheckStartTime_;
        } else {
            return *ExtraGpuCheckFinishTime_ - *ExtraGpuCheckStartTime_;
        }
    };
    auto sumOptionals = [&] (std::optional<TDuration> lhs, std::optional<TDuration> rhs) -> std::optional<TDuration> {
        if (!lhs && !rhs) {
            return std::nullopt;
        } else if (lhs && !rhs) {
            return lhs;
        } else if (rhs && !lhs) {
            return rhs;
        } else {
            return *lhs + *rhs;
        }
    };

    return NJobAgent::TTimeStatistics{
        .PrepareDuration = getPrepareDuration(),
        .ArtifactsDownloadDuration = getArtifactsDownloadDuration(),
        .PrepareRootFSDuration = getPrepareRootFSDuration(),
        .ExecDuration = getExecDuration(),
        .GpuCheckDuration = sumOptionals(getPreliminaryGpuCheckDuration(), getExtraGpuCheckDuration())};
}

std::optional<TInstant> TJob::GetStartTime() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return StartTime_;
}

EJobPhase TJob::GetPhase() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return JobPhase_;
}

int TJob::GetSlotIndex() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto slot = GetUserSlot();
    if (!slot) {
        return -1;
    }

    return slot->GetSlotIndex();
}

NClusterNode::TJobResources TJob::GetResourceUsage() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return TResourceHolder::GetResourceUsage();
}

bool TJob::IsGpuRequested() const
{
    return IsGpuRequested_;
}

const std::vector<int>& TJob::GetPorts() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return TResourceHolder::GetPorts();
}

const TError& TJob::GetJobError() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);

    return *Error_;
}

TJobResult TJob::GetResult() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);

    TJobResult result;
    ToProto(result.mutable_error(), *Error_);

    if (JobResultExtension_) {
        *result.MutableExtension(
            NControllerAgent::NProto::TJobResultExt::job_result_ext) = *JobResultExtension_;
    }

    return result;
}

double TJob::GetProgress() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return Progress_;
}

void TJob::SetResourceUsage(const NClusterNode::TJobResources& newUsage)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ == EJobPhase::Running) {
        TResourceHolder::SetBaseResourceUsage(newUsage);
    }
}

bool TJob::ResourceUsageOverdrafted() const
{
    return TResourceHolder::GetResourceUsage().UserMemory > RequestedMemory_;
}

void TJob::SetProgress(double progress)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ == EJobPhase::Running) {
        Progress_ = progress;
    }
}

i64 TJob::GetStderrSize() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return StderrSize_;
}

void TJob::SetStderrSize(i64 value)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (StderrSize_ != value) {
        StderrSize_ = value;
        HandleJobReport(MakeDefaultJobReport()
            .StderrSize(StderrSize_));
    }
}

void TJob::SetStderr(const TString& value)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    Stderr_ = value;
}

void TJob::SetFailContext(const TString& value)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    FailContext_ = value;
}

void TJob::AddProfile(TJobProfile value)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    Profiles_.push_back(std::move(value));
}

void TJob::SetCoreInfos(TCoreInfos value)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    CoreInfos_ = std::move(value);
}

const TChunkCacheStatistics& TJob::GetChunkCacheStatistics() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return ChunkCacheStatistics_;
}

TYsonString TJob::GetStatistics() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return StatisticsYson_;
}

TDataStatistics TJob::GetTotalInputDataStatistics() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return TotalInputDataStatistics_;
}

std::vector<TDataStatistics> TJob::GetOutputDataStatistics() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return OutputDataStatistics_;
}

TInstant TJob::GetStatisticsLastSendTime() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return StatisticsLastSendTime_;
}

void TJob::ResetStatisticsLastSendTime()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    StatisticsLastSendTime_ = TInstant::Now();
}

void TJob::SetStatistics(const TYsonString& statisticsYson)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ == EJobPhase::Running || JobPhase_ == EJobPhase::FinalizingJobProxy) {
        auto statistics = ConvertTo<TStatistics>(statisticsYson);
        GetTimeStatistics().AddSamplesTo(&statistics);

        if (!GpuSlots_.empty()) {
            EnrichStatisticsWithGpuInfo(&statistics);

            if (IsFullHostGpuJob()) {
                EnrichStatisticsWithRdmaDeviceInfo(&statistics);
            }
        }

        EnrichStatisticsWithDiskInfo(&statistics);

        EnrichStatisticsWithArtifactsInfo(&statistics);

        UpdateIOStatistics(statistics);

        StatisticsYson_ = ConvertToYsonString(statistics);

        HandleJobReport(MakeDefaultJobReport()
            .Statistics(StatisticsYson_));

        if (UserJobSensorProducer_) {
            TSensorBuffer userJobSensors;
            CollectSensorsFromStatistics(&userJobSensors);
            CollectSensorsFromGpuAndRdmaDeviceInfo(&userJobSensors);
            UserJobSensorProducer_->Update(std::move(userJobSensors));
        }
    }
}

void TJob::SetTotalInputDataStatistics(TDataStatistics datastatistics)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    TotalInputDataStatistics_ = std::move(datastatistics);
}

void TJob::SetOutputDataStatistics(std::vector<TDataStatistics> dataStatistics)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    OutputDataStatistics_ = std::move(dataStatistics);
}

TBriefJobInfo TJob::GetBriefInfo() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto [
        baseResourceUsage,
        additionalResourceUsage
    ] = TResourceHolder::GetDetailedResourceUsage();

    return TBriefJobInfo(
        GetId(),
        GetState(),
        GetPhase(),
        GetType(),
        GetStored(),
        IsInterrupted(),
        GetSlotIndex(),
        GetCreationTime(),
        /*jobDuration*/ TInstant::Now() - GetCreationTime(),
        GetStatistics(),
        GetOperationId(),
        baseResourceUsage,
        additionalResourceUsage,
        GetPorts(),
        JobEvents_,
        CoreInfos_,
        ExecAttributes_);
}

std::vector<TChunkId> TJob::DumpInputContext(TTransactionId transactionId)
{
    VERIFY_THREAD_AFFINITY(JobThread);
    ValidateJobRunning();

    try {
        return GetJobProbeOrThrow()->DumpInputContext(transactionId);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error requesting input contexts dump from job proxy")
            << ex;
    }
}

std::optional<TString> TJob::GetStderr()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (Stderr_) {
        return *Stderr_;
    }

    if (!UserJobSpec_) {
        return std::nullopt;
    }

    if (JobPhase_ == EJobPhase::Running) {
        try {
            return GetJobProbeOrThrow()->GetStderr();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error requesting stderr from job proxy")
                << ex;
        }
    }

    if (JobPhase_ < EJobPhase::Running ||
        JobState_ == EJobState::Aborted ||
        JobState_ == EJobState::Failed)
    {
        return std::nullopt;
    }

    // Cleanup is not atomic, so in case of job proxy failure we might see job in cleanup phase and running state.
    if (JobPhase_ == EJobPhase::Cleanup) {
        YT_VERIFY(Error_);
        if (Error_->FindMatching(NExecNode::EErrorCode::JobProxyFailed) ||
            Error_->FindMatching(NExecNode::EErrorCode::JobProxyPreparationTimeout))
        {
            return std::nullopt;
        }
    }

    // When job proxy finished with completed or failed state, Stderr_ must not be unset.
    YT_LOG_ALERT(
        "Stderr is unset for job (JobState: %v, JobPhase: %v)",
        JobState_,
        JobPhase_);

    return std::nullopt;
}

std::optional<TString> TJob::GetFailContext()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return FailContext_;
}

const TCoreInfos& TJob::GetCoreInfos()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return CoreInfos_;
}

TPollJobShellResponse TJob::PollJobShell(
    const TJobShellDescriptor& jobShellDescriptor,
    const TYsonString& parameters)
{
    VERIFY_THREAD_AFFINITY_ANY();

    try {
        return GetJobProbeOrThrow()->PollJobShell(jobShellDescriptor, parameters);
    } catch (const TErrorException& ex) {
        // The following code changes error code for more user-friendly
        // diagnostics in interactive shell.
        if (ex.Error().FindMatching(NRpc::EErrorCode::TransportError)) {
            THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::JobProxyConnectionFailed,
                "No connection to job proxy")
                << ex;
        }
        THROW_ERROR_EXCEPTION("Error polling job shell")
            << ex;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error polling job shell")
            << ex;
    }
}

void TJob::HandleJobReport(TNodeJobReport&& jobReport)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    Bootstrap_->GetJobReporter()->HandleJobReport(
        jobReport
            .OperationId(GetOperationId())
            .JobId(GetId())
            .Address(Bootstrap_->GetLocalDescriptor().GetDefaultAddress()));
}

void TJob::ReportSpec()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    HandleJobReport(MakeDefaultJobReport()
        .Spec(JobSpec_));
}

void TJob::ReportStderr()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto maybeStderr = GetStderr();
    if (!maybeStderr) {
        return;
    }
    HandleJobReport(TNodeJobReport()
        .Stderr(std::move(*maybeStderr)));
}

void TJob::ReportFailContext()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (auto failContext = GetFailContext()) {
        HandleJobReport(TNodeJobReport()
            .FailContext(*failContext));
    }
}

void TJob::ReportProfile()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    for (const auto& profile : Profiles_) {
        HandleJobReport(TNodeJobReport()
            .Profile(std::move(profile)));
    }
}

void TJob::DoInterrupt(
    TDuration timeout,
    EInterruptReason interruptionReason,
    const std::optional<TString>& preemptionReason,
    const std::optional<NScheduler::TPreemptedFor>& preemptedFor)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(interruptionReason != EInterruptReason::None);

    if (InterruptionDeadline_ && InterruptionDeadline_ < TInstant::Now() + timeout) {
        YT_LOG_DEBUG(
            "Job interruption with earlier deadline is already requested, ignore (InterruptionReason: %v, PreemptedFor: %v, CurrentError: %v, CurrentDeadline: %v)",
            InterruptionReason_,
            PreemptedFor_,
            Error_,
            InterruptionDeadline_);
        return;
    }

    YT_LOG_DEBUG(
        "Job interruption requested (Timeout: %v, InterruptionReason: %v, Preempted: %v, PreemptionReason: %v, PreemptedFor: %v)",
        timeout,
        interruptionReason,
        preemptionReason,
        preemptedFor);

    if (JobPhase_ > EJobPhase::Running) {
        // We're done with this job, no need to interrupt.
        YT_LOG_DEBUG("Job is already not running, do nothing (JobPhase: %v)", JobPhase_);
        return;
    }

    InterruptionReason_ = interruptionReason;
    PreemptedFor_ = preemptedFor;

    if (!IsInterruptible()) {
        YT_LOG_DEBUG("Job is not interruptible and will be aborted");

        auto error = TError(NJobProxy::EErrorCode::InterruptionUnsupported, "Uninterruptible job aborted")
            << TErrorAttribute("interruption_reason", InterruptionReason_)
            << TErrorAttribute("abort_reason", EAbortReason::InterruptionUnsupported);

        if (interruptionReason == EInterruptReason::Preemption) {
            error = TError("Job preempted") << error;
            error = error
                << TErrorAttribute("preemption_reason", preemptionReason)
                << TErrorAttribute("abort_reason", EAbortReason::Preemption);
        }

        if (interruptionReason == EInterruptReason::JobsDisabledOnNode) {
            error = TError("Jobs disabled on node")
                << error;
        }

        ReportJobInterruptionInfo(timeout, interruptionReason, preemptionReason, preemptedFor);

        Abort(error);
        return;
    }

    if (JobPhase_ < EJobPhase::Running) {
        auto error = TError(NJobProxy::EErrorCode::JobNotPrepared, "Interrupting job that has not started yet")
            << TErrorAttribute("interruption_reason", InterruptionReason_);

        if (interruptionReason == EInterruptReason::Preemption) {
            error = TError("Job preempted") << error;
            error = error
                << TErrorAttribute("preemption_reason", preemptionReason)
                << TErrorAttribute("abort_reason", EAbortReason::Preemption);
        }

        ReportJobInterruptionInfo(timeout, interruptionReason, preemptionReason, preemptedFor);

        Abort(error);
        return;
    }

    try {
        if (!InterruptionRequested_) {
            GetJobProbeOrThrow()->Interrupt();
        }

        InterruptionRequested_ = true;

        if (timeout) {
            TDelayedExecutor::CancelAndClear(InterruptionTimeoutCookie_);
            InterruptionTimeoutCookie_ = TDelayedExecutor::Submit(
                BIND(
                    &TJob::OnJobInterruptionTimeout,
                    MakeWeak(this),
                    InterruptionReason_,
                    preemptionReason),
                timeout,
                Bootstrap_->GetJobInvoker());
            InterruptionDeadline_ = TInstant::Now() + timeout;
        }

        ReportJobInterruptionInfo(timeout, interruptionReason, preemptionReason, preemptedFor);
    } catch (const std::exception& ex) {
        auto error = TError("Error interrupting job on job proxy")
            << ex;

        if (error.FindMatching(NJobProxy::EErrorCode::JobNotPrepared)) {
            Abort(error);
        } else {
            THROW_ERROR error;
        }
    }
}

void TJob::Fail(std::optional<TError> error)
{
    YT_LOG_INFO("Fail job (Error: %v)", error);

    try {
        DoFail(std::move(error));
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Error failing job");
    }
}

void TJob::DoFail(std::optional<TError> error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::Running) {
        if (!error) {
            error = TError("Failing job that is not running");
        }

        Terminate(EJobState::Failed, std::move(*error));

        return;
    }

    try {
        GetJobProbeOrThrow()->Fail();
    } catch (const std::exception& ex) {
        auto abortionError = TError("Error failing job on job proxy")
            << ex;
        if (error) {
            abortionError = abortionError << *error;
        }
        Abort(std::move(abortionError));
    }
}

void TJob::RequestGracefulAbort(TError error)
{
    YT_LOG_INFO("Requesting job graceful abort (Error: %v)", error);

    try {
        DoRequestGracefulAbort(std::move(error));
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to request job graceful abort");
    }
}

void TJob::DoRequestGracefulAbort(TError error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::Running) {
        Terminate(EJobState::Failed, std::move(error));
        return;
    }

    try {
        GetJobProbeOrThrow()->GracefulAbort(error);
    } catch (const std::exception& ex) {
        auto abortionError = TError("Error failing job on job proxy")
            << ex;
        abortionError <<= std::move(error);
        Abort(std::move(abortionError));
    }
}

bool TJob::GetStored() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return Stored_;
}

void TJob::SetStored()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    Stored_ = true;
    LastStoredTime_ = TInstant::Now();
}

bool TJob::IsGrowingStale(TDuration maxDelay) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Stored_);

    return LastStoredTime_ + maxDelay <= TInstant::Now();
}

bool TJob::IsJobProxyCompleted() const noexcept
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return JobProxyCompleted_;
}

bool TJob::IsInterruptible() const noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Interruptible_;
}

void TJob::OnJobInterruptionTimeout(
    EInterruptReason interruptionReason,
    const std::optional<TString>& preemptionReason)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto error = TError(NJobProxy::EErrorCode::InterruptionTimeout, "Interruption is timed out")
        << TErrorAttribute("interruption_reason", InterruptionReason_)
        << TErrorAttribute("abort_reason", EAbortReason::InterruptionTimeout);

    if (interruptionReason == EInterruptReason::Preemption) {
        error = TError("Job preempted") << error;
        error = error
            << TErrorAttribute("preemption_reason", preemptionReason)
            << TErrorAttribute("abort_reason", EAbortReason::Preemption);
    }

    Abort(std::move(error));
}

TControllerAgentConnectorPool::TControllerAgentConnectorPtr TJob::GetControllerAgentConnector() const noexcept
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return ControllerAgentConnector_.Lock();
}

void TJob::Interrupt(
    TDuration timeout,
    EInterruptReason interruptionReason,
    const std::optional<TString>& preemptionReason,
    const std::optional<NScheduler::TPreemptedFor>& preemptedFor)
{
    YT_LOG_INFO("Interrupt job (InterruptionReason: %v, PreemptionReason: %v, PreemptedFor: %v, Timeout: %v)",
        interruptionReason,
        preemptionReason,
        preemptedFor,
        timeout);

    try {
        DoInterrupt(timeout, interruptionReason, preemptionReason, preemptedFor);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to interrupt job");
    }
}

NScheduler::EInterruptReason TJob::GetInterruptionReason() const noexcept
{
    return InterruptionReason_;
}

bool TJob::IsInterrupted() const noexcept
{
    return InterruptionReason_ != EInterruptReason::None;
}

const std::optional<NScheduler::TPreemptedFor>& TJob::GetPreemptedFor() const noexcept
{
    return PreemptedFor_;
}

bool TJob::IsFinished() const noexcept
{
    switch (JobState_) {
        case EJobState::Aborted:
        case EJobState::Completed:
        case EJobState::Failed:
            return true;
        default:
            return false;
    }
}

// Helpers.

void TJob::SetJobState(EJobState state)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_DEBUG(
        "Setting new job state (Previous: %v, New: %v)",
        JobState_,
        state);

    JobState_ = state;
    AddJobEvent(state);
}

void TJob::SetJobPhase(EJobPhase phase)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_DEBUG(
        "Setting new job phase (Previous: %v, New: %v)",
        JobPhase_,
        phase);

    JobPhase_ = phase;
    AddJobEvent(phase);
}

void TJob::ValidateJobRunning() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::Running) {
        YT_LOG_DEBUG(
            "Unexpected job phase (Actual: %v, Expected: %v)",
            JobPhase_,
            EJobPhase::Running);

        THROW_ERROR_EXCEPTION(NJobProberClient::EErrorCode::JobIsNotRunning, "Job %v is not running", Id_)
            << TErrorAttribute("job_state", JobState_)
            << TErrorAttribute("job_phase", JobPhase_);
    }
}

void TJob::StartUserJobMonitoring()
{
    if (!UserJobSpec_) {
        return;
    }
    const auto& monitoringConfig = UserJobSpec_->monitoring_config();
    if (!monitoringConfig.enable()) {
        return;
    }
    for (const auto& sensorName : monitoringConfig.sensor_names()) {
        if (!SupportedMonitoringSensors_.contains(sensorName)) {
            THROW_ERROR_EXCEPTION("Unknown user job sensor %Qv", sensorName);
        }
    }
    UserJobSensorProducer_ = New<TBufferedProducer>();
    TProfiler("")
        .WithGlobal()
        .WithRequiredTag("job_descriptor", monitoringConfig.job_descriptor())
        .AddProducer("", UserJobSensorProducer_);
    HandleJobReport(TNodeJobReport()
        .MonitoringDescriptor(monitoringConfig.job_descriptor()));
}

void TJob::ReportJobInterruptionInfo(
    TDuration timeout,
    NScheduler::EInterruptReason interruptionReason,
    const std::optional<TString>& preemptionReason,
    const std::optional<NScheduler::TPreemptedFor>& preemptedFor)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    HandleJobReport(TNodeJobReport()
        .OperationId(OperationId_)
        .InterruptionInfo(TJobInterruptionInfo{
            .InterruptionReason = interruptionReason,
            .InterruptionTimeout = timeout ? std::optional(timeout) : std::nullopt,
            .PreemptionReason = preemptionReason,
            .PreemptedFor = TJobInterruptionInfo::TPreemptedFor{
                .AllocationId = preemptedFor->AllocationId,
                .OperationId = preemptedFor->OperationId,
            },
        }));
}

void TJob::DoSetResult(TError error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    DoSetResult(std::move(error), /*jobResultExtension*/ std::nullopt, /*receivedFromJobProxy*/ false);
}

void TJob::DoSetResult(
    TError error,
    std::optional<NControllerAgent::NProto::TJobResultExt> jobResultExtension,
    bool receivedFromJobProxy)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (Error_ && !Error_->IsOK()) {
        YT_LOG_DEBUG(
            "Job error is already set, do not overwrite (CurrentError: %v, Error: %v)",
            Error_,
            error);
        return;
    }

    YT_VERIFY(!error.IsOK() || jobResultExtension);

    YT_LOG_DEBUG("Set job result (Error: %v)", error);

    if (CommonConfig_->TestJobErrorTruncation) {
        if (!error.IsOK()) {
            for (int index = 0; index < 10; ++index) {
                error.MutableInnerErrors()->push_back(TError("Test error " + ToString(index)));
            }
            YT_LOG_DEBUG(error, "TestJobErrorTruncation");
        }
    }

    JobResultExtension_ = std::nullopt;
    if (jobResultExtension) {
        JobResultExtension_ = std::move(jobResultExtension);
    }

    Error_ = std::move(error).Truncate(
        2,
        16_KB,
        {
            "abort_reason",
            "graceful_abort",
        });

    JobProxyCompleted_ = receivedFromJobProxy;

    FinishTime_ = TInstant::Now();
}

bool TJob::HandleFinishingPhase()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    switch (JobPhase_) {
        case EJobPhase::WaitingForCleanup:
            Cleanup();
            return true;

        case EJobPhase::Cleanup:
        case EJobPhase::Finished:
            return true;

        default:
            return false;
    }
}

void TJob::ValidateJobPhase(EJobPhase expectedPhase) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != expectedPhase) {
        YT_LOG_DEBUG(
            "Unexpected job phase (Actual: %v, Expected: %v)",
            JobPhase_,
            expectedPhase);

        THROW_ERROR_EXCEPTION("Unexpected job phase")
            << TErrorAttribute("expected_phase", expectedPhase)
            << TErrorAttribute("actual_phase", JobPhase_);
    }
}

// Event handlers.
void TJob::OnNodeDirectoryPrepared(TErrorOr<std::unique_ptr<NNodeTrackerClient::NProto::TNodeDirectory>>&& protoNodeDirectoryOrError)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    StartTime_ = TInstant::Now();

    if (auto delay = JobTestingOptions_->DelayAfterNodeDirectoryPrepared) {
        YT_LOG_DEBUG("Simulate delay after node directory prepared");
        TDelayedExecutor::WaitForDuration(*delay);
    }

    GuardedAction(
        "OnNodeDirectoryPrepared",
        [&] {
            ValidateJobPhase(EJobPhase::PreparingNodeDirectory);
            THROW_ERROR_EXCEPTION_IF_FAILED(protoNodeDirectoryOrError,
                NExecNode::EErrorCode::NodeDirectoryPreparationFailed,
                "Failed to prepare job node directory");

            if (auto& protoNodeDirectory = protoNodeDirectoryOrError.Value()) {
                auto* jobSpecExt = JobSpec_.MutableExtension(TJobSpecExt::job_spec_ext);
                jobSpecExt->mutable_input_node_directory()->Swap(protoNodeDirectory.get());
            }

            SetJobPhase(EJobPhase::DownloadingArtifacts);

            auto artifactsFuture = DownloadArtifacts();
            artifactsFuture.Subscribe(
                BIND(&TJob::OnArtifactsDownloaded, MakeWeak(this))
                    .Via(Invoker_));
            ArtifactsFuture_ = artifactsFuture.As<void>();
        });
}

std::vector<TDevice> TJob::GetGpuDevices()
{
    std::vector<TDevice> devices;
    for (const auto& deviceName : Bootstrap_->GetGpuManager()->GetGpuDevices()) {
        bool deviceFound = false;
        for (const auto& slot : GpuSlots_) {
            auto gpuSlot = StaticPointerCast<TGpuSlot>(slot);
            if (gpuSlot->GetDeviceName() == deviceName) {
                deviceFound = true;
                break;
            }
        }

        // We should not explicitly exclude test device that does not actually exists.
        if (!deviceFound && !Bootstrap_->GetGpuManager()->ShouldTestResource()) {
            // Exclude device explicitly.
            devices.emplace_back(TDevice{
                .DeviceName = deviceName,
                .Access = "-"
            });
        }
    }

    return devices;
}

bool TJob::IsFullHostGpuJob() const
{
    return !GpuSlots_.empty() && GpuSlots_.size() == Bootstrap_->GetGpuManager()->GetGpuDevices().size();
}

void TJob::OnArtifactsDownloaded(const TErrorOr<std::vector<NDataNode::IChunkPtr>>& errorOrArtifacts)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction(
        "OnArtifactsDownloaded",
        [&] {
            ValidateJobPhase(EJobPhase::DownloadingArtifacts);
            THROW_ERROR_EXCEPTION_IF_FAILED(errorOrArtifacts, "Failed to download artifacts");

            YT_LOG_INFO("Artifacts downloaded");

            const auto& chunks = errorOrArtifacts.Value();
            for (size_t index = 0; index < Artifacts_.size(); ++index) {
                Artifacts_[index].Chunk = chunks[index];
            }

            CopyFinishTime_ = TInstant::Now();
            RunWithWorkspaceBuilder();
        });
}

void TJob::RunWithWorkspaceBuilder()
{
    VERIFY_THREAD_AFFINITY(JobThread);
    std::vector<TDevice> devices = GetGpuDevices();

    std::vector<TBind> binds;
    auto rootFsBinds = GetRootFsBinds();
    binds.reserve(rootFsBinds.size());

    for (const auto& bind : rootFsBinds) {
        binds.push_back(TBind{
            .SourcePath = bind->ExternalPath,
            .TargetPath = bind->InternalPath,
            .ReadOnly = bind->ReadOnly
        });
    }

    TUserSandboxOptions options = BuildUserSandboxOptions();

    TJobWorkspaceBuildingContext context{
        .Logger = Logger,
        .UserSandboxOptions = options,
        .Slot = GetUserSlot(),
        .Job = MakeStrong(this),
        .CommandUser = CommonConfig_->SetupCommandUser,

        .ArtifactDownloadOptions = MakeArtifactDownloadOptions(),

        .Artifacts = Artifacts_,
        .Binds = binds,
        .LayerArtifactKeys = LayerArtifactKeys_,
        .SetupCommands = GetSetupCommands(),
        .DockerImage = DockerImage_,
        .DockerAuth = BuildDockerAuthConfig(),

        .NeedGpuCheck = NeedsGpuCheck(),
        .GpuCheckBinaryPath = UserJobSpec_
            ? std::make_optional(UserJobSpec_->gpu_check_binary_path())
            : std::nullopt,
        .GpuCheckBinaryArgs = UserJobSpec_
            ? std::make_optional(FromProto<std::vector<TString>>(UserJobSpec_->gpu_check_binary_args()))
            : std::optional<std::vector<TString>>(),
        .GpuCheckType = EGpuCheckType::Preliminary,
        .GpuDevices = devices
    };

    auto workspaceBuilder = GetUserSlot()->CreateJobWorkspaceBuilder(
        Invoker_,
        std::move(context));

    workspaceBuilder->SubscribeUpdateArtifactStatistics(BIND_NO_PROPAGATE([this, this_ = MakeWeak(this)] (i64 compressedDataSize, bool cacheHit) {
        UpdateArtifactStatistics(compressedDataSize, cacheHit);
    })
        .Via(Invoker_));

    // TODO(pogorelov): Refactor it. Phase should be changed in callback, not in signal handler.
    // We intentionally subscribe here without Via(Invoker_) to prevent data race.
    workspaceBuilder->SubscribeUpdateBuilderPhase(BIND_NO_PROPAGATE([this, this_ = MakeWeak(this)] (EJobPhase phase) {
        VERIFY_THREAD_AFFINITY(JobThread);

        SetJobPhase(phase);
    }));

    // TODO(pogorelov): Do not pass TJobWorkspaceBuilderPtr, define structure.
    workspaceBuilder->SubscribeUpdateTimers(BIND_NO_PROPAGATE([this, this_ = MakeWeak(this)] (const TJobWorkspaceBuilderPtr& workspace) {
        PreliminaryGpuCheckStartTime_ = workspace->GetGpuCheckStartTime();
        PreliminaryGpuCheckFinishTime_ = workspace->GetGpuCheckFinishTime();

        StartPrepareVolumeTime_ = workspace->GetVolumePrepareStartTime();
        FinishPrepareVolumeTime_ = workspace->GetVolumePrepareFinishTime();
    })
        .Via(Invoker_));

    auto workspaceFuture = workspaceBuilder->Run();
    workspaceFuture.Subscribe(BIND(&TJob::OnWorkspacePreparationFinished, MakeStrong(this))
        .Via(Invoker_));
    WorkspaceBuildingFuture_ = workspaceFuture.AsVoid();
}

void TJob::OnWorkspacePreparationFinished(const TErrorOr<TJobWorkspaceBuildingResult>& resultOrError)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (auto delay = JobTestingOptions_->DelayBeforeSpawningJobProxy) {
        YT_LOG_DEBUG("Simulate delay before spawning job proxy");
        TDelayedExecutor::WaitForDuration(*delay);
    }

    GuardedAction(
        "OnWorkspacePreparationFinished",
        [&] {
            // There may be a possible cancellation, but this is not happening now.
            YT_VERIFY(resultOrError.IsOK());

            auto& result = resultOrError.Value();
            TmpfsPaths_ = result.TmpfsPaths;
            RootVolume_ = result.RootVolume;
            // Workspace builder may add or replace docker image.
            DockerImage_ = result.DockerImage;
            SetupCommandCount_ = result.SetupCommandCount;

            THROW_ERROR_EXCEPTION_IF_FAILED(
                result.LastBuildError,
                "Job preparation failed");

            RunJobProxy();
        });
}

void TJob::OnExtraGpuCheckCommandFinished(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    ExtraGpuCheckFinishTime_ = TInstant::Now();

    if (HandleFinishingPhase()) {
        return;
    }

    ValidateJobPhase(EJobPhase::RunningExtraGpuCheckCommand);

    YT_LOG_FATAL_IF(
        !Error_ || Error_->IsOK(),
        "Job error is not set during running extra GPU check (Error: %v)",
        Error_);

    auto initialError = std::move(*Error_);

    if (!error.IsOK()) {
        // Reset Error_ to set it with checkError
        Error_ = {};
        JobResultExtension_.reset();

        auto checkError = TError(NExecNode::EErrorCode::GpuCheckCommandFailed, "Extra GPU check command failed")
            << error
            << initialError;

        YT_LOG_WARNING(checkError, "Extra GPU check command executed after job failure is also failed");
        Finalize(std::move(checkError));
    } else {
        YT_LOG_DEBUG("Extra GPU check command finished");

        Finalize(std::move(initialError));
    }

    Cleanup();
}

void TJob::RunJobProxy()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::RunningSetupCommands &&
        JobPhase_ != EJobPhase::RunningGpuCheckCommand) {
        YT_LOG_ALERT("Unexpected phase before run job proxy (ActualPhase: %v)", JobPhase_);
    }

    ExecStartTime_ = TInstant::Now();

    SetJobPhase(EJobPhase::SpawningJobProxy);
    InitializeJobProbe();

    BIND(
        &IUserSlot::RunJobProxy,
        GetUserSlot(),
        CreateConfig(),
        Id_,
        OperationId_)
    .AsyncVia(Invoker_)
    .Run()
    .Subscribe(BIND(
        &TJob::OnJobProxyFinished,
        MakeWeak(this))
    .Via(Invoker_));

    TDelayedExecutor::Submit(
        BIND(
            &TJob::OnJobProxyPreparationTimeout,
            MakeWeak(this))
        .Via(Invoker_),
        CommonConfig_->JobProxyPreparationTimeout);
}

void TJob::OnJobProxyPreparationTimeout()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(JobPhase_ >= EJobPhase::SpawningJobProxy);

    if (JobPhase_ == EJobPhase::PreparingJob) {
        YT_LOG_INFO("Job proxy preparation timeout");

        Abort(TError(
            NExecNode::EErrorCode::JobProxyPreparationTimeout,
            "Failed to prepare job proxy within timeout, aborting job"));
    }
}

void TJob::OnJobPreparationTimeout(TDuration prepareTimeLimit, bool fatal)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ < EJobPhase::Running) {
        auto error = TError(
            fatal ? NExecNode::EErrorCode::FatalJobPreparationTimeout : NExecNode::EErrorCode::JobPreparationTimeout,
            "Failed to prepare job within timeout")
            << TErrorAttribute("prepare_time_limit", prepareTimeLimit)
            << TErrorAttribute("job_creation_time", CreationTime_)
            << TErrorAttribute("job_phase", JobPhase_);

        if (fatal) {
            Fail(std::move(error));
        } else {
            Abort(std::move(error));
        }
    }
}

void TJob::OnWaitingForCleanupTimeout()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ == EJobPhase::WaitingForCleanup) {
        auto timeout = CommonConfig_->WaitingForJobCleanupTimeout;

        auto error = TError("Failed to wait for job cleanup within timeout")
            << TErrorAttribute("job_id", Id_)
            << TErrorAttribute("operation_id", OperationId_)
            << TErrorAttribute("waiting_for_job_cleanup_timeout", timeout);
        Bootstrap_->GetSlotManager()->Disable(error);
    }
}

IUserSlotPtr TJob::GetUserSlot() const
{
    return StaticPointerCast<IUserSlot>(UserSlot_);
}

void TJob::OnJobProxyFinished(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO(error, "Job proxy finished");

    ResetJobProbe();

    if (HandleFinishingPhase()) {
        return;
    }

    const auto& currentError = Error_
        ? *Error_
        : TError();
    if (!currentError.IsOK() && NeedsGpuCheck()) {
        SetJobPhase(EJobPhase::RunningExtraGpuCheckCommand);

        TJobGpuCheckerContext context {
            .Slot = GetUserSlot(),
            .Job = MakeStrong(this),
            .RootFS = MakeWritableRootFS(),
            .CommandUser = CommonConfig_->SetupCommandUser,

            .GpuCheckBinaryPath = UserJobSpec_->gpu_check_binary_path(),
            .GpuCheckBinaryArgs = FromProto<std::vector<TString>>(UserJobSpec_->gpu_check_binary_args()),
            .GpuCheckType = EGpuCheckType::Extra,
            .CurrentStartIndex = SetupCommandCount_,
            .TestExtraGpuCheckCommandFailure = Bootstrap_->GetGpuManager()->ShouldTestExtraGpuCheckCommandFailure(),
            .GpuDevices = GetGpuDevices()
        };

        auto checker = New<TJobGpuChecker>(std::move(context), Logger);
        checker->SubscribeRunCheck(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] () {
            ExtraGpuCheckStartTime_ = TInstant::Now();
        })
            .Via(Invoker_));
        checker->SubscribeFinishCheck(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] () {
            ExtraGpuCheckFinishTime_ = TInstant::Now();
        })
            .Via(Invoker_));

        YT_LOG_DEBUG("Running extra GPU check");

        BIND(&TJobGpuChecker::RunGpuCheck, checker)
            .AsyncVia(Invoker_)
            .Run()
            .Subscribe(BIND(
                &TJob::OnExtraGpuCheckCommandFinished,
                MakeWeak(this))
                .Via(Invoker_));
    } else {
        if (!error.IsOK()) {
            Finalize(TError(NExecNode::EErrorCode::JobProxyFailed, "Job proxy failed")
                << BuildJobProxyError(error));
        } else {
            YT_VERIFY(IsFinished());
        }

        Cleanup();
    }
}

template <class TSourceTag, class TCallback>
void TJob::GuardedAction(const TSourceTag& sourceTag, const TCallback& action)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_DEBUG(
        "Run guarded action (State: %v, Phase: %v, Source: %v)",
        JobState_,
        JobPhase_,
        sourceTag);

    if (HandleFinishingPhase()) {
        return;
    }

    try {
        TForbidContextSwitchGuard contextSwitchGuard;
        action();
    } catch (const std::exception& ex) {
        // TODO(pogorelov): This method is called not only in preparation states, do something with log message.
        YT_LOG_WARNING(ex, "Error preparing scheduler job");
        Finalize(ex);
        Cleanup();
    }
}

TFuture<void> TJob::StopJobProxy()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    const auto& slot = GetUserSlot();

    YT_LOG_DEBUG("Clean processes (SlotIndex: %v)", slot->GetSlotIndex());

    return slot->CleanProcesses();
}

// Finalization.
void TJob::Cleanup()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(IsFinished());

    YT_LOG_FATAL_IF(
        JobPhase_ == EJobPhase::Cleanup || JobPhase_ == EJobPhase::Finished,
        "Job cleanup should be called only once");

    if (auto delay = JobTestingOptions_->DelayInCleanup) {
        YT_LOG_DEBUG("Simulate delay in cleanup");

        TDelayedExecutor::WaitForDuration(*delay);

        if (JobPhase_ >= EJobPhase::Cleanup) {
            return;
        }
    }

    YT_LOG_INFO("Clean up after scheduler job");

    TDelayedExecutor::Cancel(InterruptionTimeoutCookie_);

    SetJobPhase(EJobPhase::Cleanup);

    if (const auto& slot = GetUserSlot()) {
        try {
            WaitFor(StopJobProxy())
                .ThrowOnError();
            // TODO(pogorelov): Maybe we should wait until the process is actually stopped?
        } catch (const std::exception& ex) {
            // Errors during cleanup phase do not affect job outcome.
            YT_LOG_ERROR(ex, "Failed to clean processes (SlotIndex: %v)", slot->GetSlotIndex());
        }
    }

    // NodeDirectory can be really huge, we better offload its cleanup.
    // NB: do this after slot cleanup.
    {
        auto* inputNodeDirectory = JobSpec_.MutableExtension(TJobSpecExt::job_spec_ext)
            ->release_input_node_directory();
        NRpc::TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(BIND([inputNodeDirectory] () {
            delete inputNodeDirectory;
        }));
    }

    // Release resources.
    GpuStatistics_.clear();

    if (IsStarted()) {
        ReleaseCumulativeResources();
    }

    if (RootVolume_) {
        auto removeResult = WaitFor(RootVolume_->Remove());
        YT_LOG_ERROR_IF(
            !removeResult.IsOK(),
            removeResult,
            "Volume remove failed (VolumePath: %v)",
            RootVolume_->GetPath());
        RootVolume_.Reset();

        // Clean up NBD exports only if root volume has been created.
        // In this case there is no race between volume destruction and clean up.
        CleanupNbdExports();
    }

    if (const auto& slot = GetUserSlot()) {
        if (ShouldCleanSandboxes()) {
            try {
                YT_LOG_DEBUG("Clean sandbox (SlotIndex: %v)", slot->GetSlotIndex());
                slot->CleanSandbox();
            } catch (const std::exception& ex) {
                // Errors during cleanup phase do not affect job outcome.
                YT_LOG_ERROR(ex, "Failed to clean sandbox (SlotIndex: %v)", slot->GetSlotIndex());
            }
        } else {
            YT_LOG_WARNING("Sandbox cleanup is disabled by environment variable %v; should be used for testing purposes only",
                DisableSandboxCleanupEnv);
        }
    }

    ReleaseResources();

    SetJobPhase(EJobPhase::Finished);

    CleanupFinished_.Set();

    YT_LOG_INFO("Job finished (JobState: %v)", GetState());
}

//! Make sure NBD exports are unregistered in case of volume creation failures.
void TJob::CleanupNbdExports()
{
    if (auto nbdServer = Bootstrap_->GetNbdServer()) {
        for (const auto& artifactKey : LayerArtifactKeys_) {
            if (!artifactKey.has_nbd_export_id()) {
                continue;
            }

            if (nbdServer->IsDeviceRegistered(artifactKey.nbd_export_id())) {
                NNbd::TNbdProfilerCounters::Get()->GetCounter(
                    NNbd::TNbdProfilerCounters::MakeTagSet(artifactKey.data_source().path()),
                    "/device/unregistered_unexpected").Increment(1);

                YT_LOG_ERROR("NBD export is still registered, unregister it (ExportId: %v, Path: %v)",
                    artifactKey.nbd_export_id(),
                    artifactKey.data_source().path());
                nbdServer->TryUnregisterDevice(artifactKey.nbd_export_id());
            }
        }
    }
}

TFuture<void> TJob::GetCleanupFinishedEvent()
{
    return CleanupFinished_
        .ToFuture()
        .ToUncancelable();
}

// Preparation.
std::unique_ptr<NNodeTrackerClient::NProto::TNodeDirectory> TJob::PrepareNodeDirectory()
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& jobSpecExt = JobSpec_.GetExtension(TJobSpecExt::job_spec_ext);
    if (jobSpecExt.has_input_node_directory()) {
        YT_LOG_INFO("Node directory is provided by scheduler");
        return nullptr;
    }

    YT_LOG_INFO("Start preparing node directory");

    const auto& nodeDirectory = Bootstrap_->GetNodeDirectory();

    for (int attempt = 1;; ++attempt) {
        if (JobPhase_ != EJobPhase::PreparingNodeDirectory) {
            break;
        }

        std::optional<TNodeId> unresolvedNodeId;

        auto validateNodeIds = [&] (
            const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
            const TNodeDirectoryPtr& nodeDirectory)
        {
            for (const auto& chunkSpec : chunkSpecs) {
                auto replicas = GetReplicasFromChunkSpec(chunkSpec);
                for (auto replica : replicas) {
                    auto nodeId = replica.GetNodeId();
                    const auto* descriptor = nodeDirectory->FindDescriptor(nodeId);
                    if (!descriptor) {
                        unresolvedNodeId = nodeId;
                        return;
                    }
                }
            }
        };

        auto validateTableSpecs = [&] (const ::google::protobuf::RepeatedPtrField<TTableInputSpec>& tableSpecs) {
            for (const auto& tableSpec : tableSpecs) {
                validateNodeIds(tableSpec.chunk_specs(), nodeDirectory);
            }
        };

        validateTableSpecs(jobSpecExt.input_table_specs());
        validateTableSpecs(jobSpecExt.foreign_input_table_specs());

        // NB: No need to add these descriptors to the input node directory.
        for (const auto& artifact : Artifacts_) {
            validateNodeIds(artifact.Key.chunk_specs(), nodeDirectory);
        }

        for (const auto& artifactKey : LayerArtifactKeys_) {
            validateNodeIds(artifactKey.chunk_specs(), nodeDirectory);
        }

        if (!unresolvedNodeId) {
            break;
        }

        if (attempt >= CommonConfig_->NodeDirectoryPrepareRetryCount) {
            YT_LOG_WARNING("Some node ids were not resolved, skipping corresponding replicas (UnresolvedNodeId: %v)",
                *unresolvedNodeId);
            break;
        }

        YT_LOG_INFO("Unresolved node id found in job spec; backing off and retrying (NodeId: %v, Attempt: %v)",
            *unresolvedNodeId,
            attempt);
        TDelayedExecutor::WaitForDuration(CommonConfig_->NodeDirectoryPrepareBackoffTime);
    }

    auto protoNodeDirectory = std::make_unique<NNodeTrackerClient::NProto::TNodeDirectory>();
    nodeDirectory->DumpTo(protoNodeDirectory.get());

    YT_LOG_INFO("Finish preparing node directory");

    return protoNodeDirectory;
}

std::vector<NJobProxy::TBindConfigPtr> TJob::GetRootFsBinds()
{
    return Bootstrap_->GetConfig()->ExecNode->RootFSBinds;
}

TJobProxyInternalConfigPtr TJob::CreateConfig()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto proxyConfig = CloneYsonStruct(Bootstrap_->GetJobProxyConfigTemplate());
    auto localDescriptor = Bootstrap_->GetLocalDescriptor();
    proxyConfig->DataCenter = localDescriptor.GetDataCenter();
    proxyConfig->Rack = localDescriptor.GetRack();
    proxyConfig->Addresses = localDescriptor.Addresses();

    proxyConfig->LocalHostName = Bootstrap_->GetLocalHostName();

    proxyConfig->BusServer = GetUserSlot()->GetBusServerConfig();

    proxyConfig->TmpfsManager = New<TTmpfsManagerConfig>();
    proxyConfig->TmpfsManager->TmpfsPaths = TmpfsPaths_;

    proxyConfig->MemoryTracker = New<TMemoryTrackerConfig>();
    if (UserJobSpec_) {
        proxyConfig->MemoryTracker->IncludeMemoryMappedFiles = UserJobSpec_->include_memory_mapped_files();
        proxyConfig->MemoryTracker->UseSMapsMemoryTracker = UserJobSpec_->use_smaps_memory_tracker();
    } else {
        proxyConfig->MemoryTracker->IncludeMemoryMappedFiles = true;
        proxyConfig->MemoryTracker->UseSMapsMemoryTracker = false;
    }

    if (UserJobSpec_ && UserJobSpec_->set_container_cpu_limit()) {
        proxyConfig->ContainerCpuLimit = UserJobSpec_->container_cpu_limit();
        if (*proxyConfig->ContainerCpuLimit <= 0) {
            proxyConfig->ContainerCpuLimit = RequestedCpu_;
        }
    }

    if (UserJobSpec_ && UserJobSpec_->slot_container_memory_limit()) {
        proxyConfig->SlotContainerMemoryLimit = UserJobSpec_->slot_container_memory_limit();
    }

    proxyConfig->MemoryTracker->MemoryStatisticsCachePeriod = proxyConfig->MemoryTracker->UseSMapsMemoryTracker
        ? CommonConfig_->SMapsMemoryTrackerCachePeriod
        : CommonConfig_->MemoryTrackerCachePeriod;

    proxyConfig->JobTestingOptions = JobTestingOptions_;
    proxyConfig->SlotIndex = GetUserSlot()->GetSlotIndex();
    proxyConfig->SlotPath = GetUserSlot()->GetSlotPath();

    if (RootVolume_) {
        proxyConfig->RootPath = RootVolume_->GetPath();
    } else {
        // Pass docker image if root volume is not materialized yet.
        proxyConfig->DockerImage = DockerImage_;
    }

    if (RootVolume_ || DockerImage_) {
        proxyConfig->Binds = GetRootFsBinds();

        for (const auto& artifact : Artifacts_) {
            // Artifact is passed into the job via bind.
            if (!artifact.BypassArtifactCache && !artifact.CopyFile) {
                YT_VERIFY(artifact.Chunk);

                YT_LOG_INFO(
                    "Make bind for artifact (FileName: %v, Executable: %v"
                    ", SandboxKind: %v, CompressedDataSize: %v)",
                    artifact.Name,
                    artifact.Executable,
                    artifact.SandboxKind,
                    artifact.Key.GetCompressedDataSize());

                auto sandboxPath = NFS::CombinePaths("/slot", GetSandboxRelPath(artifact.SandboxKind));
                auto targetPath = NFS::CombinePaths(sandboxPath, artifact.Name);

                auto bind = New<TBindConfig>();
                bind->ExternalPath = artifact.Chunk->GetFileName();
                bind->InternalPath = targetPath;
                bind->ReadOnly = true;

                proxyConfig->Binds.push_back(std::move(bind));
            }
        }
    }

    auto tryReplaceSlotIndex = [&] (TString& str) {
        size_t index = str.find(SlotIndexPattern);
        if (index != TString::npos) {
            str.replace(index, SlotIndexPattern.size(), ToString(GetUserSlot()->GetSlotIndex()));
        }
    };

    // This replace logic is used for testing puproses.
    proxyConfig->Logging->UpdateWriters([&] (const IMapNodePtr& writerConfigNode) {
        auto writerConfig = ConvertTo<NLogging::TLogWriterConfigPtr>(writerConfigNode);
        if (writerConfig->Type != NLogging::TFileLogWriterConfig::Type) {
            return writerConfigNode;
        }

        auto fileLogWriterConfig = ConvertTo<NLogging::TFileLogWriterConfigPtr>(writerConfigNode);
        tryReplaceSlotIndex(fileLogWriterConfig->FileName);
        return writerConfig->BuildFullConfig(fileLogWriterConfig);
    });

    if (proxyConfig->StderrPath) {
        tryReplaceSlotIndex(*proxyConfig->StderrPath);
    }

    if (proxyConfig->ExecutorStderrPath) {
        tryReplaceSlotIndex(*proxyConfig->ExecutorStderrPath);
    }

    for (const auto& gpuSlot : GpuSlots_) {
        auto slot = StaticPointerCast<TGpuSlot>(gpuSlot);
        proxyConfig->GpuIndexes.push_back(slot->GetDeviceIndex());
    }

    proxyConfig->MakeRootFSWritable = UserJobSpec_ && UserJobSpec_->make_rootfs_writable();

    std::vector<TIP6Address> ipAddresses;
    ipAddresses.reserve(ResolvedNodeAddresses_.size());

    if (NetworkProjectId_) {
        for (const auto& [addressName, address] : ResolvedNodeAddresses_) {
            auto networkAddress = New<TUserJobNetworkAddress>();
            networkAddress->Address = TMtnAddress{address}
                .SetProjectId(*NetworkProjectId_)
                .SetHost(GetUserSlot()->GetSlotIndex())
                .ToIP6Address();
            networkAddress->Name = addressName;

            proxyConfig->NetworkAddresses.push_back(networkAddress);
            ipAddresses.push_back(networkAddress->Address);
        }

        if (proxyConfig->NetworkAddresses.empty()) {
            THROW_ERROR_EXCEPTION("No IPv6 node addresses were resolved");
        }

        if (UserJobSpec_ && UserJobSpec_->has_enable_nat64()) {
            proxyConfig->EnableNat64 = UserJobSpec_->enable_nat64();
        }

        if (UserJobSpec_ && UserJobSpec_->has_disable_network()) {
            proxyConfig->DisableNetwork = UserJobSpec_->disable_network();
        }

        proxyConfig->HostName = Format("slot_%v.%v",
            GetUserSlot()->GetSlotIndex(),
            Bootstrap_->GetConfig()->Addresses[0].second);
    } else {
        for (const auto& [addressName, address] : ResolvedNodeAddresses_) {
            ipAddresses.push_back(address);
        }
    }

    {
        auto userSlot = GetUserSlot();
        ExecAttributes_.SlotIndex = userSlot->GetSlotIndex();
        ExecAttributes_.SandboxPath = userSlot->GetSandboxPath(ESandboxKind::User);
        ExecAttributes_.MediumName = userSlot->GetMediumName();

        ExecAttributes_.JobProxySocketPath = userSlot->GetJobProxyUnixDomainSocketPath();

        ExecAttributes_.IPAddresses.reserve(ipAddresses.size());
        for (const auto& address : ipAddresses) {
            ExecAttributes_.IPAddresses.push_back(ToString(address));
        }

        ExecAttributes_.GpuDevices.reserve(GpuSlots_.size());
        for (const auto& gpuSlot : GpuSlots_) {
            auto slot = StaticPointerCast<TGpuSlot>(gpuSlot);
            auto& gpuDevice = ExecAttributes_.GpuDevices.emplace_back(New<TGpuDevice>());
            gpuDevice->DeviceNumber = slot->GetDeviceIndex();
            gpuDevice->DeviceName = slot->GetDeviceName();
        }
    }

    if (auto proxyDynamicConfig = Bootstrap_->GetJobController()->GetJobProxyDynamicConfig()) {
        proxyConfig->Jaeger = proxyConfig->Jaeger->ApplyDynamic(proxyDynamicConfig->Jaeger);
        proxyConfig->EnableJobShellSeccopm = proxyDynamicConfig->EnableJobShellSeccopm;
        proxyConfig->UsePortoKillForSignalling = proxyDynamicConfig->UsePortoKillForSignalling;
        proxyConfig->ForceIdleCpuPolicy = proxyDynamicConfig->ForceIdleCpuPolicy;
        proxyConfig->AbortOnUncaughtException = proxyDynamicConfig->AbortOnUncaughtException;
        proxyConfig->EnableStderrAndCoreLivePreview = proxyDynamicConfig->EnableStderrAndCoreLivePreview;
        if (proxyDynamicConfig->JobEnvironment) {
            proxyConfig->JobEnvironment = PatchNode(proxyConfig->JobEnvironment, proxyDynamicConfig->JobEnvironment);
        }
    }

    proxyConfig->JobThrottler = CloneYsonStruct(CommonConfig_->JobThrottler);
    if (!JobSpecExt_->enable_prefetching_job_throttler()) {
        proxyConfig->JobThrottler->BandwidthPrefetch->Enable = false;
        proxyConfig->JobThrottler->RpsPrefetch->Enable = false;
    }
    YT_LOG_DEBUG("Initialize prefetching job throttler (DynamicConfigEnable: %v, JobSpecEnable: %v, PrefetchEnable: %v)",
        CommonConfig_->JobThrottler->BandwidthPrefetch->Enable,
        JobSpecExt_->enable_prefetching_job_throttler(),
        proxyConfig->JobThrottler->BandwidthPrefetch->Enable);

    proxyConfig->StatisticsOutputTableCountLimit = CommonConfig_->StatisticsOutputTableCountLimit;

    return proxyConfig;
}

NCri::TCriAuthConfigPtr TJob::BuildDockerAuthConfig()
{
    if (UserJobSpec_ && UserJobSpec_->environment_size()) {
        for (const auto& var : UserJobSpec_->environment()) {
            if (var.StartsWith(DockerAuthEnvPrefix)) {
                auto ysonConfig = TYsonString(var.substr(DockerAuthEnvPrefix.length()));
                return ConvertTo<NCri::TCriAuthConfigPtr>(ysonConfig);
            }
        }
    }

    return nullptr;
}

TUserSandboxOptions TJob::BuildUserSandboxOptions()
{
    TUserSandboxOptions options;
    // NB: this eventually results in job graceful abort.
    options.DiskOverdraftCallback = BIND(&TJob::Fail, MakeWeak(this))
        .Via(Invoker_);
    options.HasRootFSQuota = false;
    options.EnableDiskQuota = Bootstrap_->GetConfig()->DataNode->VolumeManager->EnableDiskQuota;
    options.UserId = GetUserSlot()->GetUserId();

    if (UserJobSpec_) {
        for (const auto& tmpfsVolumeProto : UserJobSpec_->tmpfs_volumes()) {
            TTmpfsVolume tmpfsVolume;
            tmpfsVolume.Size = tmpfsVolumeProto.size();
            tmpfsVolume.Path = tmpfsVolumeProto.path();
            options.TmpfsVolumes.push_back(tmpfsVolume);
        }

        // COMPAT(ignat).
        if (UserJobSpec_->has_disk_space_limit()) {
            options.DiskSpaceLimit = UserJobSpec_->disk_space_limit();
        }

        // COMPAT(ignat).
        if (UserJobSpec_->has_inode_limit()) {
            options.InodeLimit = UserJobSpec_->inode_limit();
        }

        if (UserJobSpec_->has_disk_request()) {
            if (UserJobSpec_->disk_request().has_disk_space()) {
                options.DiskSpaceLimit = UserJobSpec_->disk_request().disk_space();
            }
            if (UserJobSpec_->disk_request().has_inode_count()) {
                options.InodeLimit = UserJobSpec_->disk_request().inode_count();
            }
        }

        if (options.DiskSpaceLimit.has_value() && options.DiskSpaceLimit.value() <= 0) {
            THROW_ERROR_EXCEPTION(EErrorCode::QuotaSettingFailed, "Set disk space limit must be greater than 0")
                << TErrorAttribute("disk_space_limit", options.DiskSpaceLimit.value());
        }

        if (options.InodeLimit.has_value() && options.InodeLimit.value() <= 0) {
            THROW_ERROR_EXCEPTION(EErrorCode::QuotaSettingFailed, "Set inode limit must be greater than 0")
                << TErrorAttribute("inode_limit", options.InodeLimit.value());
        }
    }

    return options;
}

// Build artifacts.
void TJob::InitializeArtifacts()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (UserJobSpec_) {
        for (const auto& descriptor : UserJobSpec_->files()) {
            Artifacts_.push_back(TArtifact{
                ESandboxKind::User,
                descriptor.file_name(),
                descriptor.executable(),
                descriptor.bypass_artifact_cache(),
                descriptor.copy_file(),
                TArtifactKey(descriptor),
                nullptr
            });
            YT_VERIFY(UserArtifactNameToIndex_.emplace(descriptor.file_name(), Artifacts_.size() - 1).second);
        }

        bool needGpuLayers = NeedGpuLayers() || Bootstrap_->GetGpuManager()->ShouldTestLayers();

        if (needGpuLayers && UserJobSpec_->enable_gpu_layers()) {
            if (UserJobSpec_->layers().empty()) {
                THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::GpuJobWithoutLayers,
                    "No layers specified for GPU job; at least a base layer is required to use GPU");
            }

            for (auto&& layerKey : Bootstrap_->GetGpuManager()->GetToppingLayers()) {
                LayerArtifactKeys_.push_back(std::move(layerKey));
            }
        }

        for (const auto& layerKey : UserJobSpec_->layers()) {
            LayerArtifactKeys_.emplace_back(layerKey);
        }

        // Mark NBD layers with NBD export ids.
        auto nbdExportCount = 0;
        for (auto& layer : LayerArtifactKeys_) {
            if (FromProto<ELayerAccessMethod>(layer.access_method()) == ELayerAccessMethod::Nbd) {
                auto nbdExportId = MakeNbdExportId(Id_, nbdExportCount);
                layer.set_nbd_export_id(ToString(nbdExportId));
                ++nbdExportCount;
            }
        }

        if (UserJobSpec_->has_docker_image()) {
            DockerImage_ = UserJobSpec_->docker_image();
        }
    }

    if (JobSpecExt_->has_input_query_spec()) {
        const auto& querySpec = JobSpecExt_->input_query_spec();
        for (const auto& function : querySpec.external_functions()) {
            TArtifactKey key;
            key.mutable_data_source()->set_type(static_cast<int>(EDataSourceType::File));

            for (const auto& chunkSpec : function.chunk_specs()) {
                *key.add_chunk_specs() = chunkSpec;
            }

            Artifacts_.push_back(TArtifact{
                ESandboxKind::Udf,
                function.name(),
                false,
                false,
                false,
                key,
                nullptr
            });
        }
    }
}

TArtifactDownloadOptions TJob::MakeArtifactDownloadOptions() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    std::vector<TString> workloadDescriptorAnnotations = {
        Format("OperationId: %v", OperationId_),
        Format("JobId: %v", Id_),
        Format("AuthenticatedUser: %v", JobSpecExt_->authenticated_user()),
    };

    auto options = TArtifactDownloadOptions{
        .TrafficMeter = TrafficMeter_,
    };

    return options;
}

// Start async artifacts download.
TFuture<std::vector<NDataNode::IChunkPtr>> TJob::DownloadArtifacts()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    const auto& chunkCache = Bootstrap_->GetChunkCache();

    std::vector<TFuture<IChunkPtr>> asyncChunks;
    for (const auto& artifact : Artifacts_) {
        i64 artifactSize = artifact.Key.GetCompressedDataSize();
        if (artifact.BypassArtifactCache) {
            ChunkCacheStatistics_.CacheBypassedArtifactsSize += artifactSize;
            asyncChunks.push_back(MakeFuture<IChunkPtr>(nullptr));
            continue;
        }

        YT_LOG_INFO("Download user file (FileName: %v, SandboxKind: %v, CompressedDataSize: %v)",
            artifact.Name,
            artifact.SandboxKind,
            artifact.Key.GetCompressedDataSize());

        auto downloadOptions = MakeArtifactDownloadOptions();
        bool fetchedFromCache = false;
        auto asyncChunk = chunkCache->DownloadArtifact(artifact.Key, downloadOptions, &fetchedFromCache)
            .Apply(BIND([fileName = artifact.Name, this, this_ = MakeStrong(this)] (const TErrorOr<IChunkPtr>& chunkOrError) {
                THROW_ERROR_EXCEPTION_IF_FAILED(chunkOrError,
                    NExecNode::EErrorCode::ArtifactDownloadFailed,
                    "Failed to prepare user file %Qv",
                    fileName);

                const auto& chunk = chunkOrError.Value();
                YT_LOG_INFO("Artifact chunk ready (FileName: %v, LocationId: %v, ChunkId: %v)",
                    fileName,
                    chunk->GetLocation()->GetId(),
                    chunk->GetId());
                return chunk;
            }));

        asyncChunks.push_back(asyncChunk);

        UpdateArtifactStatistics(artifactSize, fetchedFromCache);
    }

    return AllSucceeded(asyncChunks)
        .ToImmediatelyCancelable();
}

TError TJob::BuildJobProxyError(const TError& spawnError)
{
    // Analyse results.
    if (spawnError.IsOK()) {
        return TError();
    }

    auto jobProxyError = TError(
        NExecNode::EErrorCode::JobProxyFailed,
        "Job proxy failed")
        << spawnError;

    if (spawnError.GetCode() == EProcessErrorCode::NonZeroExitCode) {
        // Try to translate the numeric exit code into some human readable reason.
        auto reason = EJobProxyExitCode(spawnError.Attributes().Get<int>("exit_code"));
        const auto& validReasons = TEnumTraits<EJobProxyExitCode>::GetDomainValues();
        if (std::find(validReasons.begin(), validReasons.end(), reason) != validReasons.end()) {
            jobProxyError.MutableAttributes()->Set("reason", reason);
        }
    }

    return jobProxyError;
}

std::optional<EAbortReason> TJob::DeduceAbortReason()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Error_);

    const auto& resultError = *Error_;

    if (JobResultExtension_) {
        const auto& schedulerResultExt = *JobResultExtension_;

        if (!resultError.FindMatching(NNet::EErrorCode::ResolveTimedOut) &&
            !resultError.FindMatching(NChunkClient::EErrorCode::ReaderThrottlingFailed) &&
            !resultError.FindMatching(NTableClient::EErrorCode::NameTableUpdateFailed) &&
            schedulerResultExt.failed_chunk_ids_size() > 0)
        {
            return EAbortReason::FailedChunks;
        }
    }

    // This is most probably user error, still we don't want to make it fatal.
    if (resultError.FindMatching(NExecNode::EErrorCode::LayerUnpackingFailed) ||
        resultError.FindMatching(NExecNode::EErrorCode::DockerImagePullingFailed) ||
        resultError.FindMatching(NExecNode::EErrorCode::InvalidImage))
    {
        return std::nullopt;
    }

    if (resultError.GetCode() == NJobProxy::EErrorCode::UserJobFailed) {
        if (auto innerError = resultError.FindMatching(NExecNode::EErrorCode::AbortByControllerAgent)) {
            return innerError->Attributes().Find<EAbortReason>("abort_reason");
        }
    }

    auto abortReason = resultError.Attributes().Find<EAbortReason>("abort_reason");
    if (abortReason) {
        return *abortReason;
    }

    if (AbortJobIfAccountLimitExceeded_ &&
        resultError.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded))
    {
        return EAbortReason::AccountLimitExceeded;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::ResourceOverdraft)) {
        return EAbortReason::ResourceOverdraft;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::NodeResourceOvercommit)) {
        return EAbortReason::NodeResourceOvercommit;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::WaitingJobTimeout)) {
        return EAbortReason::WaitingTimeout;
    }

    if (resultError.FindMatching(NExecNode::EErrorCode::AbortByScheduler) ||
        resultError.FindMatching(NJobProxy::EErrorCode::JobNotPrepared))
    {
        return EAbortReason::Scheduler;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::ShallowMergeFailed)) {
        return EAbortReason::ShallowMergeFailed;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::InterruptionTimeout)) {
        return EAbortReason::InterruptionTimeout;
    }

    if (resultError.FindMatching(NJobProxy::EErrorCode::InterruptionUnsupported)) {
        return EAbortReason::InterruptionUnsupported;
    }

    if (resultError.FindMatching(NChunkClient::EErrorCode::AllTargetNodesFailed) ||
        resultError.FindMatching(NChunkClient::EErrorCode::ReaderThrottlingFailed) ||
        resultError.FindMatching(NChunkClient::EErrorCode::MasterCommunicationFailed) ||
        resultError.FindMatching(NChunkClient::EErrorCode::MasterNotConnected) ||
        resultError.FindMatching(NChunkClient::EErrorCode::ReaderTimeout) ||
        resultError.FindMatching(NChunkClient::EErrorCode::ChunkBlockFetchFailed) ||
        resultError.FindMatching(NChunkClient::EErrorCode::ChunkMetaFetchFailed) ||
        resultError.FindMatching(NChunkClient::EErrorCode::AutoRepairFailed) ||
        resultError.FindMatching(NExecNode::EErrorCode::ConfigCreationFailed) ||
        resultError.FindMatching(NExecNode::EErrorCode::SlotNotFound) ||
        resultError.FindMatching(NExecNode::EErrorCode::JobEnvironmentDisabled) ||
        resultError.FindMatching(NExecNode::EErrorCode::ArtifactCopyingFailed) ||
        resultError.FindMatching(NExecNode::EErrorCode::ArtifactDownloadFailed) ||
        resultError.FindMatching(NExecNode::EErrorCode::NodeDirectoryPreparationFailed) ||
        resultError.FindMatching(NExecNode::EErrorCode::SlotLocationDisabled) ||
        resultError.FindMatching(NExecNode::EErrorCode::RootVolumePreparationFailed) ||
        resultError.FindMatching(NExecNode::EErrorCode::NotEnoughDiskSpace) ||
        resultError.FindMatching(NJobProxy::EErrorCode::MemoryCheckFailed) ||
        resultError.FindMatching(NContainers::EErrorCode::FailedToStartContainer) ||
        resultError.FindMatching(EProcessErrorCode::CannotResolveBinary) ||
        resultError.FindMatching(NNet::EErrorCode::ResolveTimedOut) ||
        resultError.FindMatching(NExecNode::EErrorCode::JobProxyPreparationTimeout) ||
        resultError.FindMatching(NExecNode::EErrorCode::JobPreparationTimeout) ||
        resultError.FindMatching(NExecNode::EErrorCode::GpuCheckCommandFailed) ||
        resultError.FindMatching(NExecNode::EErrorCode::GpuLayerNotFetched) ||
        resultError.FindMatching(NJobProxy::EErrorCode::JobNotRunning))
    {
        return EAbortReason::Other;
    }

    if (auto jobProxyFailedError = resultError.FindMatching(NExecNode::EErrorCode::JobProxyFailed)) {
        if (auto processError = resultError.FindMatching(EProcessErrorCode::NonZeroExitCode)) {
            auto exitCode = NExecNode::EJobProxyExitCode(processError->Attributes().Get<int>("exit_code"));
            switch (exitCode) {
                case EJobProxyExitCode::HeartbeatFailed:
                case EJobProxyExitCode::ResultReportFailed:
                case EJobProxyExitCode::ResourcesUpdateFailed:
                case EJobProxyExitCode::GetJobSpecFailed:
                case EJobProxyExitCode::InvalidSpecVersion:
                case EJobProxyExitCode::PortoManagementFailed:
                    return EAbortReason::Other;

                case EJobProxyExitCode::ResourceOverdraft:
                    return EAbortReason::ResourceOverdraft;

                default: {
                    if (CommonConfig_->TreatJobProxyFailureAsAbort) {
                        return EAbortReason::JobProxyFailed;
                    }
                    break;
                }
            }
        }
    }

    return std::nullopt;
}

bool TJob::IsFatalError(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return
        error.FindMatching(NTableClient::EErrorCode::SortOrderViolation) ||
        error.FindMatching(NSecurityClient::EErrorCode::AuthenticationError) ||
        error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError) ||
        (
            error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded) &&
            !AbortJobIfAccountLimitExceeded_
        ) ||
        error.FindMatching(NSecurityClient::EErrorCode::NoSuchAccount) ||
        error.FindMatching(NNodeTrackerClient::EErrorCode::NoSuchNetwork) ||
        error.FindMatching(NTableClient::EErrorCode::InvalidDoubleValue) ||
        error.FindMatching(NTableClient::EErrorCode::IncomparableTypes) ||
        error.FindMatching(NTableClient::EErrorCode::UnhashableType) ||
        error.FindMatching(NTableClient::EErrorCode::CorruptedNameTable) ||
        error.FindMatching(NTableClient::EErrorCode::RowWeightLimitExceeded) ||
        error.FindMatching(NTableClient::EErrorCode::InvalidColumnFilter) ||
        error.FindMatching(NTableClient::EErrorCode::InvalidColumnRenaming) ||
        error.FindMatching(NTableClient::EErrorCode::FormatCannotRepresentRow) ||
        error.FindMatching(NExecNode::EErrorCode::SetupCommandFailed) ||
        error.FindMatching(NExecNode::EErrorCode::GpuJobWithoutLayers) ||
        error.FindMatching(NExecNode::EErrorCode::GpuCheckCommandIncorrect) ||
        error.FindMatching(NExecNode::EErrorCode::TmpfsOverflow) ||
        error.FindMatching(NExecNode::EErrorCode::FatalJobPreparationTimeout) ||
        error.FindMatching(NFormats::EErrorCode::InvalidFormat);
}

void TJob::EnrichStatisticsWithGpuInfo(TStatistics* statistics)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    TGpuStatistics aggregatedGpuStatistics;
    i64 totalGpuMemory = 0;

    auto gpuInfoMap = Bootstrap_->GetGpuManager()->GetGpuInfoMap();
    for (int index = 0; index < std::ssize(GpuSlots_); ++index) {
        const auto& gpuSlot = GpuSlots_[index];
        auto slot = StaticPointerCast<TGpuSlot>(gpuSlot);
        auto& [slotStatistics, slotStatisticsLastUpdateTime] = GpuStatistics_[index];

        NGpu::TGpuInfo gpuInfo;
        {
            auto it = gpuInfoMap.find(slot->GetDeviceIndex());
            if (it == gpuInfoMap.end()) {
                continue;
            }
            gpuInfo = it->second;
        }

        if (!slotStatisticsLastUpdateTime) {
            YT_VERIFY(ExecStartTime_);

            slotStatisticsLastUpdateTime = ExecStartTime_;
        }

        auto period = gpuInfo.UpdateTime - *slotStatisticsLastUpdateTime;
        slotStatistics.CumulativeUtilizationGpu += period.MilliSeconds() * gpuInfo.UtilizationGpuRate;
        if (gpuInfo.UtilizationGpuRate > 0) {
            slotStatistics.CumulativeLoad += period.MilliSeconds();
        }
        slotStatistics.CumulativeUtilizationMemory += period.MilliSeconds() * gpuInfo.UtilizationMemoryRate;
        slotStatistics.CumulativeMemory += period.MilliSeconds() * gpuInfo.MemoryUsed;
        slotStatistics.CumulativeMemoryMBSec += static_cast<i64>(period.SecondsFloat() * gpuInfo.MemoryUsed / 1_MB);
        slotStatistics.CumulativeUtilizationPower += period.MilliSeconds() *
            (gpuInfo.PowerLimit > 0
                ? gpuInfo.PowerDraw / gpuInfo.PowerLimit
                : 0.0);
        slotStatistics.CumulativePower += period.MilliSeconds() * gpuInfo.PowerDraw;
        slotStatistics.CumulativeUtilizationClocksSM += period.MilliSeconds() *
            (gpuInfo.ClocksMaxSM > 0
                ? static_cast<double>(gpuInfo.ClocksSM) / gpuInfo.ClocksMaxSM
                : 0.0);
        slotStatistics.CumulativeSMUtilization += period.MilliSeconds() * gpuInfo.SMUtilizationRate;
        slotStatistics.CumulativeSMOccupancy += period.MilliSeconds() * gpuInfo.SMOccupancyRate;
        slotStatistics.NvlinkRxBytes += static_cast<i64>(period.SecondsFloat() * gpuInfo.NvlinkRxByteRate);
        slotStatistics.NvlinkTxBytes += static_cast<i64>(period.SecondsFloat() * gpuInfo.NvlinkTxByteRate);
        slotStatistics.PcieRxBytes += static_cast<i64>(period.SecondsFloat() * gpuInfo.PcieRxByteRate);
        slotStatistics.PcieTxBytes += static_cast<i64>(period.SecondsFloat() * gpuInfo.PcieTxByteRate);
        slotStatistics.MaxMemoryUsed = std::max(slotStatistics.MaxMemoryUsed, gpuInfo.MemoryUsed);
        if (gpuInfo.Stuck.Status && gpuInfo.Stuck.LastTransitionTime) {
            slotStatistics.MaxStuckDuration = std::max(
                slotStatistics.MaxStuckDuration,
                static_cast<i64>((gpuInfo.UpdateTime - *gpuInfo.Stuck.LastTransitionTime).MilliSeconds()));
        }

        YT_LOG_DEBUG(
            "Updated job GPU slot statistics "
            "(GpuInfo: %v, SlotStatistics: %v, SlotStatisticsLastUpdateTime: %v, Period: %v)",
            gpuInfo,
            slotStatistics,
            *slotStatisticsLastUpdateTime,
            period);

        slotStatisticsLastUpdateTime = gpuInfo.UpdateTime;

        aggregatedGpuStatistics.CumulativeUtilizationGpu += slotStatistics.CumulativeUtilizationGpu;
        aggregatedGpuStatistics.CumulativeUtilizationMemory += slotStatistics.CumulativeUtilizationMemory;
        aggregatedGpuStatistics.CumulativeMemory += slotStatistics.CumulativeMemory;
        aggregatedGpuStatistics.CumulativeMemoryMBSec += slotStatistics.CumulativeMemoryMBSec;
        aggregatedGpuStatistics.CumulativeLoad += slotStatistics.CumulativeLoad;
        aggregatedGpuStatistics.CumulativeUtilizationPower += slotStatistics.CumulativeUtilizationPower;
        aggregatedGpuStatistics.CumulativePower += slotStatistics.CumulativePower;
        aggregatedGpuStatistics.CumulativeSMUtilization += slotStatistics.CumulativeSMUtilization;
        aggregatedGpuStatistics.CumulativeSMOccupancy += slotStatistics.CumulativeSMOccupancy;
        aggregatedGpuStatistics.NvlinkRxBytes += slotStatistics.NvlinkRxBytes;
        aggregatedGpuStatistics.NvlinkTxBytes += slotStatistics.NvlinkTxBytes;
        aggregatedGpuStatistics.PcieRxBytes += slotStatistics.PcieRxBytes;
        aggregatedGpuStatistics.PcieTxBytes += slotStatistics.PcieTxBytes;
        aggregatedGpuStatistics.MaxMemoryUsed += slotStatistics.MaxMemoryUsed;
        aggregatedGpuStatistics.MaxStuckDuration = std::max(aggregatedGpuStatistics.MaxStuckDuration, slotStatistics.MaxStuckDuration);
        totalGpuMemory += gpuInfo.MemoryTotal;
    }

    YT_LOG_DEBUG("Updated job aggregate GPU statistics (AggregateGpuStatistics: %v, TotalGpuMemory: %v)",
        aggregatedGpuStatistics,
        totalGpuMemory);

    statistics->AddSample("/user_job/gpu/cumulative_utilization_gpu", aggregatedGpuStatistics.CumulativeUtilizationGpu);
    statistics->AddSample("/user_job/gpu/cumulative_utilization_memory", aggregatedGpuStatistics.CumulativeUtilizationMemory);
    statistics->AddSample("/user_job/gpu/cumulative_utilization_power", aggregatedGpuStatistics.CumulativeUtilizationPower);
    statistics->AddSample("/user_job/gpu/cumulative_memory", aggregatedGpuStatistics.CumulativeMemory);
    statistics->AddSample("/user_job/gpu/cumulative_memory_mb_sec", aggregatedGpuStatistics.CumulativeMemoryMBSec);
    statistics->AddSample("/user_job/gpu/cumulative_power", aggregatedGpuStatistics.CumulativePower);
    statistics->AddSample("/user_job/gpu/cumulative_load", aggregatedGpuStatistics.CumulativeLoad);
    statistics->AddSample("/user_job/gpu/max_memory_used", aggregatedGpuStatistics.MaxMemoryUsed);
    statistics->AddSample("/user_job/gpu/cumulative_sm_utilization", aggregatedGpuStatistics.CumulativeSMUtilization);
    statistics->AddSample("/user_job/gpu/cumulative_sm_occupancy", aggregatedGpuStatistics.CumulativeSMOccupancy);
    statistics->AddSample("/user_job/gpu/nvlink/rx_bytes", aggregatedGpuStatistics.NvlinkRxBytes);
    statistics->AddSample("/user_job/gpu/nvlink/tx_bytes", aggregatedGpuStatistics.NvlinkTxBytes);
    statistics->AddSample("/user_job/gpu/pcie/rx_bytes", aggregatedGpuStatistics.PcieRxBytes);
    statistics->AddSample("/user_job/gpu/pcie/tx_bytes", aggregatedGpuStatistics.PcieTxBytes);
    statistics->AddSample("/user_job/gpu/max_stuck_duration", aggregatedGpuStatistics.MaxStuckDuration);
    statistics->AddSample("/user_job/gpu/memory_total", totalGpuMemory);
}

void TJob::EnrichStatisticsWithRdmaDeviceInfo(TStatistics* statistics)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    TRdmaStatistics aggregatedRdmaStatistics;
    auto rdmaDevices = Bootstrap_->GetGpuManager()->GetRdmaDevices();
    for (const auto& rdmaDevice : rdmaDevices) {
        aggregatedRdmaStatistics.RxByteRate += rdmaDevice.RxByteRate;
        aggregatedRdmaStatistics.TxByteRate += rdmaDevice.TxByteRate;
    }

    statistics->AddSample("/user_job/gpu/rdma/rx_bytes", aggregatedRdmaStatistics.RxByteRate);
    statistics->AddSample("/user_job/gpu/rdma/tx_bytes", aggregatedRdmaStatistics.TxByteRate);
}

void TJob::EnrichStatisticsWithDiskInfo(TStatistics* statistics)
{
    auto diskStatistics = GetUserSlot()->GetDiskStatistics();
    MaxDiskUsage_ = std::max(MaxDiskUsage_, diskStatistics.Usage);
    statistics->AddSample("/user_job/disk/usage", diskStatistics.Usage);
    statistics->AddSample("/user_job/disk/max_usage", MaxDiskUsage_);
    if (diskStatistics.Limit) {
        statistics->AddSample("/user_job/disk/limit", *diskStatistics.Limit);
    }
}

void TJob::EnrichStatisticsWithArtifactsInfo(TStatistics* statistics)
{
    statistics->AddSample("/exec_agent/artifacts/cache_hit_artifacts_size", ChunkCacheStatistics_.CacheHitArtifactsSize);
    statistics->AddSample("/exec_agent/artifacts/cache_miss_artifacts_size", ChunkCacheStatistics_.CacheMissArtifactsSize);
    statistics->AddSample("/exec_agent/artifacts/cache_bypassed_artifacts_size", ChunkCacheStatistics_.CacheBypassedArtifactsSize);
}

void TJob::UpdateIOStatistics(const TStatistics& statistics)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto getStat = [&] (i64 oldValue, const char *path) {
        auto iter = statistics.Data().find(path);
        i64 newValue = iter == statistics.Data().end() ? 0 : iter->second.GetSum();
        if (newValue < oldValue) {
            YT_LOG_WARNING("Job I/O statistic decreased over time (Name: %v, OldValue: %v, NewValue: %v)", path, oldValue, newValue);
            newValue = oldValue;
        }
        return newValue;
    };

    auto newBytesRead = getStat(BytesRead_, "/user_job/block_io/bytes_read");
    auto newBytesWritten = getStat(BytesWritten_, "/user_job/block_io/bytes_written");

    // NB(gepardo): Porto currently calculates only io_total, without making any difference between read and
    // write IO requests. So, we use io_total to estimate both. This place must be corrected when Porto will
    // export read and write IO requests separately (see PORTO-1011 for details).
    auto newIORequestsRead = getStat(IORequestsRead_, "/user_job/block_io/io_total");
    auto newIORequestsWritten = getStat(IORequestsWritten_, "/user_job/block_io/io_total");

    if (Bootstrap_->GetIOTracker()->IsEnabled()) {
        auto processDirection = [&] (const char *direction, i64 byteDelta, i64 ioRequestDelta) {
            if (byteDelta > 0 || ioRequestDelta > 0) {
                Bootstrap_->GetIOTracker()->Enqueue(
                    TIOCounters{
                        .Bytes = byteDelta,
                        .IORequests = ioRequestDelta,
                    },
                    /*tags*/ {
                        {FormatIOTag(EAggregateIOTag::Direction), direction},
                        {FormatIOTag(EAggregateIOTag::User), GetCurrentAuthenticationIdentity().User},
                        {FormatIOTag(EAggregateIOTag::JobIoKind), "user_job"},
                    });
            }
        };

        processDirection("read", newBytesRead - BytesRead_, newIORequestsRead - IORequestsRead_);
        processDirection("write", newBytesWritten - BytesWritten_, newIORequestsWritten - IORequestsWritten_);
    }

    BytesRead_ = newBytesRead;
    BytesWritten_ = newBytesWritten;
    IORequestsRead_ = newIORequestsRead;
    IORequestsWritten_ = newIORequestsWritten;
}

void TJob::UpdateArtifactStatistics(i64 compressedDataSize, bool cacheHit)
{
    if (cacheHit) {
        ChunkCacheStatistics_.CacheHitArtifactsSize += compressedDataSize;
    } else {
        ChunkCacheStatistics_.CacheMissArtifactsSize += compressedDataSize;
    }
}

std::vector<TShellCommandConfigPtr> TJob::GetSetupCommands()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    std::vector<TShellCommandConfigPtr> result;

    auto addIfPresent = [&] (const std::optional<TShellCommandConfigPtr>& command) {
        if (command) {
            result.push_back(*command);
        }
    };

    addIfPresent(CommonConfig_->JobSetupCommand);

    bool needGpu = NeedGpuLayers() || Bootstrap_->GetGpuManager()->ShouldTestSetupCommands();
    if (needGpu) {
        auto gpu_commands = Bootstrap_->GetGpuManager()->GetSetupCommands();
        result.insert(result.end(), gpu_commands.begin(), gpu_commands.end());
    }

    return result;
}

NContainers::TRootFS TJob::MakeWritableRootFS()
{
    VERIFY_THREAD_AFFINITY(JobThread);
    YT_VERIFY(RootVolume_);

    NContainers::TRootFS rootFS;
    auto rootFsBinds = GetRootFsBinds();

    rootFS.RootPath = RootVolume_->GetPath();
    rootFS.IsRootReadOnly = false;
    rootFS.Binds.reserve(rootFsBinds.size());

    for (const auto& bind : rootFsBinds) {
        rootFS.Binds.push_back(TBind{
            .SourcePath = bind->ExternalPath,
            .TargetPath = bind->InternalPath,
            .ReadOnly = bind->ReadOnly
        });
    }

    return rootFS;
}

TNodeJobReport TJob::MakeDefaultJobReport()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto report = TNodeJobReport()
        .Type(GetType())
        .State(GetState())
        .StartTime(GetCreationTime())
        .SpecVersion(0) // TODO: fill correct spec version.
        .CoreInfos(CoreInfos_)
        .ExecAttributes(ConvertToYsonString(ExecAttributes_));
    if (FinishTime_) {
        report.SetFinishTime(*FinishTime_);
    }
    if (JobSpecExt_->has_job_competition_id()) {
        report.SetJobCompetitionId(FromProto<TJobId>(JobSpecExt_->job_competition_id()));
    }
    if (JobSpecExt_->has_probing_job_competition_id()) {
        report.SetProbingJobCompetitionId(FromProto<TJobId>(JobSpecExt_->probing_job_competition_id()));
    }
    if (JobSpecExt_ && JobSpecExt_->has_task_name()) {
        report.SetTaskName(JobSpecExt_->task_name());
    }

    return report;
}


void TJob::InitializeJobProbe()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto probe = CreateJobProbe(GetUserSlot()->GetBusClientConfig());
    {
        auto guard = Guard(JobProbeLock_);
        std::swap(JobProbe_, probe);
    }
}

void TJob::ResetJobProbe()
{
    VERIFY_THREAD_AFFINITY_ANY();

    IJobProbePtr probe;
    {
        auto guard = Guard(JobProbeLock_);
        std::swap(JobProbe_, probe);
    }
}

IJobProbePtr TJob::GetJobProbeOrThrow()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(JobProbeLock_);
    if (!JobProbe_) {
        THROW_ERROR_EXCEPTION("Job probe is not available");
    }
    return JobProbe_;
}

bool TJob::ShouldCleanSandboxes()
{
    return GetEnv(DisableSandboxCleanupEnv) != "1";
}

bool TJob::NeedGpuLayers()
{
    auto jobEnvironmentType = Bootstrap_->GetJobEnvironmentType();
    if (jobEnvironmentType != EJobEnvironmentType::Porto) {
        return false;
    }

    if (JobSpecExt_->has_user_job_spec()) {
        const auto& userJobSpec = JobSpecExt_->user_job_spec();
        if (userJobSpec.has_cuda_toolkit_version()) {
            return true;
        }
    }

    return NeedGpu();
}

bool TJob::NeedGpu()
{
    return GetResourceUsage().Gpu > 0;
}

void TJob::ProfileSensor(const TUserJobSensorPtr& sensor, ISensorWriter* writer, double value)
{
    switch (sensor->Type) {
        case EMetricType::Counter:
            writer->AddCounter(sensor->ProfilingName, std::max<i64>(0, static_cast<i64>(value)));
            return;
        case EMetricType::Gauge:
            writer->AddGauge(sensor->ProfilingName, value);
            return;
        default:
            YT_ABORT();
    }
}

void TJob::ProfileSensor(const TString& sensorName, ISensorWriter* writer, double value)
{
    ProfileSensor(GetOrCrash(SupportedMonitoringSensors_, sensorName), writer, value);
}

void TJob::CollectSensorsFromStatistics(ISensorWriter* writer)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    IMapNodePtr statisticsNode;
    try {
        statisticsNode = ConvertTo<IMapNodePtr>(StatisticsYson_);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(TError(ex), "Failed to convert statistics to map node (JobId: %v, OperationId: %v)",
            GetId(),
            GetOperationId());
        return;
    }

    const auto& monitoringConfig = UserJobSpec_->monitoring_config();
    for (const auto& sensorName : monitoringConfig.sensor_names()) {
        // sensor must be present in config, the check was performed in constructor.
        const auto& sensor = GetOrCrash(SupportedMonitoringSensors_, sensorName);
        if (sensor->Source != EUserJobSensorSource::Statistics) {
            continue;
        }
        INodePtr node;
        try {
            node = FindNodeByYPath(statisticsNode, *(sensor->Path) + "/last");
            if (!node) {
                YT_LOG_DEBUG("Statistics node not found (SensorName: %v, Path: %v)", sensorName, sensor->Path);
                continue;
            }
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(TError(ex), "Error looking for statistics node (SensorName: %v, Path: %v)",
                sensorName,
                sensor->Path);
            continue;
        }
        if (node->GetType() != ENodeType::Int64) {
            YT_LOG_DEBUG("Wrong type of sensor (SensorName: %v, ExpectedType: %v, ActualType: %v)",
                sensorName,
                ENodeType::Int64,
                node->GetType());
            continue;
        }
        ProfileSensor(sensor, writer, node->AsInt64()->GetValue());
    }
}

void TJob::CollectSensorsFromGpuAndRdmaDeviceInfo(ISensorWriter* writer)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (GpuSlots_.empty()) {
        return;
    }

    const auto& monitoringConfig = UserJobSpec_->monitoring_config();
    auto sensorNames = THashSet<TString>(
        std::begin(monitoringConfig.sensor_names()),
        std::end(monitoringConfig.sensor_names()));

    static const TString UtilizationGpuName = "gpu/utilization_gpu";
    static const TString UtilizationMemoryName = "gpu/utilization_memory";
    static const TString UtilizationPowerName = "gpu/utilization_power";

    static const TString MemoryName = "gpu/memory";
    static const TString PowerName = "gpu/power";

    static const TString SMUtilizationName = "gpu/sm_utilization";
    static const TString SMOccupancyName = "gpu/sm_occupancy";

    static const TString NvlinkRxBytesName = "gpu/nvlink/rx_bytes";
    static const TString NvlinkTxBytesName = "gpu/nvlink/tx_bytes";
    static const TString PcieRxBytesName = "gpu/pcie/rx_bytes";
    static const TString PcieTxBytesName = "gpu/pcie/tx_bytes";

    static const TString StuckName = "gpu/stuck";

    static const TString RxBytesName = "gpu/rdma/rx_bytes";
    static const TString TxBytesName = "gpu/rdma/tx_bytes";

    auto profileSensorIfNeeded = [&] (const TString& name, double value) {
        if (sensorNames.contains(name)) {
            ProfileSensor(name, writer, value);
        }
    };

    auto gpuInfoMap = Bootstrap_->GetGpuManager()->GetGpuInfoMap();
    for (int index = 0; index < std::ssize(GpuSlots_); ++index) {
        const auto& gpuSlot = GpuSlots_[index];
        auto slot = StaticPointerCast<TGpuSlot>(gpuSlot);

        auto it = gpuInfoMap.find(slot->GetDeviceIndex());
        if (it == gpuInfoMap.end()) {
            continue;
        }

        const auto& gpuInfo = it->second;

        TWithTagGuard tagGuard(writer, "gpu_slot", ToString(index));

        profileSensorIfNeeded(UtilizationGpuName, gpuInfo.UtilizationGpuRate);
        profileSensorIfNeeded(UtilizationMemoryName, gpuInfo.UtilizationMemoryRate);
        profileSensorIfNeeded(MemoryName, gpuInfo.MemoryUsed);
        profileSensorIfNeeded(
            UtilizationPowerName,
            gpuInfo.PowerLimit == 0.0
                ? 0.0
                : gpuInfo.PowerDraw / gpuInfo.PowerLimit);
        profileSensorIfNeeded(PowerName, gpuInfo.PowerDraw);
        profileSensorIfNeeded(SMUtilizationName, gpuInfo.SMUtilizationRate);
        profileSensorIfNeeded(SMOccupancyName, gpuInfo.SMOccupancyRate);
        profileSensorIfNeeded(NvlinkRxBytesName, gpuInfo.NvlinkRxByteRate);
        profileSensorIfNeeded(NvlinkTxBytesName, gpuInfo.NvlinkTxByteRate);
        profileSensorIfNeeded(PcieRxBytesName, gpuInfo.PcieRxByteRate);
        profileSensorIfNeeded(PcieTxBytesName, gpuInfo.PcieTxByteRate);
        profileSensorIfNeeded(StuckName, static_cast<double>(gpuInfo.Stuck.Status));
    }

    if (IsFullHostGpuJob()) {
        auto rdmaDevices = Bootstrap_->GetGpuManager()->GetRdmaDevices();
        for (const auto& rdmaDevice : rdmaDevices) {
            TWithTagGuard tagGuard(writer, "rdma_device", rdmaDevice.Name);

            profileSensorIfNeeded(RxBytesName, rdmaDevice.RxByteRate);
            profileSensorIfNeeded(TxBytesName, rdmaDevice.TxByteRate);
        }
    }
}

TFuture<TSharedRef> TJob::DumpSensors()
{
    auto jobProbe = GetJobProbeOrThrow();

    return BIND([jobProbe] {
        return jobProbe->DumpSensors();
    })
        .AsyncVia(Invoker_)
        .Run()
        .WithTimeout(CommonConfig_->SensorDumpTimeout);
}

bool TJob::NeedsGpuCheck() const
{
    return UserJobSpec_ && UserJobSpec_->has_gpu_check_binary_path();
}

////////////////////////////////////////////////////////////////////////////////

TJobPtr CreateJob(
    TJobId jobId,
    TOperationId operationId,
    const NClusterNode::TJobResources& resourceUsage,
    const NClusterNode::TJobResourceAttributes& resourceAttributes,
    TJobSpec&& jobSpec,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap,
    const TJobCommonConfigPtr& commonConfig)
{
    return NewWithOffloadedDtor<TJob>(
        bootstrap->GetJobInvoker(),
        jobId,
        operationId,
        resourceUsage,
        resourceAttributes,
        std::move(jobSpec),
        std::move(agentDescriptor),
        bootstrap,
        commonConfig);
}

////////////////////////////////////////////////////////////////////////////////

void FillStatus(NScheduler::NProto::TAllocationStatus* status, const TJobPtr& job)
{
    using NYT::ToProto;

    ToProto(status->mutable_allocation_id(), job->GetAllocationId());

    status->set_state(ToProto<int>(JobStateToAllocationState(job->GetState())));
}

void FillStatus(NControllerAgent::NProto::TJobStatus* status, const TJobPtr& job)
{
    using NYT::ToProto;

    ToProto(status->mutable_job_id(), job->GetId());
    status->set_job_type(ToProto<int>(job->GetType()));
    status->set_state(ToProto<int>(job->GetState()));
    status->set_phase(ToProto<int>(job->GetPhase()));
    status->set_job_execution_completed(job->IsJobProxyCompleted());
    status->set_interruption_reason(ToProto<int>(job->GetInterruptionReason()));
    status->set_progress(job->GetProgress());
    *status->mutable_total_input_data_statistics() = job->GetTotalInputDataStatistics();
    ToProto(status->mutable_time_statistics(), job->GetTimeStatistics());

    ToProto(status->mutable_output_data_statistics(), job->GetOutputDataStatistics());

    if (auto stderrSize = job->GetStderrSize(); stderrSize > 0) {
        status->set_stderr_size(stderrSize);
    }
    if (const auto& preemptedFor = job->GetPreemptedFor()) {
        ToProto(status->mutable_preempted_for(), *preemptedFor);
    }
    if (auto startTime = job->GetStartTime()) {
        status->set_start_time(startTime->GetValue());
    }
}

template <class TStatus>
void FillJobStatus(TStatus* status, const TJobPtr& job)
{
    FillStatus(status, job);

    ToProto(status->mutable_operation_id(), job->GetOperationId());

    status->set_status_timestamp(ToProto<ui64>(TInstant::Now()));
}

template void FillJobStatus(NScheduler::NProto::TAllocationStatus* status, const TJobPtr& job);
template void FillJobStatus(NControllerAgent::NProto::TJobStatus* status, const TJobPtr& job);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
