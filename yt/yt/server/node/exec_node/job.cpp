#include "job.h"

#include "bootstrap.h"
#include "chunk_cache.h"
#include "controller_agent_connector.h"
#include "job_controller.h"
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

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/server/lib/job_agent/job_reporter.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/job_prober_client/public.h>
#include <yt/yt/ytlib/job_prober_client/job_probe.h>

#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/job_tracker_client/statistics.h>

#include <yt/yt/ytlib/job_proxy/config.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/core/rpc/dispatcher.h>

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
using namespace NJobTrackerClient;
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

using NNodeTrackerClient::TNodeDirectory;
using NChunkClient::TDataSliceDescriptor;

using NObjectClient::TypeFromId;
using NCypressClient::EObjectType;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto DisableSandboxCleanupEnv = "YT_DISABLE_SANDBOX_CLEANUP";

static const TString SlotIndexPattern("\%slot_index\%");

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    TJobId jobId,
    TOperationId operationId,
    const TNodeResources& resourceUsage,
    TJobSpec&& jobSpec,
    IBootstrap* bootstrap,
    TControllerAgentDescriptor agentDescriptor)
    : TResourceHolder(
        bootstrap->GetJobResourceManager().Get(),
        EResourcesConsumerType::SchedulerJob,
        ExecNodeLogger.WithTag(
            "JobId: %v, OperationId: %v, JobType: %v",
            jobId,
            operationId,
            CheckedEnumCast<EJobType>(jobSpec.type())),
        resourceUsage,
        jobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext).user_job_spec().port_count())
    , Id_(jobId)
    , OperationId_(operationId)
    , Bootstrap_(bootstrap)
    , ControllerAgentDescriptor_(std::move(agentDescriptor))
    , ControllerAgentConnector_(
        Bootstrap_->GetControllerAgentConnectorPool()->GetControllerAgentConnector(this))
    , Config_(Bootstrap_->GetConfig()->ExecNode)
    , DynamicConfig_(Bootstrap_->GetDynamicConfig()->ExecNode)
    , Invoker_(Bootstrap_->GetJobInvoker())
    , StartTime_(TInstant::Now())
    , TrafficMeter_(New<TTrafficMeter>(
        Bootstrap_->GetLocalDescriptor().GetDataCenter()))
    , JobSpec_(std::move(jobSpec))
    , SchedulerJobSpecExt_(&JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext))
    , UserJobSpec_(SchedulerJobSpecExt_ && SchedulerJobSpecExt_->has_user_job_spec() ? &SchedulerJobSpecExt_->user_job_spec() : nullptr)
    , JobTestingOptions_(SchedulerJobSpecExt_ && SchedulerJobSpecExt_->has_testing_options()
        ? ConvertTo<TJobTestingOptionsPtr>(TYsonString(SchedulerJobSpecExt_->testing_options()))
        : New<TJobTestingOptions>())
    , Interruptible_(SchedulerJobSpecExt_->interruptible())
    , AbortJobIfAccountLimitExceeded_(SchedulerJobSpecExt_->abort_job_if_account_limit_exceeded())
    , IsGpuRequested_(resourceUsage.gpu() > 0)
    , RequestedCpu_(resourceUsage.cpu())
    , RequestedMemory_(resourceUsage.user_memory())
    , TraceContext_(CreateTraceContextFromCurrent("Job"))
    , FinishGuard_(TraceContext_)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    PackBaggageFromJobSpec(TraceContext_, JobSpec_, OperationId_, Id_);

    SupportedMonitoringSensors_ = TUserJobMonitoringConfig::GetDefaultSensors();
    for (const auto& [sensorName, sensor] : Config_->UserJobMonitoring->Sensors) {
        SupportedMonitoringSensors_[sensorName] = sensor;
    }
    for (const auto& [sensorName, sensor] : DynamicConfig_->UserJobMonitoring->Sensors) {
        SupportedMonitoringSensors_[sensorName] = sensor;
    }

    TrafficMeter_->Start();

    AddJobEvent(JobState_, JobPhase_);

    if (UserJobSpec_ && UserJobSpec_->has_network_project_id()) {
        auto addresses = Bootstrap_->GetConfig()->Addresses;
        ResolvedNodeAddresses_.reserve(addresses.size());
        for (const auto& [addressName, address] : addresses) {
            auto* resolver = TAddressResolver::Get();
            auto resolvedAddress = WaitFor(resolver->Resolve(address))
                .ValueOrThrow();
            YT_VERIFY(resolvedAddress.IsIP6());
            ResolvedNodeAddresses_.emplace_back(addressName, resolvedAddress.ToIP6Address());
        }
    }

    HandleJobReport(MakeDefaultJobReport()
        .TreeId(SchedulerJobSpecExt_->tree_id()));
}

TJob::~TJob()
{
    // Offload job spec destruction to a large thread pool.
    auto jobSpec = std::make_unique<TJobSpec>(std::move(JobSpec_));
    NRpc::TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(
        BIND([jobSpec = std::move(jobSpec)] () mutable { jobSpec.reset(); }));
}

void TJob::Start()
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

    GuardedAction([&] () {
        StartUserJobMonitoring();

        SetJobState(EJobState::Running);

        auto now = TInstant::Now();
        PrepareTime_ = now;

        YT_LOG_INFO("Starting job");

        InitializeArtifacts();

        NScheduler::NProto::TDiskRequest diskRequest;
        diskRequest.set_disk_space(Config_->MinRequiredDiskSpace);

        if (UserJobSpec_) {
            // COMPAT(ignat).
            if (UserJobSpec_->has_disk_space_limit()) {
                diskRequest.set_disk_space(UserJobSpec_->disk_space_limit());
            }

            if (UserJobSpec_->has_disk_request()) {
                diskRequest = UserJobSpec_->disk_request();
            }

            if (UserJobSpec_->has_prepare_time_limit()) {
                auto prepareTimeLimit = FromProto<TDuration>(UserJobSpec_->prepare_time_limit());
                TDelayedExecutor::Submit(
                    BIND(&TJob::OnJobPreparationTimeout, MakeWeak(this), prepareTimeLimit, /*fatal*/ false)
                        .Via(Invoker_),
                    prepareTimeLimit);
            }

            if (auto prepareTimeLimit = Config_->JobPrepareTimeLimit) {
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
            int gpuCount = GetResourceUsage().gpu();
            YT_LOG_DEBUG("Acquiring GPU slots (Count: %v)", gpuCount);
            GpuSlots_ = Bootstrap_->GetGpuManager()->AcquireGpuSlots(gpuCount);
            for (int index = 0; index < gpuCount; ++index) {
                TGpuStatistics statistics;
                statistics.LastUpdateTime = now;
                GpuStatistics_.emplace_back(std::move(statistics));
            }

            if (UserJobSpec_ && UserJobSpec_->has_cuda_toolkit_version()) {
                Bootstrap_->GetGpuManager()->VerifyCudaToolkitDriverVersion(UserJobSpec_->cuda_toolkit_version());
            }

            std::vector<int> deviceNumbers;
            for (const auto& slot : GpuSlots_) {
                deviceNumbers.push_back(slot->GetDeviceNumber());
            }
            YT_LOG_DEBUG("GPU slots acquired (DeviceNumbers: %v)", deviceNumbers);
        }

        NScheduler::NProto::TCpuRequest cpuRequest;
        cpuRequest.set_cpu(RequestedCpu_);
        cpuRequest.set_allow_cpu_idle_policy(SchedulerJobSpecExt_->allow_cpu_idle_policy());

        YT_LOG_INFO("Acquiring slot (DiskRequest: %v, CpuRequest: %v)",
            diskRequest,
            cpuRequest);

        auto slotManager = Bootstrap_->GetSlotManager();
        Slot_ = slotManager->AcquireSlot(diskRequest, cpuRequest);

        YT_LOG_INFO("Slot acquired (SlotIndex: %v)", Slot_->GetSlotIndex());

        SetJobPhase(EJobPhase::PreparingNodeDirectory);
        // This is a heavy part of preparation, offload it to compression invoker.
        BIND(&TJob::PrepareNodeDirectory, MakeWeak(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetCompressionPoolInvoker())
            .Run()
            .Subscribe(
                BIND(&TJob::OnNodeDirectoryPrepared, MakeWeak(this))
                    .Via(Invoker_));
    });
}

bool TJob::IsStarted() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return Started_;
}

void TJob::OnResourcesAcquired()
{
    Start();
}

TResourceHolder* TJob::AsResourceHolder()
{
    return this;
}

void TJob::Abort(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto timeout = DynamicConfig_->JobAbortionTimeout.value_or(Config_->JobAbortionTimeout);

    YT_LOG_INFO(error, "Job abort requested (Phase: %v, Timeout: %v)",
        JobPhase_,
        timeout);

    auto startAbortion = [&, error{std::move(error)}] () {
        SetJobStatePhase(EJobState::Aborting, EJobPhase::WaitingAbort);
        DoSetResult(std::move(error));
        TDelayedExecutor::Submit(
            BIND(&TJob::OnJobAbortionTimeout, MakeStrong(this))
                .Via(Invoker_),
            timeout);
    };

    switch (JobPhase_) {
        case EJobPhase::Created:
        case EJobPhase::DownloadingArtifacts:
        case EJobPhase::RunningGpuCheckCommand:
        case EJobPhase::RunningExtraGpuCheckCommand:
        case EJobPhase::Running:
            startAbortion();
            ArtifactsFuture_.Cancel(TError("Job aborted"));

            // Do the actual cleanup asynchronously.
            BIND(&TJob::Cleanup, MakeStrong(this))
                .Via(Bootstrap_->GetJobInvoker())
                .Run();

            break;

        case EJobPhase::PreparingNodeDirectory:
        case EJobPhase::PreparingSandboxDirectories:
        case EJobPhase::PreparingArtifacts:
        case EJobPhase::PreparingRootVolume:
        case EJobPhase::SpawningJobProxy:
        case EJobPhase::PreparingJob:
            // Wait for the next event handler to complete the abortion.
            startAbortion();
            Slot_->CancelPreparation();
            break;

        default:
            YT_LOG_DEBUG("Cannot abort job (JobState: %v, JobPhase: %v)",
                JobState_,
                JobPhase_);
            break;
    }
}

void TJob::OnJobProxySpawned()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction([&] {
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

    GuardedAction([&] {
        YT_LOG_DEBUG("Preparing job artifact (ArtifactName: %v, PipePath: %v)",
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
            YT_LOG_INFO("Downloading artifact with cache bypass (FileName: %v, Executable: %v, SandboxKind: %v, CompressedDataSize: %v)",
                artifact.Name,
                artifact.Executable,
                artifact.SandboxKind,
                artifact.Key.GetCompressedDataSize());

            const auto& chunkCache = Bootstrap_->GetChunkCache();
            auto downloadOptions = MakeArtifactDownloadOptions();
            auto producer = chunkCache->MakeArtifactDownloadProducer(artifact.Key, downloadOptions);

            ArtifactPrepareFutures_.push_back(
                Slot_->MakeFile(
                    Id_,
                    artifact.Name,
                    artifact.SandboxKind,
                    producer,
                    pipe));
        } else if (artifact.CopyFile) {
            YT_VERIFY(artifact.Chunk);

            YT_LOG_INFO("Copying artifact (FileName: %v, Executable: %v, SandboxKind: %v, CompressedDataSize: %v)",
                artifact.Name,
                artifact.Executable,
                artifact.SandboxKind,
                artifact.Key.GetCompressedDataSize());

            ArtifactPrepareFutures_.push_back(
                Slot_->MakeCopy(
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

    GuardedAction([&] {
        ValidateJobPhase(EJobPhase::PreparingArtifacts);

        Slot_->OnArtifactPreparationFailed(
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

    GuardedAction([&] {
        YT_LOG_INFO("Artifacts prepared");

        ValidateJobPhase(EJobPhase::PreparingArtifacts);
        SetJobPhase(EJobPhase::PreparingJob);
    });
}

void TJob::OnJobPrepared()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction([&] {
        JobPrepared_.Fire();

        YT_LOG_INFO("Job prepared");

        ValidateJobPhase(EJobPhase::PreparingJob);
        SetJobPhase(EJobPhase::Running);
    });
}

void TJob::OnResultReceived(TJobResult jobResult)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction([&] {
        SetJobPhase(EJobPhase::FinalizingJobProxy);

        std::optional<NScheduler::NProto::TSchedulerJobResultExt> schedulerResultExtension;
        if (jobResult.HasExtension(NScheduler::NProto::TSchedulerJobResultExt::job_result_ext)) {
            schedulerResultExtension = std::move(
                *jobResult.ReleaseExtension(NScheduler::NProto::TSchedulerJobResultExt::job_result_ext));
        }
        DoSetResult(
            FromProto<TError>(jobResult.error()),
            std::move(schedulerResultExtension),
            /*receivedFromJobProxy*/ true);
    });
}

TJobId TJob::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

TOperationId TJob::GetOperationId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return OperationId_;
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
        "Updating controller agent (ControllerAgentAddress: %v -> %v, ControllerAgentIncarnationId: %v)",
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

TInstant TJob::GetStartTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StartTime_;
}

NJobAgent::TTimeStatistics TJob::GetTimeStatistics() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto getPrepareDuration = [&] () -> std::optional<TDuration> {
        if (!PrepareTime_) {
            return std::nullopt;
        } else if (!ExecTime_) {
            return TInstant::Now() - *PrepareTime_;
        } else {
            return *ExecTime_ - *PrepareTime_;
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
        if (!PrepareTime_) {
            return std::nullopt;
        } else if (!CopyTime_) {
            return TInstant::Now() - *PrepareTime_;
        } else {
            return *CopyTime_ - *PrepareTime_;
        }
    };
    auto getExecDuration = [&] () -> std::optional<TDuration> {
        if (!ExecTime_) {
            return std::nullopt;
        } else if (!FinishTime_) {
            return TInstant::Now() - *ExecTime_;
        } else {
            return *FinishTime_ - *ExecTime_;
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

EJobPhase TJob::GetPhase() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return JobPhase_;
}

int TJob::GetSlotIndex() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (!Slot_) {
        return -1;
    }

    return Slot_->GetSlotIndex();
}

const TNodeResources& TJob::GetResourceUsage() const
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
            NScheduler::NProto::TSchedulerJobResultExt::job_result_ext) = *JobResultExtension_;
    }

    return result;
}

double TJob::GetProgress() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return Progress_;
}

void TJob::SetResourceUsage(const TNodeResources& newUsage)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ == EJobPhase::Running) {
        auto delta = TResourceHolder::SetResourceUsage(newUsage);
        ResourcesUpdated_.Fire(delta);
    }
}

bool TJob::ResourceUsageOverdrafted() const
{
    return TResourceHolder::GetResourceUsage().user_memory() > RequestedMemory_;
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
            CollectSensorsFromGpuInfo(&userJobSensors);
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

void TJob::BuildOrchid(NYTree::TFluentMap fluent) const
{
    fluent
        .Item("events").Value(JobEvents_)
        .Item("core_infos").Value(CoreInfos_)
        .Item("exec_attributes").Value(ExecAttributes_);
}

std::vector<TChunkId> TJob::DumpInputContext()
{
    VERIFY_THREAD_AFFINITY(JobThread);
    ValidateJobRunning();

    try {
        return GetJobProbeOrThrow()->DumpInputContext();
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
        JobState_ == EJobState::Aborting ||
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
            return nullptr;
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
            .JobId(GetId()));
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

void TJob::GuardedInterrupt(
    TDuration timeout,
    EInterruptReason interruptionReason,
    const std::optional<TString>& preemptionReason,
    const std::optional<NScheduler::TPreemptedFor>& preemptedFor)
{
    VERIFY_THREAD_AFFINITY(JobThread);

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
            << TError(NExecNode::EErrorCode::AbortByScheduler, "Job aborted by scheduler");

        if (interruptionReason == EInterruptReason::Preemption) {
            error = TError("Job preempted") << error;
            error = error
                << TErrorAttribute("preemption_reason", preemptionReason)
                << TErrorAttribute("abort_reason", EAbortReason::Preemption);
        }

        Abort(error);
        return;
    }

    if (JobPhase_ < EJobPhase::Running) {
        TError error(NJobProxy::EErrorCode::JobNotPrepared, "Interrupting job that has not started yet");

        if (interruptionReason == EInterruptReason::Preemption) {
            error = TError("Job preempted") << error;
            error = error
                << TErrorAttribute("preemption_reason", preemptionReason)
                << TErrorAttribute("abort_reason", EAbortReason::Preemption);
        }

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
                BIND(&TJob::OnJobInterruptionTimeout, MakeWeak(this)),
                timeout,
                Bootstrap_->GetJobInvoker());
            InterruptionDeadline_ = TInstant::Now() + timeout;
        }
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

void TJob::GuardedFail()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::Running) {
        Abort(TError(NJobProxy::EErrorCode::JobNotRunning, "Failing job that is not running"));

        return;
    }

    try {
        GetJobProbeOrThrow()->Fail();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error failing job on job proxy")
                << ex;
    }
}

bool TJob::GetStored() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return Stored_;
}

void TJob::SetStored(bool value)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    Stored_ = value;
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

void TJob::OnJobInterruptionTimeout()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    Abort(TError(NJobProxy::EErrorCode::InterruptionTimeout, "Interruption is timed out"));
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
    YT_LOG_INFO("Interrupting job (InterruptionReason: %v, PreemptionReason: %v, PreemptedFor: %v, Timeout: %v)",
        interruptionReason,
        preemptionReason,
        preemptedFor,
        timeout);

    try {
        GuardedInterrupt(timeout, interruptionReason, preemptionReason, preemptedFor);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to interrupt job");
    }
}

void TJob::Fail()
{
    YT_LOG_INFO("Failing job");

    try {
        GuardedFail();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to fail job");
    }
}

NScheduler::EInterruptReason TJob::GetInterruptionReason() const noexcept
{
    return InterruptionReason_;
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

    JobState_ = state;
    AddJobEvent(state);
}

void TJob::SetJobPhase(EJobPhase phase)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    JobPhase_ = phase;
    AddJobEvent(phase);
}

void TJob::SetJobStatePhase(EJobState state, EJobPhase phase)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    JobState_ = state;
    JobPhase_ = phase;
    AddJobEvent(state, phase);
}

void TJob::ValidateJobRunning() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != EJobPhase::Running) {
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

void TJob::DoSetResult(TError error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    DoSetResult(std::move(error), /*jobResultExtension*/ std::nullopt, /*receivedFromJobProxy*/ false);
}

void TJob::DoSetResult(
    TError error,
    std::optional<NScheduler::NProto::TSchedulerJobResultExt> jobResultExtension,
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

    if (Config_->TestJobErrorTruncation) {
        if (!error.IsOK()) {
            for (int index = 0; index < 10; ++index) {
                error.MutableInnerErrors()->push_back(TError("Test error " + ToString(index)));
            }
            YT_LOG_DEBUG(error, "TestJobErrorTruncation");
        }
    }

    JobResultExtension_ = std::nullopt;
    if (jobResultExtension)
    {
        JobResultExtension_ = std::move(jobResultExtension);
    }

    {
        Error_ = error.Truncate();
    }

    JobProxyCompleted_ = receivedFromJobProxy;

    FinishTime_ = TInstant::Now();
}

bool TJob::HandleFinishingPhase()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    switch (JobPhase_) {
        case EJobPhase::WaitingAbort:
            Cleanup();
            return true;

        case EJobPhase::Cleanup:
        case EJobPhase::Finished:
            return true;

        case EJobPhase::Created:
            YT_VERIFY(JobState_ == EJobState::Waiting);
            return false;

        default:
            YT_VERIFY(JobState_ == EJobState::Running);
            return false;
    }
}

void TJob::ValidateJobPhase(EJobPhase expectedPhase) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ != expectedPhase) {
        THROW_ERROR_EXCEPTION("Unexpected job phase")
            << TErrorAttribute("expected_phase", expectedPhase)
            << TErrorAttribute("actual_phase", JobPhase_);
    }
}

// Event handlers.
void TJob::OnNodeDirectoryPrepared(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (auto delay = JobTestingOptions_->DelayAfterNodeDirectoryPrepared) {
        TDelayedExecutor::WaitForDuration(*delay);
    }

    GuardedAction([&] {
        ValidateJobPhase(EJobPhase::PreparingNodeDirectory);
        THROW_ERROR_EXCEPTION_IF_FAILED(error,
            NExecNode::EErrorCode::NodeDirectoryPreparationFailed,
            "Failed to prepare job node directory");

        SetJobPhase(EJobPhase::DownloadingArtifacts);
        auto artifactsFuture = DownloadArtifacts();
        artifactsFuture.Subscribe(
            BIND(&TJob::OnArtifactsDownloaded, MakeWeak(this))
                .Via(Invoker_));
        ArtifactsFuture_ = artifactsFuture.As<void>();
    });
}

void TJob::OnArtifactsDownloaded(const TErrorOr<std::vector<NDataNode::IChunkPtr>>& errorOrArtifacts)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction([&] {
        ValidateJobPhase(EJobPhase::DownloadingArtifacts);
        THROW_ERROR_EXCEPTION_IF_FAILED(errorOrArtifacts, "Failed to download artifacts");

        YT_LOG_INFO("Artifacts downloaded");

        const auto& chunks = errorOrArtifacts.Value();
        for (size_t index = 0; index < Artifacts_.size(); ++index) {
            Artifacts_[index].Chunk = chunks[index];
        }

        CopyTime_ = TInstant::Now();
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);
        BIND(&TJob::PrepareSandboxDirectories, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run()
            .Subscribe(BIND(
                &TJob::OnSandboxDirectoriesPrepared,
                MakeWeak(this))
            .Via(Invoker_));
    });
}

void TJob::OnSandboxDirectoriesPrepared(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction([&] {
        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to prepare sandbox directories");

        if (LayerArtifactKeys_.empty()) {
            RunJobProxy();
        } else {
            StartPrepareVolumeTime_ = TInstant::Now();
            SetJobPhase(EJobPhase::PreparingRootVolume);
            YT_LOG_INFO("Preparing root volume (LayerCount: %v)", LayerArtifactKeys_.size());

            for (const auto& layer : LayerArtifactKeys_) {
                i64 layerSize = layer.GetCompressedDataSize();
                // NB(gritukan): This check is racy. Layer can be removed
                // from cache after this check and before actual preparation.
                UpdateArtifactStatistics(layerSize, Slot_->IsLayerCached(layer));
            }

            Slot_->PrepareRootVolume(LayerArtifactKeys_, MakeArtifactDownloadOptions())
                .Subscribe(BIND(
                    &TJob::OnVolumePrepared,
                    MakeWeak(this))
                .Via(Invoker_));
        }
    });
}

void TJob::OnVolumePrepared(const TErrorOr<IVolumePtr>& volumeOrError)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    FinishPrepareVolumeTime_ = TInstant::Now();

    GuardedAction([&] {
        ValidateJobPhase(EJobPhase::PreparingRootVolume);
        if (!volumeOrError.IsOK()) {
            auto error = TError(EErrorCode::RootVolumePreparationFailed, "Failed to prepare artifacts")
                << volumeOrError;

            // Corrupted user layers should not disable scheduler jobs.
            // if (!error.FindMatching(NDataNode::EErrorCode::LayerUnpackingFailed)) {
            //     Bootstrap_->GetSlotManager()->Disable(error);
            // }

            THROW_ERROR error;
        }

        RootVolume_ = volumeOrError.Value();

        SetJobPhase(EJobPhase::RunningSetupCommands);

        // Even though #RunSetupCommands returns future, we still need to pass it through invoker
        // since Porto API is used and can cause context switch.
        BIND(&TJob::RunSetupCommands, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run()
            .Subscribe(BIND(
                &TJob::OnSetupCommandsFinished,
                MakeWeak(this))
                .Via(Invoker_));

    });
}

void TJob::OnSetupCommandsFinished(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction([&] {
        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        if (!error.IsOK()) {
            THROW_ERROR_EXCEPTION(EErrorCode::SetupCommandFailed, "Failed to run setup commands")
                << error;
        }

        if (UserJobSpec_ && UserJobSpec_->has_gpu_check_binary_path()) {
            SetJobPhase(EJobPhase::RunningGpuCheckCommand);

            // since Porto API is used and can cause context switch.
            BIND(&TJob::RunGpuCheckCommand, MakeStrong(this))
                .AsyncVia(Invoker_)
                .Run(
                    UserJobSpec_->gpu_check_binary_path(),
                    FromProto<std::vector<TString>>(UserJobSpec_->gpu_check_binary_args()),
                    EGpuCheckType::Preliminary)
                .Subscribe(BIND(
                    &TJob::OnGpuCheckCommandFinished,
                    MakeWeak(this))
                    .Via(Invoker_));
        } else {
            RunJobProxy();
        }
    });
}

TFuture<void> TJob::RunGpuCheckCommand(
    const TString& gpuCheckBinaryPath,
    std::vector<TString> gpuCheckBinaryArgs,
    EGpuCheckType gpuCheckType)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO("Running %lv GPU check commands", gpuCheckType);

    int checkStartIndex = 0;
    switch (gpuCheckType) {
        case EGpuCheckType::Preliminary:
            PreliminaryGpuCheckStartTime_ = TInstant::Now();
            checkStartIndex = SetupCommandsCount_;
            break;
        case EGpuCheckType::Extra:
            ExtraGpuCheckStartTime_ = TInstant::Now();
            checkStartIndex = SetupCommandsCount_ + 2;
            break;
        default:
            Y_UNREACHABLE();
    }

    {
        auto testFileCommand = New<TShellCommandConfig>();
        testFileCommand->Path = "/usr/bin/test";
        testFileCommand->Args = {"-f", gpuCheckBinaryPath};

        auto testFileError = WaitFor(Slot_->RunSetupCommands(
            Id_,
            {testFileCommand},
            MakeWritableRootFS(),
            Config_->JobController->SetupCommandUser,
            /*devices*/ std::nullopt,
            /*startIndex*/ checkStartIndex));

        if (!testFileError.IsOK()) {
            THROW_ERROR_EXCEPTION(EErrorCode::GpuCheckCommandIncorrect, "Path to GPU check binary is not a file")
                << TErrorAttribute("path", gpuCheckBinaryPath)
                << testFileError;
        }
    }

    auto checkCommand = New<TShellCommandConfig>();
    checkCommand->Path = gpuCheckBinaryPath;
    checkCommand->Args = std::move(gpuCheckBinaryArgs);

    std::vector<TDevice> devices;
    for (const auto& deviceName : Bootstrap_->GetGpuManager()->GetGpuDevices()) {
        bool deviceFound = false;
        for (const auto& slot : GpuSlots_) {
            if (slot->GetDeviceName() == deviceName) {
                deviceFound = true;
                break;
            }
        }

        // We should not explicitly exclude test device that does not actually exists.
        if (!deviceFound && !Config_->JobController->GpuManager->TestResource) {
            // Exclude device explicitly.
            devices.emplace_back(TDevice{
                .DeviceName = deviceName,
                .Enabled = false});
        }
    }

    if (Config_->JobController->GpuManager->TestExtraGpuCheckCommandFailure) {
        return MakeFuture(TError("Testing extra GPU check command failed"));
    }

    return Slot_->RunSetupCommands(
        Id_,
        {checkCommand},
        MakeWritableRootFS(),
        Config_->JobController->SetupCommandUser,
        devices,
        /*startIndex*/ checkStartIndex + 1);
}

void TJob::OnGpuCheckCommandFinished(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    PreliminaryGpuCheckFinishTime_ = TInstant::Now();

    GuardedAction([&] {
        ValidateJobPhase(EJobPhase::RunningGpuCheckCommand);
        if (!error.IsOK()) {
            auto checkError = TError(EErrorCode::GpuCheckCommandFailed, "Preliminary GPU check command failed")
                << error;
            THROW_ERROR checkError;
        }

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
    if (!error.IsOK()) {
        YT_LOG_FATAL_IF(
            !Error_ || Error_->IsOK(),
            "Job error is not set (Error: %v)", Error_);

        auto initialError = std::move(*Error_);
        // Reset Error_ to set it with checkError
        Error_ = {};
        JobResultExtension_.reset();

        auto checkError = TError(EErrorCode::GpuCheckCommandFailed, "Extra GPU check command failed")
            << error
            << initialError;

        YT_LOG_WARNING(checkError, "Extra GPU check command executed after job failure is also failed");
        DoSetResult(std::move(checkError));
    }

    Cleanup();
}

void TJob::RunJobProxy()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    ExecTime_ = TInstant::Now();
    SetJobPhase(EJobPhase::SpawningJobProxy);
    InitializeJobProbe();

    BIND(
        &ISlot::RunJobProxy,
        Slot_,
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
        Config_->JobProxyPreparationTimeout);
}

void TJob::OnJobProxyPreparationTimeout()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    GuardedAction([&] {
        if (JobPhase_ == EJobPhase::PreparingJob) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::JobProxyPreparationTimeout,
                "Failed to prepare job proxy within timeout, aborting job");
        }
    });
}

void TJob::OnJobPreparationTimeout(TDuration prepareTimeLimit, bool fatal)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ < EJobPhase::Running) {
        auto error = TError(
            fatal ? EErrorCode::JobPreparationTimeout : EErrorCode::FatalJobPreparationTimeout,
            "Failed to prepare job within timeout")
            << TErrorAttribute("prepare_time_limit", prepareTimeLimit)
            << TErrorAttribute("job_start_time", StartTime_);
        Abort(error);
    }
}

void TJob::OnJobAbortionTimeout()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobState_ == EJobState::Aborting) {
        auto error = TError("Failed to abort job within timeout")
            << TErrorAttribute("job_id", Id_)
            << TErrorAttribute("operation_id", OperationId_)
            << TErrorAttribute("job_abortion_timeout", Config_->JobAbortionTimeout);
        Bootstrap_->GetSlotManager()->Disable(error);
    }
}

void TJob::OnJobProxyFinished(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    ResetJobProbe();

    if (HandleFinishingPhase()) {
        return;
    }

    YT_LOG_INFO("Job proxy finished");

    const auto& currentError = Error_
        ? *Error_
        : TError();
    if (!currentError.IsOK() && UserJobSpec_ && UserJobSpec_->has_gpu_check_binary_path()) {
        SetJobPhase(EJobPhase::RunningExtraGpuCheckCommand);

        BIND(&TJob::RunGpuCheckCommand, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(
                UserJobSpec_->gpu_check_binary_path(),
                FromProto<std::vector<TString>>(UserJobSpec_->gpu_check_binary_args()),
                EGpuCheckType::Extra)
            .Subscribe(BIND(
                &TJob::OnExtraGpuCheckCommandFinished,
                MakeWeak(this))
                .Via(Invoker_));
    } else {
        if (!error.IsOK()) {
            DoSetResult(TError(EErrorCode::JobProxyFailed, "Job proxy failed")
                << BuildJobProxyError(error));
        }

        Cleanup();
    }
}

void TJob::GuardedAction(std::function<void()> action)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (HandleFinishingPhase()) {
        return;
    }

    try {
        TForbidContextSwitchGuard contextSwitchGuard;
        action();
    } catch (const std::exception& ex) {
        // TODO(pogorelov): This method is called not only in preparation states, do something with log message.
        YT_LOG_WARNING(ex, "Error preparing scheduler job");

        DoSetResult(ex);
        Cleanup();
    }
}

// Finalization.
void TJob::Cleanup()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobPhase_ == EJobPhase::Cleanup || JobPhase_ == EJobPhase::Finished) {
        return;
    }

    YT_LOG_INFO("Cleaning up after scheduler job");

    FinishTime_ = TInstant::Now();
    SetJobPhase(EJobPhase::Cleanup);

    TDelayedExecutor::Cancel(InterruptionTimeoutCookie_);

    if (auto delay = JobTestingOptions_->DelayInCleanup) {
        TDelayedExecutor::WaitForDuration(*delay);
    }

    if (Slot_) {
        try {
            YT_LOG_DEBUG("Clean processes (SlotIndex: %v)", Slot_->GetSlotIndex());
            Slot_->CleanProcesses();
        } catch (const std::exception& ex) {
            // Errors during cleanup phase do not affect job outcome.
            YT_LOG_ERROR(ex, "Failed to clean processes (SlotIndex: %v)", Slot_->GetSlotIndex());
        }
    }

    // NodeDirectory can be really huge, we better offload its cleanup.
    // NB: do this after slot cleanup.
    {
        auto* inputNodeDirectory = JobSpec_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext)
            ->release_input_node_directory();
        NRpc::TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(BIND([inputNodeDirectory] () {
            delete inputNodeDirectory;
        }));
    }

    YT_VERIFY(Error_);

    // Copy info from traffic meter to statistics.
    auto statistics = ConvertTo<TStatistics>(StatisticsYson_);
    FillTrafficStatistics(ExecAgentTrafficStatisticsPrefix, statistics, TrafficMeter_);
    StatisticsYson_ = ConvertToYsonString(statistics);

    auto& error = *Error_;

    if (error.IsOK()) {
        SetJobState(EJobState::Completed);
    } else if (IsFatalError(error)) {
        error.MutableAttributes()->Set("fatal", true);
        SetJobState(EJobState::Failed);
    } else {
        auto abortReason = GetAbortReason();
        if (abortReason) {
            error.MutableAttributes()->Set("abort_reason", abortReason);
            SetJobState(EJobState::Aborted);
        } else {
            SetJobState(EJobState::Failed);
        }
    }

    if (!error.IsOK()) {
        // NB: it is required to report error that occurred in some place different
        // from OnJobFinished method.
        HandleJobReport(MakeDefaultJobReport().Error(error));
    }

    YT_LOG_INFO(
        error,
        "Setting final job state (JobState: %v, ResourceUsage: %v)",
        GetState(),
        GetResourceUsage());

    // Release resources.
    GpuSlots_.clear();
    GpuStatistics_.clear();

    if (IsStarted()) {
        auto oneUserSlotResources = ZeroNodeResources();
        oneUserSlotResources.set_user_slots(1);

        TResourceHolder::SetResourceUsage(oneUserSlotResources);
    }

    JobFinished_.Fire();

    if (Slot_) {
        if (ShouldCleanSandboxes()) {
            try {
                YT_LOG_DEBUG("Clean sandbox (SlotIndex: %v)", Slot_->GetSlotIndex());
                Slot_->CleanSandbox();
            } catch (const std::exception& ex) {
                // Errors during cleanup phase do not affect job outcome.
                YT_LOG_ERROR(ex, "Failed to clean sandbox (SlotIndex: %v)", Slot_->GetSlotIndex());
            }
        } else {
            YT_LOG_WARNING("Sandbox cleanup is disabled by environment variable %v; should be used for testing purposes only",
                DisableSandboxCleanupEnv);
        }
        YT_LOG_DEBUG("Release slot (SlotIndex: %v)", Slot_->GetSlotIndex());
        Slot_.Reset();
    }

    // NB: we should disable slot here to give scheduler information about job failure.
    if (error.FindMatching(EErrorCode::GpuCheckCommandFailed) && !error.FindMatching(EErrorCode::GpuCheckCommandIncorrect)) {
        Bootstrap_->GetSlotManager()->OnGpuCheckCommandFailed(error);
    }

    ReleaseResources();

    SetJobPhase(EJobPhase::Finished);

    YT_LOG_INFO("Job finished (JobState: %v)", GetState());
}

// Preparation.
void TJob::PrepareNodeDirectory()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* schedulerJobSpecExt = JobSpec_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

    if (schedulerJobSpecExt->has_input_node_directory()) {
        YT_LOG_INFO("Node directory is provided by scheduler");
        return;
    }

    YT_LOG_INFO("Started preparing node directory");

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
                auto replicas = FromProto<TChunkReplicaList>(chunkSpec.replicas());
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

        validateTableSpecs(schedulerJobSpecExt->input_table_specs());
        validateTableSpecs(schedulerJobSpecExt->foreign_input_table_specs());

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

        if (attempt >= Config_->NodeDirectoryPrepareRetryCount) {
            YT_LOG_WARNING("Some node ids were not resolved, skipping corresponding replicas (UnresolvedNodeId: %v)",
                *unresolvedNodeId);
            break;
        }

        YT_LOG_INFO("Unresolved node id found in job spec; backing off and retrying (NodeId: %v, Attempt: %v)",
            *unresolvedNodeId,
            attempt);
        TDelayedExecutor::WaitForDuration(Config_->NodeDirectoryPrepareBackoffTime);
    }

    nodeDirectory->DumpTo(schedulerJobSpecExt->mutable_input_node_directory());

    YT_LOG_INFO("Finished preparing node directory");
}

TJobProxyConfigPtr TJob::CreateConfig()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto proxyConfig = CloneYsonSerializable(Bootstrap_->GetJobProxyConfigTemplate());
    auto localDescriptor = Bootstrap_->GetLocalDescriptor();
    proxyConfig->DataCenter = localDescriptor.GetDataCenter();
    proxyConfig->Rack = localDescriptor.GetRack();
    proxyConfig->Addresses = localDescriptor.Addresses();

    proxyConfig->LocalHostName = Bootstrap_->GetLocalHostName();

    proxyConfig->BusServer = Slot_->GetBusServerConfig();

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

    proxyConfig->MemoryTracker->MemoryStatisticsCachePeriod = proxyConfig->MemoryTracker->UseSMapsMemoryTracker
        ? Config_->SMapsMemoryTrackerCachePeriod
        : Config_->MemoryTrackerCachePeriod;

    proxyConfig->SlotIndex = Slot_->GetSlotIndex();

    if (RootVolume_) {
        proxyConfig->RootPath = RootVolume_->GetPath();
        proxyConfig->Binds = Config_->RootFSBinds;
    }

    auto tryReplaceSlotIndex = [&] (TString& str) {
        size_t index = str.find(SlotIndexPattern);
        if (index != TString::npos) {
            str.replace(index, SlotIndexPattern.size(), ToString(Slot_->GetSlotIndex()));
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

    for (const auto& slot : GpuSlots_) {
        proxyConfig->GpuDevices.push_back(slot->GetDeviceName());
    }

    proxyConfig->MakeRootFSWritable = UserJobSpec_ && UserJobSpec_->make_rootfs_writable();

    std::vector<TIP6Address> ipAddresses;
    ipAddresses.reserve(ResolvedNodeAddresses_.size());

    if (NetworkProjectId_) {
        for (const auto& [addressName, address] : ResolvedNodeAddresses_) {
            auto networkAddress = New<TUserJobNetworkAddress>();
            networkAddress->Address = TMtnAddress{address}
                .SetProjectId(*NetworkProjectId_)
                .SetHost(Slot_->GetSlotIndex())
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

        proxyConfig->HostName = Format("slot_%v.%v",
            Slot_->GetSlotIndex(),
            Bootstrap_->GetConfig()->Addresses[0].second);
    } else {
        for (const auto& [addressName, address] : ResolvedNodeAddresses_) {
            ipAddresses.push_back(address);
        }
    }

    {
        ExecAttributes_.SlotIndex = Slot_->GetSlotIndex();
        ExecAttributes_.SandboxPath = Slot_->GetSandboxPath(ESandboxKind::User);
        ExecAttributes_.MediumName = Slot_->GetMediumName();

        ExecAttributes_.IPAddresses.reserve(ipAddresses.size());
        for (const auto& address : ipAddresses) {
            ExecAttributes_.IPAddresses.push_back(ToString(address));
        }

        ExecAttributes_.GpuDevices.reserve(GpuSlots_.size());
        for (const auto& gpuSlot : GpuSlots_) {
            auto& gpuDevice = ExecAttributes_.GpuDevices.emplace_back(New<TGpuDevice>());
            gpuDevice->DeviceNumber = gpuSlot->GetDeviceNumber();
            gpuDevice->DeviceName = gpuSlot->GetDeviceName();
        }
    }

    if (auto proxyDynamicConfig = Bootstrap_->GetJobController()->GetJobProxyDynamicConfig()) {
        proxyConfig->Jaeger = proxyConfig->Jaeger->ApplyDynamic(proxyDynamicConfig->Jaeger);
        proxyConfig->EnableJobShellSeccopm = proxyDynamicConfig->EnableJobShellSeccopm;
        proxyConfig->UsePortoKillForSignalling = proxyDynamicConfig->UsePortoKillForSignalling;
        proxyConfig->ForceIdleCpuPolicy = proxyDynamicConfig->ForceIdleCpuPolicy;
        proxyConfig->UploadDebugArtifactChunks = proxyDynamicConfig->UploadDebugArtifactChunks;
        proxyConfig->AbortOnUncaughtException = proxyDynamicConfig->AbortOnUncaughtException;
        if (proxyDynamicConfig->JobEnvironment) {
            proxyConfig->JobEnvironment = PatchNode(proxyConfig->JobEnvironment, proxyDynamicConfig->JobEnvironment);
        }
    }

    proxyConfig->JobThrottler = CloneYsonSerializable(DynamicConfig_->JobThrottler);
    if (!SchedulerJobSpecExt_->enable_prefetching_job_throttler()) {
        proxyConfig->JobThrottler->BandwidthPrefetch->Enable = false;
        proxyConfig->JobThrottler->RpsPrefetch->Enable = false;
    }
    YT_LOG_DEBUG("Prefetching job throttler init (DynamicConfigEnable: %v, JobSpecEnable: %v, PrefetchEnable: %v)",
        DynamicConfig_->JobThrottler->BandwidthPrefetch->Enable,
        SchedulerJobSpecExt_->enable_prefetching_job_throttler(),
        proxyConfig->JobThrottler->BandwidthPrefetch->Enable);

    proxyConfig->StatisticsOutputTableCountLimit = DynamicConfig_->StatisticsOutputTableCountLimit;

    return proxyConfig;
}

void TJob::PrepareSandboxDirectories()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO("Started preparing sandbox directories");

    TUserSandboxOptions options;
    // NB: this eventually results in job failure.
    options.DiskOverdraftCallback = BIND(&TJob::Abort, MakeWeak(this))
        .Via(Invoker_);

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
    }

    TmpfsPaths_ = WaitFor(Slot_->PrepareSandboxDirectories(options))
        .ValueOrThrow();

    for (const auto& artifact : Artifacts_) {
        // Artifact is passed into the job via symlink.
        if (!artifact.BypassArtifactCache && !artifact.CopyFile) {
            YT_VERIFY(artifact.Chunk);

            YT_LOG_INFO("Making symlink for artifact (FileName: %v, Executable: %v, SandboxKind: %v, CompressedDataSize: %v)",
                artifact.Name,
                artifact.Executable,
                artifact.SandboxKind,
                artifact.Key.GetCompressedDataSize());

            auto sandboxPath = Slot_->GetSandboxPath(artifact.SandboxKind);
            auto symlinkPath = NFS::CombinePaths(sandboxPath, artifact.Name);

            WaitFor(Slot_->MakeLink(
                Id_,
                artifact.Name,
                artifact.SandboxKind,
                artifact.Chunk->GetFileName(),
                symlinkPath,
                artifact.Executable))
                .ThrowOnError();
        } else {
            YT_VERIFY(artifact.SandboxKind == ESandboxKind::User);
        }
    }

    YT_LOG_INFO("Finished preparing sandbox directories");
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

        bool needGpuLayers = NeedGpuLayers() || Config_->JobController->GpuManager->TestLayers;

        if (needGpuLayers && UserJobSpec_->enable_gpu_layers()) {
            if (UserJobSpec_->layers().empty()) {
                THROW_ERROR_EXCEPTION(EErrorCode::GpuJobWithoutLayers,
                    "No layers specified for GPU job; at least a base layer is required to use GPU");
            }

            for (auto&& layerKey : Bootstrap_->GetGpuManager()->GetToppingLayers()) {
                LayerArtifactKeys_.push_back(std::move(layerKey));
            }
        }

        for (const auto& layerKey : UserJobSpec_->layers()) {
            LayerArtifactKeys_.emplace_back(layerKey);
        }
    }

    if (SchedulerJobSpecExt_->has_input_query_spec()) {
        const auto& querySpec = SchedulerJobSpecExt_->input_query_spec();
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
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<TString> workloadDescriptorAnnotations = {
        Format("OperationId: %v", OperationId_),
        Format("JobId: %v", Id_),
        Format("AuthenticatedUser: %v", SchedulerJobSpecExt_->authenticated_user()),
    };

    auto options = TArtifactDownloadOptions{
        .TrafficMeter = TrafficMeter_,
    };

    if (UserJobSpec_->has_enable_squashfs()) {
        options.ConvertLayerToSquashFS = UserJobSpec_->enable_squashfs();
    }

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

        YT_LOG_INFO("Downloading user file (FileName: %v, SandboxKind: %v, CompressedDataSize: %v)",
            artifact.Name,
            artifact.SandboxKind,
            artifact.Key.GetCompressedDataSize());

        auto downloadOptions = MakeArtifactDownloadOptions();
        bool fetchedFromCache = false;
        auto asyncChunk = chunkCache->DownloadArtifact(artifact.Key, downloadOptions, &fetchedFromCache)
            .Apply(BIND([=, fileName = artifact.Name, this, this_ = MakeStrong(this)] (const TErrorOr<IChunkPtr>& chunkOrError) {
                THROW_ERROR_EXCEPTION_IF_FAILED(chunkOrError,
                    EErrorCode::ArtifactDownloadFailed,
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

    return AllSucceeded(asyncChunks);
}

TFuture<void> TJob::RunSetupCommands()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto commands = GetSetupCommands();
    SetupCommandsCount_ = commands.size();
    if (commands.empty()) {
        return VoidFuture;
    }

    YT_LOG_INFO("Running setup commands");

    return Slot_->RunSetupCommands(
        Id_,
        commands,
        MakeWritableRootFS(),
        Config_->JobController->SetupCommandUser,
        /*devices*/ std::nullopt,
        /*startIndex*/ 0);
}

// Analyse results.
TError TJob::BuildJobProxyError(const TError& spawnError)
{
    if (spawnError.IsOK()) {
        return TError();
    }

    auto jobProxyError = TError(EErrorCode::JobProxyFailed, "Job proxy failed")
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

std::optional<EAbortReason> TJob::GetAbortReason()
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
    if (resultError.FindMatching(EErrorCode::LayerUnpackingFailed)) {
        return std::nullopt;
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
                    if (DynamicConfig_->TreatJobProxyFailureAsAbort) {
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
        error.FindMatching(EErrorCode::SetupCommandFailed) ||
        error.FindMatching(EErrorCode::GpuJobWithoutLayers) ||
        error.FindMatching(EErrorCode::GpuCheckCommandIncorrect) ||
        error.FindMatching(EErrorCode::TmpfsOverflow) ||
        error.FindMatching(EErrorCode::FatalJobPreparationTimeout) ||
        error.FindMatching(NFormats::EErrorCode::InvalidFormat);
}

void TJob::EnrichStatisticsWithGpuInfo(TStatistics* statistics)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    i64 totalCumulativeUtilizationGpu = 0;
    i64 totalCumulativeUtilizationMemory = 0;
    i64 totalCumulativeMemory = 0;
    i64 totalCumulativeLoad = 0;
    i64 totalCumulativeUtilizationPower = 0;
    i64 totalCumulativePower = 0;
    i64 totalMaxMemoryUsed = 0;
    i64 totalMemoryTotal = 0;
    i64 totalCumulativeSMUtilization = 0;
    i64 totalCumulativeSMOccupancy = 0;

    auto gpuInfoMap = Bootstrap_->GetGpuManager()->GetGpuInfoMap();
    for (int index = 0; index < std::ssize(GpuSlots_); ++index) {
        const auto& slot = GpuSlots_[index];
        auto& slotStatistics = GpuStatistics_[index];

        TGpuInfo gpuInfo;
        {
            auto it = gpuInfoMap.find(slot->GetDeviceNumber());
            if (it == gpuInfoMap.end()) {
                continue;
            }
            gpuInfo = it->second;
        }

        slotStatistics.CumulativeUtilizationGpu +=
            (gpuInfo.UpdateTime - slotStatistics.LastUpdateTime).MilliSeconds() *
            gpuInfo.UtilizationGpuRate;
        if (gpuInfo.UtilizationGpuRate > 0) {
            slotStatistics.CumulativeLoad += (gpuInfo.UpdateTime - slotStatistics.LastUpdateTime).MilliSeconds();
        }
        slotStatistics.CumulativeUtilizationMemory +=
            (gpuInfo.UpdateTime - slotStatistics.LastUpdateTime).MilliSeconds() *
            gpuInfo.UtilizationMemoryRate;
        slotStatistics.CumulativeMemory +=
            (gpuInfo.UpdateTime - slotStatistics.LastUpdateTime).MilliSeconds() *
            gpuInfo.MemoryUsed;
        slotStatistics.CumulativeUtilizationPower +=
            (gpuInfo.UpdateTime - slotStatistics.LastUpdateTime).MilliSeconds() *
            (gpuInfo.PowerDraw / gpuInfo.PowerLimit);
        slotStatistics.CumulativePower +=
            (gpuInfo.UpdateTime - slotStatistics.LastUpdateTime).MilliSeconds() *
            gpuInfo.PowerDraw;
        slotStatistics.CumulativeUtilizationClocksSm +=
            (gpuInfo.UpdateTime - slotStatistics.LastUpdateTime).MilliSeconds() *
            (static_cast<double>(gpuInfo.ClocksSm) / gpuInfo.ClocksMaxSm);
        slotStatistics.CumulativeSMUtilization +=
            (gpuInfo.UpdateTime - slotStatistics.LastUpdateTime).MilliSeconds() *
            gpuInfo.SMUtilizationRate;
        slotStatistics.CumulativeSMOccupancy +=
            (gpuInfo.UpdateTime - slotStatistics.LastUpdateTime).MilliSeconds() *
            gpuInfo.SMOccupancyRate;
        slotStatistics.MaxMemoryUsed = std::max(slotStatistics.MaxMemoryUsed, gpuInfo.MemoryUsed);
        slotStatistics.LastUpdateTime = gpuInfo.UpdateTime;

        totalCumulativeUtilizationGpu += slotStatistics.CumulativeUtilizationGpu;
        totalCumulativeUtilizationMemory += slotStatistics.CumulativeUtilizationMemory;
        totalCumulativeMemory += slotStatistics.CumulativeMemory;
        totalCumulativeLoad += slotStatistics.CumulativeLoad;
        totalCumulativeUtilizationPower += slotStatistics.CumulativeUtilizationPower;
        totalCumulativePower += slotStatistics.CumulativePower;
        totalCumulativeSMUtilization += slotStatistics.CumulativeSMUtilization;
        totalCumulativeSMOccupancy += slotStatistics.CumulativeSMOccupancy;
        totalMaxMemoryUsed += slotStatistics.MaxMemoryUsed;
        totalMemoryTotal += gpuInfo.MemoryTotal;
    }

    statistics->AddSample("/user_job/gpu/cumulative_utilization_gpu", totalCumulativeUtilizationGpu);
    statistics->AddSample("/user_job/gpu/cumulative_utilization_memory", totalCumulativeUtilizationMemory);
    statistics->AddSample("/user_job/gpu/cumulative_utilization_power", totalCumulativeUtilizationPower);
    statistics->AddSample("/user_job/gpu/cumulative_memory", totalCumulativeMemory);
    statistics->AddSample("/user_job/gpu/cumulative_power", totalCumulativePower);
    statistics->AddSample("/user_job/gpu/cumulative_load", totalCumulativeLoad);
    statistics->AddSample("/user_job/gpu/max_memory_used", totalMaxMemoryUsed);
    statistics->AddSample("/user_job/gpu/memory_total", totalMemoryTotal);
    statistics->AddSample("/user_job/gpu/cumulative_sm_utilization", totalCumulativeSMUtilization);
    statistics->AddSample("/user_job/gpu/cumulative_sm_occupancy", totalCumulativeSMOccupancy);
}

void TJob::EnrichStatisticsWithDiskInfo(TStatistics* statistics)
{
    auto diskStatistics = Slot_->GetDiskStatistics();
    MaxDiskUsage_ = std::max(MaxDiskUsage_, diskStatistics.Usage);
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

    addIfPresent(Config_->JobController->JobSetupCommand);

    bool needGpu = NeedGpuLayers() || Config_->JobController->GpuManager->TestSetupCommands;
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

    rootFS.RootPath = RootVolume_->GetPath();
    rootFS.IsRootReadOnly = false;
    rootFS.Binds.reserve(Config_->RootFSBinds.size());

    for (const auto& bind : Config_->RootFSBinds) {
        rootFS.Binds.push_back({bind->ExternalPath, bind->InternalPath, bind->ReadOnly});
    }

    return rootFS;
}

TNodeJobReport TJob::MakeDefaultJobReport()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto report = TNodeJobReport()
        .Type(GetType())
        .State(GetState())
        .StartTime(GetStartTime())
        .SpecVersion(0) // TODO: fill correct spec version.
        .CoreInfos(CoreInfos_)
        .ExecAttributes(ConvertToYsonString(ExecAttributes_));
    if (FinishTime_) {
        report.SetFinishTime(*FinishTime_);
    }
    if (SchedulerJobSpecExt_->has_job_competition_id()) {
        report.SetJobCompetitionId(FromProto<TGuid>(SchedulerJobSpecExt_->job_competition_id()));
    }
    if (SchedulerJobSpecExt_->has_probing_job_competition_id()) {
        report.SetProbingJobCompetitionId(FromProto<TGuid>(SchedulerJobSpecExt_->probing_job_competition_id()));
    }
    if (SchedulerJobSpecExt_ && SchedulerJobSpecExt_->has_task_name()) {
        report.SetTaskName(SchedulerJobSpecExt_->task_name());
    }

    return report;
}


void TJob::InitializeJobProbe()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto probe = CreateJobProbe(Slot_->GetBusClientConfig(), Id_);
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
    if (SchedulerJobSpecExt_->has_user_job_spec()) {
        const auto& userJobSpec = SchedulerJobSpecExt_->user_job_spec();
        if (userJobSpec.has_cuda_toolkit_version()) {
            return true;
        }
    }

    return GetResourceUsage().gpu() > 0;
}

bool TJob::NeedGpu()
{
    return GetResourceUsage().gpu() > 0;
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

void TJob::CollectSensorsFromGpuInfo(ISensorWriter* writer)
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

    auto gpuInfoMap = Bootstrap_->GetGpuManager()->GetGpuInfoMap();
    for (int index = 0; index < std::ssize(GpuSlots_); ++index) {
        const auto& slot = GpuSlots_[index];

        auto it = gpuInfoMap.find(slot->GetDeviceNumber());
        if (it == gpuInfoMap.end()) {
            continue;
        }

        const auto& gpuInfo = it->second;

        TWithTagGuard tagGuard(writer, "gpu_slot", ToString(index));

        if (sensorNames.contains(UtilizationGpuName)) {
            ProfileSensor(UtilizationGpuName, writer, gpuInfo.UtilizationGpuRate);
        }

        if (sensorNames.contains(UtilizationMemoryName)) {
            ProfileSensor(UtilizationMemoryName, writer, gpuInfo.UtilizationMemoryRate);
        }
        if (sensorNames.contains(MemoryName)) {
            ProfileSensor(MemoryName, writer, gpuInfo.MemoryUsed);
        }

        if (sensorNames.contains(UtilizationPowerName)) {
            auto utilizationPower = gpuInfo.PowerLimit == 0
                ? 0
                : gpuInfo.PowerDraw / gpuInfo.PowerLimit;
            ProfileSensor(UtilizationPowerName, writer, utilizationPower);
        }
        if (sensorNames.contains(PowerName)) {
            ProfileSensor(PowerName, writer, gpuInfo.PowerDraw);
        }
        if (sensorNames.contains(SMUtilizationName)) {
            ProfileSensor(SMUtilizationName, writer, gpuInfo.SMUtilizationRate);
        }
        if (sensorNames.contains(SMOccupancyName)) {
            ProfileSensor(SMOccupancyName, writer, gpuInfo.SMOccupancyRate);
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
        .WithTimeout(Config_->SensorDumpTimeout);
}

////////////////////////////////////////////////////////////////////////////////

TJobPtr CreateJob(
    TJobId jobId,
    TOperationId operationId,
    const TNodeResources& resourceUsage,
    TJobSpec&& jobSpec,
    IBootstrap* bootstrap,
    TControllerAgentDescriptor agentDescriptor)
{
    return New<TJob>(
        jobId,
        operationId,
        resourceUsage,
        std::move(jobSpec),
        bootstrap,
        std::move(agentDescriptor));
}

////////////////////////////////////////////////////////////////////////////////

void FillStatus(NScheduler::NProto::TAllocationStatus* status, const TJobPtr& job)
{
    using NYT::ToProto;

    ToProto(status->mutable_allocation_id(), job->GetId());

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
    ToProto(status->mutable_output_data_statistics(), job->GetOutputDataStatistics());

    if (auto stderrSize = job->GetStderrSize(); stderrSize > 0) {
        status->set_stderr_size(stderrSize);
    }
    if (const auto& preemptedFor = job->GetPreemptedFor()) {
        ToProto(status->mutable_preempted_for(), *preemptedFor);
    }
}

void FillStatus(NJobTrackerClient::NProto::TJobStatus* status, const TJobPtr& job)
{
    ToProto(status->mutable_job_id(), job->GetId());
    status->set_state(ToProto<int>(job->GetState()));
}

template <class TStatus>
void FillJobStatus(TStatus* status, const TJobPtr& job)
{
    FillStatus(status, job);

    ToProto(status->mutable_operation_id(), job->GetOperationId());
    ToProto(status->mutable_time_statistics(), job->GetTimeStatistics());

    status->set_status_timestamp(ToProto<ui64>(TInstant::Now()));
}

template void FillJobStatus(NScheduler::NProto::TAllocationStatus* status, const TJobPtr& job);
template void FillJobStatus(NControllerAgent::NProto::TJobStatus* status, const TJobPtr& job);
template void FillJobStatus(NJobTrackerClient::NProto::TJobStatus* status, const TJobPtr& job);

////////////////////////////////////////////////////////////////////////////////

bool TControllerAgentDescriptor::operator==(const TControllerAgentDescriptor& other) const noexcept
{
    return other.Address == Address && other.IncarnationId == IncarnationId;
}

bool TControllerAgentDescriptor::operator!=(const TControllerAgentDescriptor& other) const noexcept
{
    return !(*this == other);
}

bool TControllerAgentDescriptor::Empty() const noexcept
{
    return *this == TControllerAgentDescriptor{};
}

TControllerAgentDescriptor::operator bool() const noexcept
{
    return !Empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

size_t THash<NYT::NExecNode::TControllerAgentDescriptor>::operator () (
    const NYT::NExecNode::TControllerAgentDescriptor& descriptor) const
{
    size_t hash = THash<decltype(descriptor.Address)>{}(descriptor.Address);
    NYT::HashCombine(hash, descriptor.IncarnationId);

    return hash;
}
