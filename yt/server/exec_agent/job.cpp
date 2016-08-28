#include "job.h"
#include "private.h"
#include "config.h"
#include "slot.h"
#include "slot_manager.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/artifact.h>
#include <yt/server/data_node/chunk_cache.h>
#include <yt/server/data_node/master_connector.h>
#include <yt/server/data_node/chunk.h>

#include <yt/server/job_agent/job.h>

#include <yt/server/scheduler/config.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/actions/cancelable_context.h>

#include <yt/core/logging/log_manager.h>
#include <yt/core/misc/proc.h>

namespace NYT {
namespace NExecAgent {

using namespace NRpc;
using namespace NJobProxy;
using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NFileClient;
using namespace NCellNode;
using namespace NDataNode;
using namespace NCellNode;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NJobProberClient;
using namespace NJobTrackerClient::NProto;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NConcurrency;
using namespace NApi;

using NNodeTrackerClient::TNodeDirectory;
using NScheduler::NProto::TUserJobSpec;

////////////////////////////////////////////////////////////////////////////////

struct TArtifactInfo
{
    Stroka Name;
    bool IsExecutable;
    TArtifactKey Key;
};

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public NJobAgent::IJob
{
public:
    DEFINE_SIGNAL(void(const TNodeResources&), ResourcesUpdated);

public:
    TJob(
        const TJobId& jobId,
        const TOperationId& operationId,
        const TNodeResources& resourceUsage,
        TJobSpec&& jobSpec,
        TBootstrap* bootstrap)
        : Id_(jobId)
        , OperationId_(operationId)
        , Bootstrap_(bootstrap)
        , Statistics_("{}")
        , ResourceUsage_(resourceUsage)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        JobSpec_.Swap(&jobSpec);

        const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        if (schedulerJobSpecExt.has_aux_node_directory()) {
            AuxNodeDirectory_->MergeFrom(schedulerJobSpecExt.aux_node_directory());
        }

        Invoker_ = Bootstrap_->GetControlInvoker();
        InitializeArtifacts();

        Logger.AddTag("JobId: %v, OperationId: %v, JobType: %v",
            Id_,
            OperationId_,
            GetType());
    }

    virtual void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        switch (JobPhase_) {
            case EJobPhase::Created:
                break;

            default:
                LOG_DEBUG("Cannot start job (JobState: %lv, JobPhase: %lv)", JobState_, JobPhase_);
                return;
        }

        GuardedAction([&] () {
            JobState_ = EJobState::Running;
            PrepareTime_ = TInstant::Now();

            auto slotManager = Bootstrap_->GetExecSlotManager();
            Slot_ = slotManager->AcquireSlot();

            JobPhase_ = EJobPhase::DownloadingArtifacts;
            auto artifactsFuture = DownloadArtifacts();
            artifactsFuture.Subscribe(BIND(
                &TJob::OnArtifactsDownloaded,
                MakeWeak(this))
            .Via(Invoker_));
            ArtifactsFuture_ = artifactsFuture.As<void>();
        });
    }

    virtual void Abort(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        LOG_INFO(error, "Job abort requested");

        switch (JobPhase_) {
            case EJobPhase::Created:
            case EJobPhase::DownloadingArtifacts:
            case EJobPhase::Running:
                JobState_ = EJobState::Aborting;
                ArtifactsFuture_.Cancel();
                DoSetResult(error);
                Cleanup();
                break;

            case EJobPhase::PreparingDirectories:
            case EJobPhase::PreparingArtifacts:
            case EJobPhase::PreparingProxy:
                // Wait for the next event handler to complete the abortion.
                JobState_ = EJobState::Aborting;
                JobPhase_ = EJobPhase::WaitingAbort;
                DoSetResult(error);
                Slot_->CancelPreparation();
                break;

            default:
                LOG_DEBUG("Cannot abort job (JobState: %v, JobPhase: %v)", JobState_, JobPhase_);
                break;
        }
    }

    virtual void OnJobPrepared() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        GuardedAction([&] () {
            ValidateJobPhase(EJobPhase::PreparingProxy);
            JobPhase_ = EJobPhase::Running;
        });
    }

    virtual void SetResult(const TJobResult& jobResult) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        GuardedAction([&] () {
            JobPhase_ = EJobPhase::FinalizingProxy;
            DoSetResult(jobResult);
        });
    }

    virtual const TJobId& GetId() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return Id_;
    }

    virtual const TJobId& GetOperationId() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return OperationId_;
    }

    virtual EJobType GetType() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return EJobType(JobSpec_.type());
    }

    virtual const TJobSpec& GetSpec() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return JobSpec_;
    }

    virtual EJobState GetState() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return JobState_;
    }

    virtual TNullable<TDuration> GetPrepareDuration() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        if (!PrepareTime_) {
            return Null;
        } else if (!ExecTime_) {
            return TInstant::Now() - *PrepareTime_;
        } else {
            return *ExecTime_ - *PrepareTime_;
        }
    }

    virtual TNullable<TDuration> GetExecDuration() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        if (!ExecTime_) {
            return Null;
        } else if (!FinishTime_) {
            return TInstant::Now() - *ExecTime_;
        } else {
            return *FinishTime_ - *ExecTime_;
        }
    }

    virtual EJobPhase GetPhase() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return JobPhase_;
    }

    virtual TNodeResources GetResourceUsage() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return ResourceUsage_;
    }

    virtual TJobResult GetResult() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return JobResult_.Get();
    }

    virtual double GetProgress() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return Progress_;
    }

    virtual void SetResourceUsage(const TNodeResources& newUsage) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        if (JobPhase_ == EJobPhase::Running) {
            auto delta = newUsage - ResourceUsage_;
            ResourceUsage_ = newUsage;
            ResourcesUpdated_.Fire(delta);
        }
    }

    virtual void SetProgress(double progress) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        if (JobPhase_ == EJobPhase::Running) {
            Progress_ = progress;
        }
    }

    virtual TNullable<TYsonString> GetStatistics() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return Statistics_;
    }

    virtual TInstant GetStatisticsLastSendTime() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return StatisticsLastSendTime_;
    }

    virtual void ResetStatisticsLastSendTime() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        StatisticsLastSendTime_ = TInstant::Now();
    }

    virtual void SetStatistics(const TYsonString& statistics) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        if (JobPhase_ == EJobPhase::Running || JobPhase_ == EJobPhase::FinalizingProxy) {
            Statistics_ = statistics;
        }
    }

    virtual std::vector<TChunkId> DumpInputContext() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        ValidateJobRunning();

        auto proxy = Slot_->GetJobProberProxy();
        auto req = proxy.DumpInputContext();

        ToProto(req->mutable_job_id(), Id_);
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting input contexts dump from job proxy");
        const auto& rsp = rspOrError.Value();

        return FromProto<std::vector<TChunkId>>(rsp->chunk_ids());
    }

    virtual TYsonString Strace() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        ValidateJobRunning();

        auto proxy = Slot_->GetJobProberProxy();
        auto req = proxy.Strace();

        ToProto(req->mutable_job_id(), Id_);
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting strace dump from job proxy");
        const auto& rsp = rspOrError.Value();

        return TYsonString(rsp->trace());
    }

    virtual void SignalJob(const Stroka& signalName) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        ValidateJobRunning();
        auto proxy = Slot_->GetJobProberProxy();

        Signaled_ = true;
        auto req = proxy.SignalJob();

        ToProto(req->mutable_job_id(), Id_);
        ToProto(req->mutable_signal_name(), signalName);
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error sending signal to job proxy");
    }

    virtual TYsonString PollJobShell(const TYsonString& parameters) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        ValidateJobRunning();

        auto proxy = Slot_->GetJobProberProxy();
        auto req = proxy.PollJobShell();

        ToProto(req->mutable_job_id(), Id_);
        ToProto(req->mutable_parameters(), parameters.Data());
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error polling job shell");
        const auto& rsp = rspOrError.Value();

        return TYsonString(rsp->result());
    }

private:
    const TJobId Id_;
    const TOperationId OperationId_;
    NCellNode::TBootstrap* const Bootstrap_;

    IInvokerPtr Invoker_;
    TJobSpec JobSpec_;

    // Used to terminate artifacts downloading in case of cancelation.
    TFuture<void> ArtifactsFuture_ = VoidFuture;

    double Progress_ = 0.0;

    TYsonString Statistics_;
    TInstant StatisticsLastSendTime_ = TInstant::Now();

    bool Signaled_ = false;

    TNullable<TJobResult> JobResult_;

    TNullable<TInstant> PrepareTime_;
    TNullable<TInstant> ExecTime_;
    TNullable<TInstant> FinishTime_;


    ISlotPtr Slot_;
    TNullable<Stroka> TmpfsPath_;

    struct TArtifact
    {
        ESandboxKind SandboxKind;
        Stroka Name;
        bool IsExecutable;
        TArtifactKey Key;
        NDataNode::IChunkPtr Chunk;
    };

    std::vector<TArtifact> Artifacts_;

    TNodeDirectoryPtr AuxNodeDirectory_ = New<TNodeDirectory>();

    TNodeResources ResourceUsage_;
    EJobState JobState_ = EJobState::Waiting;
    EJobPhase JobPhase_ = EJobPhase::Created;

    DECLARE_THREAD_AFFINITY_SLOT(ControllerThread);
    NLogging::TLogger Logger = ExecAgentLogger;

    // Helpers.

    void ValidateJobRunning() const
    {
        if (JobPhase_ != EJobPhase::Running) {
            THROW_ERROR_EXCEPTION("Job %v is not running", Id_)
                << TErrorAttribute("job_state", JobState_)
                << TErrorAttribute("job_phase", JobPhase_);
        }
    }

    void DoSetResult(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        TJobResult jobResult;
        ToProto(jobResult.mutable_error(), error);
        DoSetResult(jobResult);
    }

    void DoSetResult(const TJobResult& jobResult)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        if (JobResult_) {
            auto error = FromProto<TError>(JobResult_->error());
            if (!error.IsOK()) {
                return;
            }
        }

        JobResult_ = jobResult;
        FinishTime_ = TInstant::Now();
    }

    bool HandleFinishingPhase()
    {
        switch (JobPhase_) {
            case EJobPhase::WaitingAbort:
                Cleanup();
                return true;

            case EJobPhase::Cleanup:
            case EJobPhase::Finished:
                return true;

            case EJobPhase::Created:
                YCHECK(JobState_ == EJobState::Waiting);
                return false;

            default:
                YCHECK(JobState_ == EJobState::Running);
                return false;
        }
    }

    void ValidateJobPhase(EJobPhase expectedPhase)
    {
        if (JobPhase_ != expectedPhase) {
            THROW_ERROR_EXCEPTION("Unexpected job phase")
                << TErrorAttribute("expected_phase", expectedPhase)
                << TErrorAttribute("actual_phase", JobPhase_);
        }
    }

    // Event handlers.

    void OnArtifactsDownloaded(const TErrorOr<std::vector<NDataNode::IChunkPtr>>& errorOrArtifacts)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        GuardedAction([&] () {
            ValidateJobPhase(EJobPhase::DownloadingArtifacts);
            THROW_ERROR_EXCEPTION_IF_FAILED(errorOrArtifacts, "Failed to download artifacts")
            const auto& chunks = errorOrArtifacts.Value();

            for (size_t index = 0; index < Artifacts_.size(); ++index) {
                Artifacts_[index].Chunk = chunks[index];
            }

            JobPhase_ = EJobPhase::PreparingDirectories;
            BIND(&TJob::PrepareDirectories, MakeStrong(this))
                .AsyncVia(Invoker_)
                .Run()
                .Subscribe(BIND(
                    &TJob::OnDirectoriesPrepared,
                    MakeWeak(this))
                .Via(Invoker_));
        });
    }

    void OnDirectoriesPrepared(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        GuardedAction([&] () {
            ValidateJobPhase(EJobPhase::PreparingDirectories);
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to prepare sandbox directories");

            JobPhase_ = EJobPhase::PreparingArtifacts;
            BIND(&TJob::PrepareArtifacts, MakeWeak(this))
                .AsyncVia(Invoker_)
                .Run()
                .Subscribe(BIND(
                    &TJob::OnArtifactsPrepared,
                    MakeWeak(this))
                .Via(Invoker_));
        });
    }

    void OnArtifactsPrepared(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        GuardedAction([&] () {
            ValidateJobPhase(EJobPhase::PreparingArtifacts);
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to prepare artifacts");

            ExecTime_ = TInstant::Now();
            JobPhase_ = EJobPhase::PreparingProxy;

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
        });
    }

    void OnJobProxyFinished(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        if (HandleFinishingPhase()) {
            return;
        }

        if (!error.IsOK()) {
            DoSetResult(TError("Job proxy failed") << error);
        }

        Cleanup();
    }

    void GuardedAction(std::function<void()> action)
    {
        if (HandleFinishingPhase()) {
            return;
        }

        try {
            TContextSwitchedGuard contextSwitchGuard(BIND([] { Y_UNREACHABLE(); }));
            action();
        } catch (const std::exception& ex) {
            DoSetResult(ex);
            Cleanup();
        }
    }

    // Finalization.
    void Cleanup()
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        if (JobPhase_ == EJobPhase::Cleanup || JobPhase_ == EJobPhase::Finished) {
            return;
        }

        FinishTime_ = TInstant::Now();
        JobPhase_ = EJobPhase::Cleanup;

        if (Slot_) {
            try {
                Slot_->Cleanup();
            } catch (const std::exception& ex) {
                // Errors during cleanup phase do not affect job outcome.
                LOG_ERROR(ex, "Failed to clean up slot %v", Slot_->GetSlotIndex());
            }

            Bootstrap_->GetExecSlotManager()->ReleaseSlot(Slot_->GetSlotIndex());
        }

        JobPhase_ = EJobPhase::Finished;
        FinalizeJob();
    }

    void FinalizeJob()
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        YCHECK(JobResult_);

        auto resourceDelta = ZeroNodeResources() - ResourceUsage_;
        ResourceUsage_ = ZeroNodeResources();

        if (JobState_ != EJobState::Waiting) {
            ResourcesUpdated_.Fire(resourceDelta);
        }

        auto error = FromProto<TError>(JobResult_->error());

        if (error.IsOK()) {
            JobState_ = EJobState::Completed;
            return;
        }

        if (IsFatalError(error)) {
            error.Attributes().Set("fatal", IsFatalError(error));
            ToProto(JobResult_->mutable_error(), error);
            JobState_ = EJobState::Failed;
            return;
        }

        auto abortReason = GetAbortReason(*JobResult_);
        if (abortReason) {
            error.Attributes().Set("abort_reason", abortReason);
            ToProto(JobResult_->mutable_error(), error);
            JobState_ = EJobState::Aborted;
            return;
        }

        JobState_ = EJobState::Failed;
    }

    // Preparation.

    TJobProxyConfigPtr CreateConfig()
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        INodePtr ioConfigNode;
        try {
            const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            ioConfigNode = ConvertToNode(TYsonString(schedulerJobSpecExt.io_config()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error deserializing job IO configuration")
                << ex;
        }

        auto ioConfig = New<TJobIOConfig>();
        try {
            ioConfig->Load(ioConfigNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error validating job IO configuration")
                << ex;
        }

        auto proxyConfig = CloneYsonSerializable(Bootstrap_->GetJobProxyConfig());
        proxyConfig->JobIO = ioConfig;
        proxyConfig->RpcServer = Slot_->GetRpcServerConfig();
        proxyConfig->TmpfsPath = TmpfsPath_;
        proxyConfig->SlotIndex = Slot_->GetSlotIndex();

        return proxyConfig;
    }

    void PrepareDirectories()
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        WaitFor(Slot_->CreateSandboxDirectories())
            .ThrowOnError();

        const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        if (schedulerJobSpecExt.has_user_job_spec()) {
            const auto& userJobSpec = schedulerJobSpecExt.user_job_spec();
            if (userJobSpec.has_tmpfs_path() && Bootstrap_->GetConfig()->ExecAgent->SlotManager->EnableTmpfs) {
                TmpfsPath_ = WaitFor(Slot_->PrepareTmpfs(
                    ESandboxKind::User,
                    userJobSpec.tmpfs_size(),
                    userJobSpec.tmpfs_path()))
                .ValueOrThrow();
            }
        }
    }

    void InitializeArtifacts()
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        if (schedulerJobSpecExt.has_user_job_spec()) {
            const auto& userJobSpec = schedulerJobSpecExt.user_job_spec();
            for (const auto& descriptor : userJobSpec.files()) {
                Artifacts_.push_back(TArtifact{
                    ESandboxKind::User,
                    descriptor.file_name(),
                    descriptor.executable(),
                    TArtifactKey(descriptor),
                    nullptr});
            }
        }

        if (schedulerJobSpecExt.has_input_query_spec()) {
            const auto& querySpec = schedulerJobSpecExt.input_query_spec();

            AuxNodeDirectory_->MergeFrom(querySpec.node_directory());

            for (const auto& function : querySpec.external_functions()) {
                TArtifactKey key;
                key.set_type(static_cast<int>(NObjectClient::EObjectType::File));
                key.mutable_chunks()->MergeFrom(function.chunk_specs());

                Artifacts_.push_back(TArtifact{
                    ESandboxKind::Udf,
                    function.name(),
                    false,
                    key,
                    nullptr});
            }
        }
    }

    TFuture<std::vector<NDataNode::IChunkPtr>> DownloadArtifacts()
    {
        auto chunkCache = Bootstrap_->GetChunkCache();

        std::vector<TFuture<IChunkPtr>> asyncChunks;
        for (const auto& artifact : Artifacts_) {
            LOG_INFO("Downloading user file (FileName: %v, SandboxKind: %v)",
                artifact.Name,
                artifact.SandboxKind);

            auto asyncChunk = chunkCache->PrepareArtifact(artifact.Key, AuxNodeDirectory_)
                .Apply(BIND([fileName = artifact.Name] (const TErrorOr<IChunkPtr>& chunkOrError) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(chunkOrError,
                        "Failed to prepare user file %Qv",
                        fileName);

                    return chunkOrError
                        .Value();
                }));

            asyncChunks.push_back(asyncChunk);
        }

        return Combine(asyncChunks);
    }

    //! Putting files to sandbox.
    void PrepareArtifacts()
    {
        const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        bool copyFiles = schedulerJobSpecExt.has_user_job_spec() && schedulerJobSpecExt.user_job_spec().copy_files();

        for (const auto& artifact : Artifacts_) {
            YCHECK(artifact.Chunk);

            if (copyFiles) {
                LOG_INFO("Copying artifact (FileName: %v, IsExecutable: %v, SandboxKind: %v)",
                    artifact.Name,
                    artifact.IsExecutable,
                    artifact.SandboxKind);

                WaitFor(Slot_->MakeCopy(
                    artifact.SandboxKind,
                    artifact.Chunk->GetFileName(),
                    artifact.Name,
                    artifact.IsExecutable))
                .ThrowOnError();
            } else {
                LOG_INFO("Making symlink for artifact (FileName: %v, IsExecutable: %v, SandboxKind: %v)",
                    artifact.Name,
                    artifact.IsExecutable,
                    artifact.SandboxKind);

                WaitFor(Slot_->MakeLink(
                    artifact.SandboxKind,
                    artifact.Chunk->GetFileName(),
                    artifact.Name,
                    artifact.IsExecutable))
                .ThrowOnError();
            }

            LOG_INFO("Artifact prepared successfully (FileName: %v, SandboxKind: %v)",
                artifact.Name,
                artifact.SandboxKind);
        }
    }

    // Analyse results.

    static TError BuildJobProxyError(const TError& spawnError)
    {
        if (spawnError.IsOK()) {
            return TError();
        }

        auto jobProxyError = TError("Job proxy failed") << spawnError;

        if (spawnError.GetCode() == EProcessErrorCode::NonZeroExitCode) {
            // Try to translate the numeric exit code into some human readable reason.
            auto reason = EJobProxyExitCode(spawnError.Attributes().Get<int>("exit_code"));
            const auto& validReasons = TEnumTraits<EJobProxyExitCode>::GetDomainValues();
            if (std::find(validReasons.begin(), validReasons.end(), reason) != validReasons.end()) {
                jobProxyError.Attributes().Set("reason", reason);
            }
        }

        return jobProxyError;
    }

    TNullable<EAbortReason> GetAbortReason(const TJobResult& jobResult)
    {
        if (jobResult.HasExtension(TSchedulerJobResultExt::scheduler_job_result_ext)) {
            const auto& schedulerResultExt = jobResult.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            if (schedulerResultExt.failed_chunk_ids_size() > 0) {
                return EAbortReason::FailedChunks;
            }
        }

        auto resultError = FromProto<TError>(jobResult.error());
        if (resultError.FindMatching(NExecAgent::EErrorCode::ResourceOverdraft)) {
            return EAbortReason::ResourceOverdraft;
        }

        if (resultError.FindMatching(NExecAgent::EErrorCode::WaitingJobTimeout)) {
            return EAbortReason::WaitingTimeout;
        }

        if (resultError.FindMatching(NExecAgent::EErrorCode::AbortByScheduler)) {
            return EAbortReason::Scheduler;
        }

        if (resultError.FindMatching(NChunkClient::EErrorCode::AllTargetNodesFailed) ||
            resultError.FindMatching(NChunkClient::EErrorCode::MasterCommunicationFailed) ||
            resultError.FindMatching(NChunkClient::EErrorCode::MasterNotConnected) ||
            resultError.FindMatching(NExecAgent::EErrorCode::ConfigCreationFailed) ||
            resultError.FindMatching(NExecAgent::EErrorCode::AllLocationsDisabled) ||
            resultError.FindMatching(NExecAgent::EErrorCode::JobEnvironmentDisabled) ||
            resultError.FindMatching(NJobProxy::EErrorCode::MemoryCheckFailed))
        {
            return EAbortReason::Other;
        }

        if (auto processError = resultError.FindMatching(EProcessErrorCode::NonZeroExitCode)) {
            auto exitCode = NExecAgent::EJobProxyExitCode(processError->Attributes().Get<int>("exit_code"));
            if (exitCode == EJobProxyExitCode::HeartbeatFailed ||
                exitCode == EJobProxyExitCode::ResultReportFailed ||
                exitCode == EJobProxyExitCode::ResourcesUpdateFailed ||
                exitCode == EJobProxyExitCode::GetJobSpecFailed)
            {
                return EAbortReason::Other;
            }
        }

        if (Signaled_) {
            return EAbortReason::UserRequest;
        }

        return Null;
    }

    static bool IsFatalError(const TError& error)
    {
        return
            error.FindMatching(NTableClient::EErrorCode::SortOrderViolation) ||
            error.FindMatching(NSecurityClient::EErrorCode::AuthenticationError) ||
            error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError) ||
            error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded) ||
            error.FindMatching(NSecurityClient::EErrorCode::NoSuchAccount) ||
            error.FindMatching(NNodeTrackerClient::EErrorCode::NoSuchNetwork) ||
            error.FindMatching(NTableClient::EErrorCode::InvalidDoubleValue) ||
            error.FindMatching(NTableClient::EErrorCode::IncomparableType) ||
            error.FindMatching(NTableClient::EErrorCode::UnhashableType) ||
            error.FindMatching(NTableClient::EErrorCode::CorruptedNameTable);
    }
};

////////////////////////////////////////////////////////////////////////////////

NJobAgent::IJobPtr CreateUserJob(
    const TJobId& jobId,
    const TOperationId& operationId,
    const TNodeResources& resourceUsage,
    TJobSpec&& jobSpec,
    TBootstrap* bootstrap)
{
    return New<TJob>(
        jobId,
        operationId,
        resourceUsage,
        std::move(jobSpec),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT



