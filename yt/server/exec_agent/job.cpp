#include "job.h"
#include "private.h"
#include "config.h"
#include "slot.h"
#include "slot_manager.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/containers/public.h>

#include <yt/server/data_node/artifact.h>
#include <yt/server/data_node/chunk.h>
#include <yt/server/data_node/chunk_cache.h>
#include <yt/server/data_node/location.h>
#include <yt/server/data_node/master_connector.h>
#include <yt/server/data_node/volume_manager.h>

#include <yt/server/job_agent/job.h>
#include <yt/server/job_agent/gpu_manager.h>
#include <yt/server/job_agent/statistics_reporter.h>

#include <yt/server/scheduler/config.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/ytlib/job_prober_client/public.h>
#include <yt/ytlib/job_prober_client/job_probe.h>
#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/job_proxy/public.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/client/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/delayed_executor.h>

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
using namespace NJobAgent;
using namespace NJobProberClient;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NConcurrency;
using namespace NApi;

using NNodeTrackerClient::TNodeDirectory;
using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public NJobAgent::IJob
{
public:
    DEFINE_SIGNAL(void(const TNodeResources&), ResourcesUpdated);
    DEFINE_SIGNAL(void(), PortsReleased);

public:
    TJob(
        const TJobId& jobId,
        const TOperationId& operationId,
        const TNodeResources& resourceUsage,
        TJobSpec&& jobSpec,
        NCellNode::TBootstrap* bootstrap)
        : Id_(jobId)
        , OperationId_(operationId)
        , Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->ExecAgent)
        , Invoker_(Bootstrap_->GetControlInvoker())
        , StartTime_(TInstant::Now())
        , TrafficMeter_(New<TTrafficMeter>(
            Bootstrap_->GetMasterConnector()->GetLocalDescriptor().GetDataCenter()))
        , ResourceUsage_(resourceUsage)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        JobSpec_.Swap(&jobSpec);

        TrafficMeter_->Start();

        const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        AbortJobIfAccountLimitExceeded_ = schedulerJobSpecExt.abort_job_if_account_limit_exceeded();

        Logger.AddTag("JobId: %v, OperationId: %v, JobType: %v",
            Id_,
            OperationId_,
            GetType());

        JobEvents_.emplace_back(JobState_, JobPhase_);
        ReportStatistics(
            TJobStatistics()
                .Type(GetType())
                .State(GetState())
                .StartTime(TInstant::Now()) // TODO(ignat): fill correct start time.
                .Events(JobEvents_));
    }

    virtual void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        if (JobPhase_ != EJobPhase::Created) {
            LOG_DEBUG("Cannot start job, unexpected job phase (JobState: %v, JobPhase: %v)",
                JobState_,
                JobPhase_);
            return;
        }

        GuardedAction([&] () {
            SetJobState(EJobState::Running);
            PrepareTime_ = TInstant::Now();

            LOG_INFO("Starting job");

            InitializeArtifacts();

            i64 diskSpaceLimit = Config_->MinRequiredDiskSpace;

            const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            if (schedulerJobSpecExt.has_user_job_spec()) {
                const auto& userJobSpec = schedulerJobSpecExt.user_job_spec();
                if (userJobSpec.has_disk_space_limit()) {
                    diskSpaceLimit = userJobSpec.disk_space_limit();
                }
            }

            if (!Config_->JobController->TestGpu) {
                for (int i = 0; i < GetResourceUsage().gpu(); ++i) {
                    GpuSlots_.emplace_back(Bootstrap_->GetGpuManager()->AcquireGpuSlot());
                }
            }

            auto slotManager = Bootstrap_->GetExecSlotManager();
            Slot_ = slotManager->AcquireSlot(diskSpaceLimit);

            SetJobPhase(EJobPhase::PreparingNodeDirectory);
            BIND(&TJob::PrepareNodeDirectory, MakeWeak(this))
                .AsyncVia(Invoker_)
                .Run()
                .Subscribe(
                    BIND(&TJob::OnNodeDirectoryPrepared, MakeWeak(this))
                        .Via(Invoker_));
        });
    }

    virtual void Abort(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        LOG_INFO(error, "Job abort requested (Phase: %v)", JobPhase_);

        switch (JobPhase_) {
            case EJobPhase::Created:
            case EJobPhase::PreparingNodeDirectory:
            case EJobPhase::DownloadingArtifacts:
            case EJobPhase::Running:
                SetJobStatePhase(EJobState::Aborting, EJobPhase::WaitingAbort);
                ArtifactsFuture_.Cancel();
                DoSetResult(error);

                // Do the actual cleanup asynchronously.
                BIND(&TJob::Cleanup, MakeStrong(this))
                    .Via(Bootstrap_->GetControlInvoker())
                    .Run();

                break;

            case EJobPhase::PreparingSandboxDirectories:
            case EJobPhase::PreparingArtifacts:
            case EJobPhase::PreparingRootVolume:
            case EJobPhase::PreparingProxy:
                // Wait for the next event handler to complete the abortion.
                SetJobStatePhase(EJobState::Aborting, EJobPhase::WaitingAbort);
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

        GuardedAction([&] {
            LOG_INFO("Job prepared");

            ValidateJobPhase(EJobPhase::PreparingProxy);
            SetJobPhase(EJobPhase::Running);
        });
    }

    virtual void SetResult(const TJobResult& jobResult) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        GuardedAction([&] () {
            SetJobPhase(EJobPhase::FinalizingProxy);
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

    virtual int GetPortCount() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        if (schedulerJobSpecExt.has_user_job_spec()) {
            return schedulerJobSpecExt.user_job_spec().port_count();
        }

        return 0;
    }

    virtual void SetPorts(const std::vector<int>& ports) override
    {
        Ports_ = ports;
    }

    virtual EJobState GetState() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        return JobState_;
    }

    virtual TInstant GetStartTime() const override
    {
        return StartTime_;
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

    virtual TNullable<TDuration> GetDownloadDuration() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        if (!PrepareTime_) {
            return Null;
        } else if (!CopyTime_) {
            return TInstant::Now() - *PrepareTime_;
        } else {
            return *CopyTime_ - *PrepareTime_;
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

    virtual std::vector<int> GetPorts() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        return Ports_;
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

    virtual ui64 GetStderrSize() const override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        return StderrSize_;
    }

    virtual void SetStderrSize(ui64 value) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        StderrSize_ = value;
    }

    virtual void SetStderr(const TString& value) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        Stderr_ = value;
    }

    virtual void SetFailContext(const TString& value) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        FailContext_ = value;
    }

    virtual TYsonString GetStatistics() const override
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
            ReportStatistics(TJobStatistics()
                .Statistics(Statistics_)
                .Type(GetType())
                .State(GetState()));
        }
    }

    virtual std::vector<TChunkId> DumpInputContext() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        ValidateJobRunning();

        try {
            return Slot_->GetJobProberClient()->DumpInputContext();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error requesting input contexts dump from job proxy")
                << ex;
        }
    }

    virtual TString GetStderr() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        if (Stderr_) {
            return *Stderr_;
        }

        ValidateJobRunning();

        try {
            return Slot_->GetJobProberClient()->GetStderr();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error requesting stderr from job proxy")
                << ex;
        }
    }

    virtual TNullable<TString> GetFailContext() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        return FailContext_;
    }

    virtual TYsonString StraceJob() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        ValidateJobRunning();

        try {
            return Slot_->GetJobProberClient()->StraceJob();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error requesting strace dump from job proxy")
                << ex;
        }
    }

    virtual void SignalJob(const TString& signalName) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        ValidateJobRunning();

        Signaled_ = true;

        try {
            Slot_->GetJobProberClient()->SignalJob(signalName);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error sending signal to job proxy")
                << ex;
        }
    }

    virtual TYsonString PollJobShell(const TYsonString& parameters) override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        ValidateJobRunning();

        try {
            return Slot_->GetJobProberClient()->PollJobShell(parameters);
        } catch (const TErrorException& ex) {
            // The following code changes error code for more user-friendly
            // diagnostics in interactive shell
            if (ex.Error().FindMatching(NRpc::EErrorCode::TransportError)) {
                THROW_ERROR_EXCEPTION(NExecAgent::EErrorCode::JobProxyConnectionFailed,
                    "No connection to job proxy")
                    << ex;
            }
            THROW_ERROR_EXCEPTION("Error polling job shell")
                << ex;
        }
    }

    virtual void ReportStatistics(TJobStatistics&& statistics) override
    {
        Bootstrap_->GetStatisticsReporter()->ReportStatistics(
            std::move(statistics).OperationId(GetOperationId()).JobId(GetId()));
    }

    virtual void ReportSpec() override
    {
        ReportStatistics(
            TJobStatistics()
                .Type(GetType())
                .State(GetState())
                .Spec(JobSpec_)
                .StartTime(GetStartTime())
                .SpecVersion(0) // TODO: fill correct spec version.
                .Events(JobEvents_));
    }

    virtual void ReportStderr() override
    {
        ReportStatistics(
            TJobStatistics()
                .Stderr(GetStderr()));
    }

    virtual void ReportFailContext() override
    {
        auto failContext = GetFailContext();

        if (failContext) {
            ReportStatistics(
                TJobStatistics()
                    .FailContext(*failContext));
        }
    }

    virtual void Interrupt() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        if (JobPhase_ < EJobPhase::Running) {
            Abort(TError(NJobProxy::EErrorCode::JobNotPrepared, "Interrupting job that has not started yet"));
            return;
        } else if (JobPhase_ > EJobPhase::Running) {
            // We're done with this job, no need to interrupt.
            return;
        }

        try {
            Slot_->GetJobProberClient()->Interrupt();
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

    virtual void Fail() override
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);
        ValidateJobRunning();

        try {
            Slot_->GetJobProberClient()->Fail();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error failing job on job proxy")
                    << ex;
        }
    }

    virtual bool GetStored() const override
    {
        return Stored_;
    }

    virtual void SetStored(bool value) override
    {
        Stored_ = value;
    }

private:
    const TJobId Id_;
    const TOperationId OperationId_;
    NCellNode::TBootstrap* const Bootstrap_;

    const TExecAgentConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TInstant StartTime_;
    const TTrafficMeterPtr TrafficMeter_;

    TJobSpec JobSpec_;

    bool AbortJobIfAccountLimitExceeded_;

    // Used to terminate artifacts downloading in case of cancelation.
    TFuture<void> ArtifactsFuture_ = VoidFuture;

    double Progress_ = 0.0;
    ui64 StderrSize_ = 0;

    TNullable<TString> Stderr_;
    TNullable<TString> FailContext_;

    TYsonString Statistics_ = TYsonString("{}");
    TInstant StatisticsLastSendTime_ = TInstant::Now();

    bool Signaled_ = false;

    TNullable<TJobResult> JobResult_;

    TNullable<TInstant> PrepareTime_;
    TNullable<TInstant> CopyTime_;
    TNullable<TInstant> ExecTime_;
    TNullable<TInstant> FinishTime_;

    std::vector<TGpuManager::TGpuSlotPtr> GpuSlots_;

    ISlotPtr Slot_;
    TNullable<TString> TmpfsPath_;

    struct TArtifact
    {
        ESandboxKind SandboxKind;
        TString Name;
        bool IsExecutable;
        TArtifactKey Key;
        NDataNode::IChunkPtr Chunk;
    };

    std::vector<TArtifact> Artifacts_;
    std::vector<TArtifactKey> LayerArtifactKeys_;

    IVolumePtr RootVolume_;

    TNodeResources ResourceUsage_;
    std::vector<int> Ports_;

    EJobState JobState_ = EJobState::Waiting;
    EJobPhase JobPhase_ = EJobPhase::Created;

    DECLARE_THREAD_AFFINITY_SLOT(ControllerThread);
    NLogging::TLogger Logger = ExecAgentLogger;

    TJobEvents JobEvents_;

    //! True if scheduler asked to store this job.
    bool Stored_ = false;

    // Helpers.

    template <class... U>
    void AddJobEvent(U&&... u)
    {
        JobEvents_.emplace_back(std::forward<U>(u)...);
        auto statistics = TJobStatistics()
            .Events(JobEvents_)
            .State(JobState_);
        if (FinishTime_) {
            statistics.SetFinishTime(*FinishTime_);
        }

        ReportStatistics(std::move(statistics));
    }

    void SetJobState(EJobState state)
    {
        JobState_ = state;
        AddJobEvent(state);
    }

    void SetJobPhase(EJobPhase phase)
    {
        JobPhase_ = phase;
        AddJobEvent(phase);
    }

    void SetJobStatePhase(EJobState state, EJobPhase phase)
    {
        JobState_ = state;
        JobPhase_ = phase;
        AddJobEvent(state, phase);
    }

    void ValidateJobRunning() const
    {
        if (JobPhase_ != EJobPhase::Running) {
            THROW_ERROR_EXCEPTION(NJobProberClient::EErrorCode::JobIsNotRunning, "Job %v is not running", Id_)
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

    void ValidateJobPhase(EJobPhase expectedPhase) const
    {
        if (JobPhase_ != expectedPhase) {
            THROW_ERROR_EXCEPTION("Unexpected job phase")
                << TErrorAttribute("expected_phase", expectedPhase)
                << TErrorAttribute("actual_phase", JobPhase_);
        }
    }

    // Event handlers.
    void OnNodeDirectoryPrepared(const TError& error)
    {
        GuardedAction([&] {
            ValidateJobPhase(EJobPhase::PreparingNodeDirectory);
            THROW_ERROR_EXCEPTION_IF_FAILED(error,
                NExecAgent::EErrorCode::NodeDirectoryPreparationFailed,
                "Failed to prepare job node directory");

            LOG_INFO("Node directory prepared");

            SetJobPhase(EJobPhase::DownloadingArtifacts);
            auto artifactsFuture = DownloadArtifacts();
            artifactsFuture.Subscribe(
                BIND(&TJob::OnArtifactsDownloaded, MakeWeak(this))
                    .Via(Invoker_));
            ArtifactsFuture_ = artifactsFuture.As<void>();
        });
    }

    void OnArtifactsDownloaded(const TErrorOr<std::vector<NDataNode::IChunkPtr>>& errorOrArtifacts)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        GuardedAction([&] {
            ValidateJobPhase(EJobPhase::DownloadingArtifacts);
            THROW_ERROR_EXCEPTION_IF_FAILED(errorOrArtifacts, "Failed to download artifacts");

            LOG_INFO("Artifacts downloaded");

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
                    &TJob::OnDirectoriesPrepared,
                    MakeWeak(this))
                .Via(Invoker_));
        });
    }

    void OnDirectoriesPrepared(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        GuardedAction([&] {
            ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to prepare sandbox directories");

            LOG_INFO("Slot directories prepared");

            SetJobPhase(EJobPhase::PreparingArtifacts);
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

        GuardedAction([&] {
            ValidateJobPhase(EJobPhase::PreparingArtifacts);
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to prepare artifacts");

            LOG_INFO("Artifacts prepared");
            if (LayerArtifactKeys_.empty()) {
                RunJobProxy();
            } else {
                SetJobPhase(EJobPhase::PreparingRootVolume);
                LOG_INFO("Preparing root volume (LayerCount: %v)", LayerArtifactKeys_.size());
                Slot_->PrepareRootVolume(LayerArtifactKeys_)
                    .Subscribe(BIND(
                        &TJob::OnVolumePrepared,
                        MakeWeak(this))
                    .Via(Invoker_));
            }
        });
    }

    void OnVolumePrepared(const TErrorOr<IVolumePtr>& volumeOrError)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        GuardedAction([&] {
            ValidateJobPhase(EJobPhase::PreparingRootVolume);
            if (!volumeOrError.IsOK()) {
                THROW_ERROR TError(EErrorCode::RootVolumePreparationFailed, "Failed to prepare artifacts")
                    << volumeOrError;
            }

            RootVolume_ = volumeOrError.Value();
            RunJobProxy();
        });
    }

    void RunJobProxy()
    {
        ExecTime_ = TInstant::Now();
        SetJobPhase(EJobPhase::PreparingProxy);

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
    }

    void OnJobProxyFinished(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        if (HandleFinishingPhase()) {
            return;
        }

        LOG_INFO("Job proxy finished");

        if (!error.IsOK()) {
            DoSetResult(TError("Job proxy failed")
                << BuildJobProxyError(error));
        }

        Cleanup();
    }

    void GuardedAction(std::function<void()> action)
    {
        if (HandleFinishingPhase()) {
            return;
        }

        try {
            TForbidContextSwitchGuard contextSwitchGuard;
            action();
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Error preparing scheduler job");

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

        LOG_INFO("Cleaning up after scheduler job");

        FinishTime_ = TInstant::Now();
        SetJobPhase(EJobPhase::Cleanup);

        if (Slot_) {
            try {
                LOG_DEBUG("Clean processes (SlotIndex: %v)", Slot_->GetSlotIndex());
                Slot_->CleanProcesses();
            } catch (const std::exception& ex) {
                // Errors during cleanup phase do not affect job outcome.
                LOG_ERROR(ex, "Failed to clean processed (SlotIndex: %v)", Slot_->GetSlotIndex());
            }
        }

        YCHECK(JobResult_);

        // Copy info from traffic meter to statistics.
        auto deserializedStatistics = ConvertTo<NJobTrackerClient::TStatistics>(Statistics_);
        FillTrafficStatistics(ExecAgentTrafficStatisticsPrefix, deserializedStatistics, TrafficMeter_);
        Statistics_ = ConvertToYsonString(deserializedStatistics);

        auto error = FromProto<TError>(JobResult_->error());

        if (error.IsOK()) {
            SetJobState(EJobState::Completed);
        } else if (IsFatalError(error)) {
            error.Attributes().Set("fatal", true);
            ToProto(JobResult_->mutable_error(), error);
            SetJobState(EJobState::Failed);
        } else {
            auto abortReason = GetAbortReason(*JobResult_);
            if (abortReason) {
                error.Attributes().Set("abort_reason", abortReason);
                ToProto(JobResult_->mutable_error(), error);
                SetJobState(EJobState::Aborted);
            } else {
                SetJobState(EJobState::Failed);
            }
        }

        LOG_INFO(error, "Setting final job state (JobState: %v)", GetState());

        // Release resources.
        GpuSlots_.clear();

        auto resourceDelta = ZeroNodeResources() - ResourceUsage_;
        ResourceUsage_ = ZeroNodeResources();
        ResourcesUpdated_.Fire(resourceDelta);
        PortsReleased_.Fire();

        if (Slot_) {
            try {
                LOG_DEBUG("Clean sandbox (SlotIndex: %v)", Slot_->GetSlotIndex());
                Slot_->CleanSandbox();
            } catch (const std::exception& ex) {
                // Errors during cleanup phase do not affect job outcome.
                LOG_ERROR(ex, "Failed to clean sandbox (SlotIndex: %v)", Slot_->GetSlotIndex());
            }
            Bootstrap_->GetExecSlotManager()->ReleaseSlot(Slot_->GetSlotIndex());
        }

        SetJobPhase(EJobPhase::Finished);

        LOG_INFO("Job finalized (JobState: %v)", GetState());

        Bootstrap_->GetExecSlotManager()->OnJobFinished(GetState());
    }

    // Preparation.
    void PrepareNodeDirectory()
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        auto* schedulerJobSpecExt = JobSpec_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        if (schedulerJobSpecExt->has_input_node_directory()) {
            LOG_INFO("Node directory is provided by scheduler");
            return;
        }

        const auto& nodeDirectory = Bootstrap_->GetNodeDirectory();
        TNodeDirectoryBuilder inputNodeDirectoryBuilder(
            nodeDirectory,
            schedulerJobSpecExt->mutable_input_node_directory());

        for (int attempt = 1;; ++attempt) {
            if (JobPhase_ != EJobPhase::PreparingNodeDirectory) {
                break;
            }

            TNullable<TNodeId> unresolvedNodeId;

            auto validateNodeIds = [&] (
                const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
                const TNodeDirectoryPtr& nodeDirectory,
                TNodeDirectoryBuilder* nodeDirectoryBuilder)
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
                        if (nodeDirectoryBuilder) {
                            nodeDirectoryBuilder->Add(replica);
                        }
                    }
                }
            };

            auto validateTableSpecs = [&] (const ::google::protobuf::RepeatedPtrField<TTableInputSpec>& tableSpecs) {
                for (const auto& tableSpec : tableSpecs) {
                    validateNodeIds(tableSpec.chunk_specs(), nodeDirectory, &inputNodeDirectoryBuilder);
                }
            };

            validateTableSpecs(schedulerJobSpecExt->input_table_specs());
            validateTableSpecs(schedulerJobSpecExt->foreign_input_table_specs());

            // NB: No need to add these descriptors to the input node directory.
            for (const auto& artifact : Artifacts_) {
                validateNodeIds(artifact.Key.chunk_specs(), nodeDirectory, nullptr);
            }

            for (const auto& artifactKey : LayerArtifactKeys_) {
                validateNodeIds(artifactKey.chunk_specs(), nodeDirectory, nullptr);
            }

            if (!unresolvedNodeId) {
                break;
            }

            if (attempt >= Config_->NodeDirectoryPrepareRetryCount) {
                LOG_WARNING("Some node ids were not resolved, skipping corresponding replicas (UnresolvedNodeId: %v)", *unresolvedNodeId);
                break;
            }

            LOG_INFO("Unresolved node id found in job spec; backing off and retrying (NodeId: %v, Attempt: %v)",
                *unresolvedNodeId,
                attempt);
            TDelayedExecutor::WaitForDuration(Config_->NodeDirectoryPrepareBackoffTime);
        }

        LOG_INFO("Node directory is constructed locally");
    }

    TJobProxyConfigPtr CreateConfig()
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        auto proxyConfig = Bootstrap_->BuildJobProxyConfig();
        proxyConfig->BusServer = Slot_->GetBusServerConfig();
        proxyConfig->TmpfsPath = TmpfsPath_;
        proxyConfig->SlotIndex = Slot_->GetSlotIndex();
        if (RootVolume_) {
            proxyConfig->RootPath = RootVolume_->GetPath();
            proxyConfig->Binds = Config_->RootFSBinds;
        }

        for (const auto& slot : GpuSlots_) {
            proxyConfig->GpuDevices.push_back(slot->GetDeviceName());
        }

        return proxyConfig;
    }

    void PrepareSandboxDirectories()
    {
        VERIFY_THREAD_AFFINITY(ControllerThread);

        TUserSandboxOptions options;

        const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        if (schedulerJobSpecExt.has_user_job_spec()) {
            const auto& userJobSpec = schedulerJobSpecExt.user_job_spec();
            if (userJobSpec.has_tmpfs_path()) {
                options.TmpfsSizeLimit = userJobSpec.tmpfs_size();
                options.TmpfsPath = userJobSpec.tmpfs_path();
            }

            if (userJobSpec.has_inode_limit()) {
                options.InodeLimit = userJobSpec.inode_limit();
            }

            if (userJobSpec.has_disk_space_limit()) {
                options.DiskSpaceLimit = userJobSpec.disk_space_limit();
            }
        }

        TmpfsPath_ = WaitFor(Slot_->CreateSandboxDirectories(options))
            .ValueOrThrow();
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

            for (const auto& descriptor : userJobSpec.layers()) {
                LayerArtifactKeys_.push_back(TArtifactKey(descriptor));
            }
        }

        if (schedulerJobSpecExt.has_input_query_spec()) {
            const auto& querySpec = schedulerJobSpecExt.input_query_spec();
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
                    key,
                    nullptr});
            }
        }
    }

    TFuture<std::vector<NDataNode::IChunkPtr>> DownloadArtifacts()
    {
        const auto& chunkCache = Bootstrap_->GetChunkCache();

        std::vector<TFuture<IChunkPtr>> asyncChunks;
        for (const auto& artifact : Artifacts_) {
            LOG_INFO("Downloading user file (FileName: %v, SandboxKind: %v)",
                artifact.Name,
                artifact.SandboxKind);

            const auto& nodeDirectory = Bootstrap_->GetNodeDirectory();
            auto asyncChunk = chunkCache->PrepareArtifact(artifact.Key, nodeDirectory, TrafficMeter_)
                .Apply(BIND([=, fileName = artifact.Name, this_ = MakeStrong(this)] (const TErrorOr<IChunkPtr>& chunkOrError) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(chunkOrError,
                        "Failed to prepare user file %Qv",
                        fileName);

                    const auto& chunk = chunkOrError.Value();
                    LOG_INFO("Artifact chunk ready (FileName: %v, LocationId: %v, ChunkId: %v)",
                        fileName,
                        chunk->GetLocation()->GetId(),
                        chunk->GetId());
                    return chunk;
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
            // Artifact preparation is uncancelable, so we check for an early exit.
            if (JobPhase_ != EJobPhase::PreparingArtifacts) {
                return;
            }

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

        // When all artifacts are prepared we can finally change permission for sandbox which will
        // take away write access from the current user (see slot_location.cpp for details).
        if (schedulerJobSpecExt.has_user_job_spec()) {
            LOG_INFO("Setting sandbox permissions");
            WaitFor(Slot_->FinalizePreparation())
                .ThrowOnError();
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
        auto resultError = FromProto<TError>(jobResult.error());

        if (jobResult.HasExtension(TSchedulerJobResultExt::scheduler_job_result_ext)) {
            const auto& schedulerResultExt = jobResult.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

            if (!resultError.FindMatching(NNet::EErrorCode::ResolveTimedOut) &&
                !resultError.FindMatching(NChunkClient::EErrorCode::BandwidthThrottlingFailed) &&
                schedulerResultExt.failed_chunk_ids_size() > 0)
            {
                return EAbortReason::FailedChunks;
            }
        }

        auto abortReason = resultError.Attributes().Find<EAbortReason>("abort_reason");
        if (abortReason) {
            return abortReason.Get();
        }

        if (AbortJobIfAccountLimitExceeded_ &&
            resultError.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded))
        {
            return EAbortReason::AccountLimitExceeded;
        }

        if (resultError.FindMatching(NExecAgent::EErrorCode::ResourceOverdraft)) {
            return EAbortReason::ResourceOverdraft;
        }

        if (resultError.FindMatching(NExecAgent::EErrorCode::WaitingJobTimeout)) {
            return EAbortReason::WaitingTimeout;
        }

        if (resultError.FindMatching(NExecAgent::EErrorCode::AbortByScheduler) ||
            resultError.FindMatching(NJobProxy::EErrorCode::JobNotPrepared))
        {
            return EAbortReason::Scheduler;
        }

        if (resultError.FindMatching(NChunkClient::EErrorCode::AllTargetNodesFailed) ||
            resultError.FindMatching(NChunkClient::EErrorCode::BandwidthThrottlingFailed) ||
            resultError.FindMatching(NChunkClient::EErrorCode::MasterCommunicationFailed) ||
            resultError.FindMatching(NChunkClient::EErrorCode::MasterNotConnected) ||
            resultError.FindMatching(NExecAgent::EErrorCode::ConfigCreationFailed) ||
            resultError.FindMatching(NExecAgent::EErrorCode::SlotNotFound) ||
            resultError.FindMatching(NExecAgent::EErrorCode::JobEnvironmentDisabled) ||
            resultError.FindMatching(NExecAgent::EErrorCode::ArtifactCopyingFailed) ||
            resultError.FindMatching(NExecAgent::EErrorCode::NodeDirectoryPreparationFailed) ||
            resultError.FindMatching(NExecAgent::EErrorCode::SlotLocationDisabled) ||
            resultError.FindMatching(NExecAgent::EErrorCode::RootVolumePreparationFailed) ||
            resultError.FindMatching(NExecAgent::EErrorCode::NotEnoughDiskSpace) ||
            resultError.FindMatching(NJobProxy::EErrorCode::MemoryCheckFailed) ||
            resultError.FindMatching(NContainers::EErrorCode::FailedToStartContainer) ||
            resultError.FindMatching(EProcessErrorCode::CannotResolveBinary) ||
            resultError.FindMatching(NNet::EErrorCode::ResolveTimedOut))
        {
            return EAbortReason::Other;
        }

        if (auto processError = resultError.FindMatching(EProcessErrorCode::NonZeroExitCode))
        {
            auto exitCode = NExecAgent::EJobProxyExitCode(processError->Attributes().Get<int>("exit_code"));
            if (exitCode == EJobProxyExitCode::HeartbeatFailed ||
                exitCode == EJobProxyExitCode::ResultReportFailed ||
                exitCode == EJobProxyExitCode::ResourcesUpdateFailed ||
                exitCode == EJobProxyExitCode::GetJobSpecFailed ||
                exitCode == EJobProxyExitCode::InvalidSpecVersion ||
                exitCode == EJobProxyExitCode::PortoManagmentFailed)
            {
                return EAbortReason::Other;
            }
            if (exitCode == EJobProxyExitCode::ResourceOverdraft) {
                return EAbortReason::ResourceOverdraft;
            }
        }

        if (Signaled_) {
            return EAbortReason::UserRequest;
        }

        return Null;
    }

    bool IsFatalError(const TError& error)
    {
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
            error.FindMatching(NTableClient::EErrorCode::IncomparableType) ||
            error.FindMatching(NTableClient::EErrorCode::UnhashableType) ||
            error.FindMatching(NTableClient::EErrorCode::CorruptedNameTable) ||
            error.FindMatching(NTableClient::EErrorCode::RowWeightLimitExceeded) ||
            error.FindMatching(NTableClient::EErrorCode::InvalidColumnFilter) ||
            error.FindMatching(NTableClient::EErrorCode::InvalidColumnRenaming) ||
            error.FindMatching(NDataNode::EErrorCode::LayerUnpackingFailed);
    }
};

////////////////////////////////////////////////////////////////////////////////

NJobAgent::IJobPtr CreateUserJob(
    const TJobId& jobId,
    const TOperationId& operationId,
    const TNodeResources& resourceUsage,
    TJobSpec&& jobSpec,
    NCellNode::TBootstrap* bootstrap)
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



