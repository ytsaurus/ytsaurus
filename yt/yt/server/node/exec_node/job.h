#pragma once

#include "chunk_cache.h"
#include "controller_agent_connector.h"
#include "gpu_manager.h"
#include "job_info.h"
#include "public.h"
#include "volume_manager.h"

#include <yt/yt/server/node/exec_node/chunk_cache.h>

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/containers/cri/public.h>

#include <yt/yt/server/lib/exec_node/public.h>
#include <yt/yt/server/lib/exec_node/job_report.h>

#include <yt/yt/server/lib/misc/job_report.h>

#include <yt/yt/server/lib/job_agent/public.h>
#include <yt/yt/server/lib/job_agent/structs.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/job_prober_client/public.h>

#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NExecNode
{

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGpuCheckType,
    (Preliminary)
    (Extra)
);

////////////////////////////////////////////////////////////////////////////////

struct TArtifact
{
    ESandboxKind SandboxKind;
    TString Name;
    bool Executable;
    bool BypassArtifactCache;
    bool CopyFile;
    NDataNode::TArtifactKey Key;
    NDataNode::IChunkPtr Chunk;
};

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public NJobAgent::TResourceHolder
{
    struct TNameWithAddress
    {
        TString Name;
        NNet::TIP6Address Address;
    };

public:
    DEFINE_SIGNAL(void(TJobPtr), JobPrepared);
    DEFINE_SIGNAL(void(TJobPtr), JobFinished);

public:
    TJob(
        TJobId jobId,
        TOperationId operationId,
        const NClusterNode::TJobResources& resourceUsage,
        const NClusterNode::TJobResourceAttributes& resourceAttributes,
        NControllerAgent::NProto::TJobSpec&& jobSpec,
        TControllerAgentDescriptor agentDescriptor,
        IBootstrap* bootstrap,
        const TJobCommonConfigPtr& commonConfig);

    ~TJob();

    void Start() noexcept;
    void DoStart(TErrorOr<std::vector<TNameWithAddress>>&& resolvedNodeAddresses);
    bool IsStarted() const;

    void Abort(TError error, bool graceful = false);
    void Fail(std::optional<TError> error);

    void OnJobProxySpawned();

    void PrepareArtifact(
        const TString& artifactName,
        const TString& pipePath);

    void OnArtifactPreparationFailed(
        const TString& artifactName,
        const TString& artifactPath,
        const TError& error);

    void OnArtifactsPrepared();

    void OnJobPrepared();

    void OnResultReceived(NControllerAgent::NProto::TJobResult jobResult);

    TJobId GetId() const noexcept;
    TGuid GetIdAsGuid() const noexcept override;
    NScheduler::TAllocationId GetAllocationId() const;

    TOperationId GetOperationId() const;

    IInvokerPtr GetInvoker() const;

    const TControllerAgentDescriptor& GetControllerAgentDescriptor() const;

    void UpdateControllerAgentDescriptor(TControllerAgentDescriptor agentInfo);

    EJobType GetType() const;

    const NControllerAgent::NProto::TJobSpec& GetSpec() const;

    const std::vector<int>& GetPorts() const;

    EJobState GetState() const;

    TInstant GetCreationTime() const;

    NJobAgent::TTimeStatistics GetTimeStatistics() const;

    std::optional<TInstant> GetStartTime() const;

    EJobPhase GetPhase() const;

    int GetSlotIndex() const;

    NClusterNode::TJobResources GetResourceUsage() const;
    bool IsGpuRequested() const;

    const TError& GetJobError() const;
    NControllerAgent::NProto::TJobResult GetResult() const;

    double GetProgress() const;

    void SetResourceUsage(const NClusterNode::TJobResources& newUsage);

    bool ResourceUsageOverdrafted() const;

    void SetProgress(double progress);

    i64 GetStderrSize() const;

    void SetStderrSize(i64 value);

    void SetStderr(const TString& value);

    void SetFailContext(const TString& value);

    void AddProfile(NJobAgent::TJobProfile value);

    void SetCoreInfos(NControllerAgent::TCoreInfos value);

    const NJobAgent::TChunkCacheStatistics& GetChunkCacheStatistics() const;

    NYson::TYsonString GetStatistics() const;
    NChunkClient::NProto::TDataStatistics GetTotalInputDataStatistics() const;
    std::vector<NChunkClient::NProto::TDataStatistics> GetOutputDataStatistics() const;

    TInstant GetStatisticsLastSendTime() const;

    void ResetStatisticsLastSendTime();

    void SetStatistics(const NYson::TYsonString& statisticsYson);
    void SetTotalInputDataStatistics(NChunkClient::NProto::TDataStatistics dataStatistics);
    void SetOutputDataStatistics(std::vector<NChunkClient::NProto::TDataStatistics> dataStatistics);

    TBriefJobInfo GetBriefInfo() const;

    std::vector<NChunkClient::TChunkId> DumpInputContext(NTransactionClient::TTransactionId transactionId);

    std::optional<TString> GetStderr();

    std::optional<TString> GetFailContext();

    const NControllerAgent::TCoreInfos& GetCoreInfos();

    NApi::TPollJobShellResponse PollJobShell(
        const NJobProberClient::TJobShellDescriptor& jobShellDescriptor,
        const NYson::TYsonString& parameters);

    void HandleJobReport(NExecNode::TNodeJobReport&& jobReport);

    void ReportSpec();

    void ReportStderr();

    void ReportFailContext();

    void ReportProfile();

    void DoInterrupt(
        TDuration timeout,
        NScheduler::EInterruptReason interruptionReason,
        const std::optional<TString>& preemptionReason,
        const std::optional<NScheduler::TPreemptedFor>& preemptedFor);

    void DoFail(std::optional<TError> error);

    void RequestGracefulAbort(TError error);
    void DoRequestGracefulAbort(TError error);

    bool GetStored() const;

    void SetStored();

    bool IsGrowingStale(TDuration maxDelay) const;

    bool IsJobProxyCompleted() const noexcept;

    bool IsInterruptible() const noexcept;

    void OnJobInterruptionTimeout(
        NScheduler::EInterruptReason interruptionReason,
        const std::optional<TString>& preemptionReason);

    TControllerAgentConnectorPool::TControllerAgentConnectorPtr GetControllerAgentConnector() const noexcept;

    void Interrupt(
        TDuration timeout,
        NScheduler::EInterruptReason interruptionReason,
        const std::optional<TString>& preemptionReason,
        const std::optional<NScheduler::TPreemptedFor>& preemptedFor);

    NScheduler::EInterruptReason GetInterruptionReason() const noexcept;
    bool IsInterrupted() const noexcept;
    const std::optional<NScheduler::TPreemptedFor>& GetPreemptedFor() const noexcept;

    bool IsFinished() const noexcept;

    TFuture<void> GetCleanupFinishedEvent();

private:
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    const TJobId Id_;
    const TOperationId OperationId_;
    IBootstrap* const Bootstrap_;

    TControllerAgentDescriptor ControllerAgentDescriptor_;
    TWeakPtr<TControllerAgentConnectorPool::TControllerAgentConnector> ControllerAgentConnector_;

    const TJobCommonConfigPtr CommonConfig_;
    const IInvokerPtr Invoker_;
    const TInstant CreationTime_;
    const NChunkClient::TTrafficMeterPtr TrafficMeter_;

    NControllerAgent::NProto::TJobSpec JobSpec_;
    const NControllerAgent::NProto::TJobSpecExt* const JobSpecExt_;
    const NControllerAgent::NProto::TUserJobSpec* const UserJobSpec_;
    const NJobProxy::TJobTestingOptionsPtr JobTestingOptions_;

    const bool Interruptible_;
    const bool AbortJobIfAccountLimitExceeded_;

    THashMap<TString, TUserJobSensorPtr> SupportedMonitoringSensors_;

    // Used to terminate artifacts downloading in case of cancelation.
    TFuture<void> ArtifactsFuture_ = VoidFuture;
    TFuture<void> WorkspaceBuildingFuture_ = VoidFuture;

    double Progress_ = 0.0;
    i64 StderrSize_ = 0;

    std::optional<TString> Stderr_;
    std::optional<TString> FailContext_;
    std::vector<NJobAgent::TJobProfile> Profiles_;
    NControllerAgent::TCoreInfos CoreInfos_;

    bool InterruptionRequested_ = false;
    NConcurrency::TDelayedExecutorCookie InterruptionTimeoutCookie_;
    TInstant InterruptionDeadline_;

    NYson::TYsonString StatisticsYson_ = NYson::TYsonString(TStringBuf("{}"));

    using TGpuStatisticsWithUpdateTime = std::pair<TGpuStatistics, std::optional<TInstant>>;
    std::vector<TGpuStatisticsWithUpdateTime> GpuStatistics_;
    NChunkClient::NProto::TDataStatistics TotalInputDataStatistics_;
    std::vector<NChunkClient::NProto::TDataStatistics> OutputDataStatistics_;
    TInstant StatisticsLastSendTime_ = TInstant::Now();

    NProfiling::TBufferedProducerPtr UserJobSensorProducer_;

    TExecAttributes ExecAttributes_;

    std::optional<TError> Error_;
    std::optional<NControllerAgent::NProto::TJobResultExt> JobResultExtension_;

    std::optional<TInstant> PrepareStartTime_;
    std::optional<TInstant> CopyFinishTime_;
    std::optional<TInstant> StartTime_;
    std::optional<TInstant> ExecStartTime_;
    std::optional<TInstant> FinishTime_;

    std::optional<TInstant> StartPrepareVolumeTime_;
    std::optional<TInstant> FinishPrepareVolumeTime_;

    std::optional<TInstant> PreliminaryGpuCheckStartTime_;
    std::optional<TInstant> PreliminaryGpuCheckFinishTime_;

    std::optional<TInstant> ExtraGpuCheckStartTime_;
    std::optional<TInstant> ExtraGpuCheckFinishTime_;

    i64 MaxDiskUsage_ = 0;

    int SetupCommandCount_ = 0;

    std::optional<ui32> NetworkProjectId_;
    std::vector<TString> TmpfsPaths_;

    std::vector<TArtifact> Artifacts_;
    std::vector<NDataNode::TArtifactKey> LayerArtifactKeys_;
    std::optional<TString> DockerImage_;

    //! Artifact name -> index of the artifact in #Artifacts_ list.
    THashMap<TString, int> UserArtifactNameToIndex_;

    IVolumePtr RootVolume_;

    bool IsGpuRequested_;
    double RequestedCpu_;
    i64 RequestedMemory_;

    EJobState JobState_ = EJobState::Waiting;
    EJobPhase JobPhase_ = EJobPhase::Created;

    TJobEvents JobEvents_;

    NScheduler::EInterruptReason InterruptionReason_ = NScheduler::EInterruptReason::None;
    std::optional<NScheduler::TPreemptedFor> PreemptedFor_;

    //! True if agent asked to store this job.
    bool Stored_ = false;
    TInstant LastStoredTime_;

    TPromise<void> CleanupFinished_ = NewPromise<void>();

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, JobProbeLock_);
    NJobProxy::IJobProbePtr JobProbe_;

    std::vector<TNameWithAddress> ResolvedNodeAddresses_;

    // Artifact statistics.
    NJobAgent::TChunkCacheStatistics ChunkCacheStatistics_;

    std::vector<TFuture<void>> ArtifactPrepareFutures_;

    bool JobProxyCompleted_ = false;

    bool Started_ = false;

    // IO statistics.
    i64 BytesRead_ = 0;
    i64 BytesWritten_ = 0;
    i64 IORequestsRead_ = 0;
    i64 IORequestsWritten_ = 0;

    // Tracing.
    NTracing::TTraceContextPtr TraceContext_;
    NTracing::TTraceContextFinishGuard FinishGuard_;

    // Helpers.

    template <class... U>
    void AddJobEvent(U&&... u)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        JobEvents_.emplace_back(std::forward<U>(u)...);
        HandleJobReport(MakeDefaultJobReport()
            .Events(JobEvents_));
    }

    void SetJobState(EJobState state);

    void SetJobPhase(EJobPhase phase);

    void ValidateJobRunning() const;

    void StartUserJobMonitoring();

    void ReportJobInterruptionInfo(
        TDuration timeout,
        NScheduler::EInterruptReason interruptionReason,
        const std::optional<TString>& preemptionReason,
        const std::optional<NScheduler::TPreemptedFor>& preemptedFor);

    void DoSetResult(TError error);

    void DoSetResult(
        TError error,
        std::optional<NControllerAgent::NProto::TJobResultExt> jobResultExtension,
        bool receivedFromJobProxy);

    bool HandleFinishingPhase();

    void ValidateJobPhase(EJobPhase expectedPhase) const;

    // Event handlers.
    void OnNodeDirectoryPrepared(TErrorOr<std::unique_ptr<NNodeTrackerClient::NProto::TNodeDirectory>>&& protoNodeDirectoryOrError);

    void OnArtifactsDownloaded(const TErrorOr<std::vector<NDataNode::IChunkPtr>>& errorOrArtifacts);

    void OnSandboxDirectoriesPrepared(const TError& error);

    void OnVolumePrepared(const TErrorOr<IVolumePtr>& volumeOrError);

    void OnSetupCommandsFinished(const TError& error);

    std::vector<NContainers::TDevice> GetGpuDevices();

    bool IsFullHostGpuJob() const;

    void RunWithWorkspaceBuilder();

    IUserSlotPtr GetUserSlot() const;

    TFuture<void> RunGpuCheckCommand(
        const TString& gpuCheckBinaryPath,
        std::vector<TString> gpuCheckBinaryArgs,
        EGpuCheckType gpuCheckType);

    void OnGpuCheckCommandFinished(const TError& error);

    void OnExtraGpuCheckCommandFinished(const TError& error);

    void RunJobProxy();

    void OnJobProxyPreparationTimeout();

    void OnJobPreparationTimeout(TDuration prepareTimeLimit, bool fatal);

    void OnWaitingForCleanupTimeout();

    void OnJobProxyFinished(const TError& error);

    template <class TSourceTag, class TCallback>
    void GuardedAction(const TSourceTag& sourceTag, const TCallback& action);

    void OnWorkspacePreparationFinished(const TErrorOr<TJobWorkspaceBuildingResult>& resultOrError);

    // Stop job proxy and Porto containers.
    TFuture<void> StopJobProxy();

    // Finalization.
    void Cleanup();
    void CleanupNbdExports();

    // Preparation.
    std::unique_ptr<NNodeTrackerClient::NProto::TNodeDirectory> PrepareNodeDirectory();

    NJobProxy::TJobProxyInternalConfigPtr CreateConfig();
    std::vector<NJobProxy::TBindConfigPtr> GetRootFsBinds();

    void PrepareSandboxDirectories();

    // Build artifacts.
    void InitializeArtifacts();

    TArtifactDownloadOptions MakeArtifactDownloadOptions() const;

    // Start async artifacts download.
    TFuture<std::vector<NDataNode::IChunkPtr>> DownloadArtifacts();

    TFuture<void> RunSetupCommands();

    // Analyse results.
    static TError BuildJobProxyError(const TError& spawnError);

    NContainers::NCri::TCriAuthConfigPtr BuildDockerAuthConfig();

    TUserSandboxOptions BuildUserSandboxOptions();

    std::optional<NScheduler::EAbortReason> DeduceAbortReason();

    bool IsFatalError(const TError& error);

    void EnrichStatisticsWithGpuInfo(TStatistics* statistics);
    void EnrichStatisticsWithRdmaDeviceInfo(TStatistics* statistics);
    void EnrichStatisticsWithDiskInfo(TStatistics* statistics);
    void EnrichStatisticsWithArtifactsInfo(TStatistics* statistics);

    void UpdateIOStatistics(const TStatistics& statistics);

    void UpdateArtifactStatistics(i64 compressedDataSize, bool cacheHit);

    std::vector<TShellCommandConfigPtr> GetSetupCommands();

    NContainers::TRootFS MakeWritableRootFS();

    TNodeJobReport MakeDefaultJobReport();

    void InitializeJobProbe();

    void ResetJobProbe();

    NJobProxy::IJobProbePtr GetJobProbeOrThrow();

    static bool ShouldCleanSandboxes();

    bool NeedGpuLayers();

    bool NeedGpu();

    void ProfileSensor(const TUserJobSensorPtr& sensor, NProfiling::ISensorWriter* writer, double value);
    void ProfileSensor(const TString& sensorName, NProfiling::ISensorWriter* writer, double value);

    void CollectSensorsFromStatistics(NProfiling::ISensorWriter* writer);
    void CollectSensorsFromGpuAndRdmaDeviceInfo(NProfiling::ISensorWriter* writer);

    TFuture<TSharedRef> DumpSensors();

    void OnResourcesAcquired() noexcept override;

    void Terminate(EJobState finalState, TError error);

    bool Finalize(
        std::optional<EJobState> finalJobState,
        TError error,
        std::optional<NControllerAgent::NProto::TJobResultExt> jobResultExtension,
        bool byJobProxyCompletion);
    void Finalize(TError error);
    void Finalize(EJobState finalState, TError error);

    void OnJobFinalized();

    void DeduceAndSetFinishedJobState();

    bool NeedsGpuCheck() const;
};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

TJobPtr CreateJob(
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::TOperationId operationId,
    const NClusterNode::TJobResources& resourceUsage,
    const NClusterNode::TJobResourceAttributes& resourceAttributes,
    NControllerAgent::NProto::TJobSpec&& jobSpec,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap,
    const TJobCommonConfigPtr& commonConfig);

////////////////////////////////////////////////////////////////////////////////

template <class TStatus>
void FillJobStatus(TStatus* status, const TJobPtr& schedulerJob);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
