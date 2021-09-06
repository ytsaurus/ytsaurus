#pragma once

#include "job.h"
#include "chunk_cache.h"
#include "gpu_manager.h"
#include "public.h"
#include "volume_manager.h"

#include <yt/yt/server/node/exec_node/chunk_cache.h>

#include <yt/yt/server/node/job_agent/job.h>
#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/server/lib/job_agent/job_report.h>
#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NExecNode
{

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGpuCheckType,
    (Preliminary)
    (Extra)
)

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public NJobAgent::IJob
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(const NNodeTrackerClient::NProto::TNodeResources&), ResourcesUpdated);
    DEFINE_SIGNAL_OVERRIDE(void(), PortsReleased);
    DEFINE_SIGNAL_OVERRIDE(void(), JobPrepared);
    DEFINE_SIGNAL_OVERRIDE(void(), JobFinished);

public:
    TJob(
        TJobId jobId,
        TOperationId operationId,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage,
        NJobTrackerClient::NProto::TJobSpec&& jobSpec,
        IBootstrap* bootstrap,
        TControllerAgentDescriptor agentDescriptor);
    
    ~TJob();

    void Start() override;

    void Abort(const TError& error) override;

    void OnJobProxySpawned() override;

    void PrepareArtifact(
        const TString& artifactName,
        const TString& pipePath) override;
    
    void OnArtifactPreparationFailed(
        const TString& artifactName,
        const TString& artifactPath,
        const TError& error) override;
    
    void OnArtifactsPrepared() override;

    void OnJobPrepared() override;

    void SetResult(const NJobTrackerClient::NProto::TJobResult& jobResult) override;

    TJobId GetId() const override;

    TOperationId GetOperationId() const override;

    const TControllerAgentDescriptor& GetControllerAgentDescriptor() const;

    void UpdateControllerAgentDescriptor(TControllerAgentDescriptor agentInfo);

    EJobType GetType() const override;

    const NJobTrackerClient::NProto::TJobSpec& GetSpec() const override;

    int GetPortCount() const override;

    void SetPorts(const std::vector<int>& ports) override;

    EJobState GetState() const override;

    TInstant GetStartTime() const override;

    NJobAgent::TTimeStatistics GetTimeStatistics() const override;

    EJobPhase GetPhase() const override;

    int GetSlotIndex() const override;

    NNodeTrackerClient::NProto::TNodeResources GetResourceUsage() const override;

    std::vector<int> GetPorts() const override;

    NJobTrackerClient::NProto::TJobResult GetResult() const override;

    double GetProgress() const override;

    void SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& newUsage) override;

    void SetProgress(double progress) override;

    i64 GetStderrSize() const override;

    void SetStderrSize(i64 value) override;

    void SetStderr(const TString& value) override;

    void SetFailContext(const TString& value) override;

    void SetProfile(const NJobAgent::TJobProfile& value) override;

    void SetCoreInfos(NCoreDump::TCoreInfos value) override;

    const NJobAgent::TChunkCacheStatistics& GetChunkCacheStatistics() const override;

    NYson::TYsonString GetStatistics() const override;

    TInstant GetStatisticsLastSendTime() const override;

    void ResetStatisticsLastSendTime() override;

    void SetStatistics(const NYson::TYsonString& statisticsYson) override;

    void BuildOrchid(NYTree::TFluentMap fluent) const override;

    std::vector<NChunkClient::TChunkId> DumpInputContext() override;

    TString GetStderr() override;

    std::optional<TString> GetFailContext() override;

    std::optional<NJobAgent::TJobProfile> GetProfile();

    const NCoreDump::TCoreInfos& GetCoreInfos();

    NYson::TYsonString PollJobShell(
        const NJobProberClient::TJobShellDescriptor& jobShellDescriptor,
        const NYson::TYsonString& parameters) override;

    void HandleJobReport(NJobAgent::TNodeJobReport&& jobReport) override;

    void ReportSpec() override;

    void ReportStderr() override;

    void ReportFailContext() override;

    void ReportProfile() override;

    void Interrupt() override;

    void Fail() override;

    bool GetStored() const override;

    void SetStored(bool value) override;

private:
    const TJobId Id_;
    const TOperationId OperationId_;
    IBootstrap* const Bootstrap_;

    TControllerAgentDescriptor ControllerAgentDescriptor_;

    const TExecNodeConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TInstant StartTime_;
    const NChunkClient::TTrafficMeterPtr TrafficMeter_;

    NJobTrackerClient::NProto::TJobSpec JobSpec_;
    const NScheduler::NProto::TSchedulerJobSpecExt* const SchedulerJobSpecExt_;
    const NScheduler::NProto::TUserJobSpec* const UserJobSpec_;

    const bool AbortJobIfAccountLimitExceeded_;

    const NLogging::TLogger Logger;

    // Used to terminate artifacts downloading in case of cancelation.
    TFuture<void> ArtifactsFuture_ = VoidFuture;

    double Progress_ = 0.0;
    i64 StderrSize_ = 0;

    std::optional<TString> Stderr_;
    std::optional<TString> FailContext_;
    std::optional<NJobAgent::TJobProfile> Profile_;
    NCoreDump::TCoreInfos CoreInfos_;

    NYson::TYsonString StatisticsYson_ = NYson::TYsonString(TStringBuf("{}"));
    TInstant StatisticsLastSendTime_ = TInstant::Now();

    NProfiling::TBufferedProducerPtr UserJobSensorProducer_;

    NJobAgent::TExecAttributes ExecAttributes_;

    std::optional<NJobTrackerClient::NProto::TJobResult> JobResult_;

    std::optional<TInstant> PrepareTime_;
    std::optional<TInstant> CopyTime_;
    std::optional<TInstant> StartPrepareVolumeTime_;
    std::optional<TInstant> FinishPrepareVolumeTime_;
    std::optional<TInstant> ExecTime_;
    std::optional<TInstant> FinishTime_;

    std::optional<TInstant> PreliminaryGpuCheckStartTime_;
    std::optional<TInstant> PreliminaryGpuCheckFinishTime_;

    std::optional<TInstant> ExtraGpuCheckStartTime_;
    std::optional<TInstant> ExtraGpuCheckFinishTime_;

    std::vector<TGpuManager::TGpuSlotPtr> GpuSlots_;
    std::vector<TGpuStatistics> GpuStatistics_;

    int SetupCommandsCount_ = 0;

    std::optional<ui32> NetworkProjectId_;

    ISlotPtr Slot_;
    std::vector<TString> TmpfsPaths_;

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

    std::vector<TArtifact> Artifacts_;
    std::vector<NDataNode::TArtifactKey> LayerArtifactKeys_;

    //! Artifact name -> index of the artifact in #Artifacts_ list.
    THashMap<TString, int> UserArtifactNameToIndex_;

    IVolumePtr RootVolume_;

    NNodeTrackerClient::NProto::TNodeResources ResourceUsage_;
    std::vector<int> Ports_;

    EJobState JobState_ = EJobState::Waiting;
    EJobPhase JobPhase_ = EJobPhase::Created;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    NJobAgent::TJobEvents JobEvents_;

    //! True if scheduler asked to store this job.
    bool Stored_ = false;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, JobProbeLock_);
    NJobProberClient::IJobProbePtr JobProbe_;

    std::vector<std::pair<TString, NNet::TIP6Address>> ResolvedNodeAddresses_;

    // Artifact statistics.
    NJobAgent::TChunkCacheStatistics ChunkCacheStatistics_;

    std::vector<TFuture<void>> ArtifactPrepareFutures_;

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

    void SetJobStatePhase(EJobState state, EJobPhase phase);

    void ValidateJobRunning() const;

    void StartUserJobMonitoring();

    void DoSetResult(const TError& error);

    void DoSetResult(NJobTrackerClient::NProto::TJobResult jobResult);

    bool HandleFinishingPhase();

    void ValidateJobPhase(EJobPhase expectedPhase) const;

    // Event handlers.
    void OnNodeDirectoryPrepared(const TError& error);

    void OnArtifactsDownloaded(const TErrorOr<std::vector<NDataNode::IChunkPtr>>& errorOrArtifacts);

    void OnSandboxDirectoriesPrepared(const TError& error);

    void OnVolumePrepared(const TErrorOr<IVolumePtr>& volumeOrError);

    void OnSetupCommandsFinished(const TError& error);

    TFuture<void> RunGpuCheckCommand(const TString& gpuCheckBinaryPath, EGpuCheckType gpuCheckType);

    void OnGpuCheckCommandFinished(const TError& error);

    void OnExtraGpuCheckCommandFinished(const TError& error);

    void RunJobProxy();

    void OnJobProxyPreparationTimeout();

    void OnJobPreparationTimeout(TDuration prepareTimeLimit);

    void OnJobAbortionTimeout();

    void OnJobProxyFinished(const TError& error);

    void GuardedAction(std::function<void()> action);

    // Finalization.
    void Cleanup();

    // Preparation.
    void PrepareNodeDirectory();

    NJobProxy::TJobProxyConfigPtr CreateConfig();

    void PrepareSandboxDirectories();

    // Build artifacts.
    void InitializeArtifacts();

    TArtifactDownloadOptions MakeArtifactDownloadOptions() const;

    // Start async artifacts download.
    TFuture<std::vector<NDataNode::IChunkPtr>> DownloadArtifacts();

    TFuture<void> RunSetupCommands();

    // Analyse results.
    static TError BuildJobProxyError(const TError& spawnError);

    std::optional<NScheduler::EAbortReason> GetAbortReason(const NJobTrackerClient::NProto::TJobResult& jobResult);

    bool IsFatalError(const TError& error);

    void EnrichStatisticsWithGpuInfo(TStatistics* statistics);

    void EnrichStatisticsWithArtifactsInfo(TStatistics* statistics);

    void UpdateArtifactStatistics(i64 compressedDataSize, bool cacheHit);

    std::vector<NJobAgent::TShellCommandConfigPtr> GetSetupCommands();

    NContainers::TRootFS MakeWritableRootFS();

    NJobAgent::TNodeJobReport MakeDefaultJobReport();

    void InitializeJobProbe();

    void ResetJobProbe();

    NJobProberClient::IJobProbePtr GetJobProbeOrThrow();

    static bool ShouldCleanSandboxes();

    bool NeedGpuLayers();

    bool NeedGpu();

    bool IsSensorFromStatistics(const TString& sensorName);

    void CollectSensorsFromStatistics(NProfiling::ISensorWriter* writer);

    void CollectSensorsFromGpuInfo(NProfiling::ISensorWriter* writer);

    TFuture<TSharedRef> DumpSensors();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
