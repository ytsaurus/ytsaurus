#pragma once

#include "job.h"
#include "public.h"

#include <yt/yt/server/node/job_agent/job.h>

#include <yt/yt/server/lib/core_dump/helpers.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TMasterJobBase
    : public NJobAgent::IJob
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(const NNodeTrackerClient::NProto::TNodeResources& resourcesDelta), ResourcesUpdated);
    DEFINE_SIGNAL_OVERRIDE(void(), PortsReleased);
    DEFINE_SIGNAL_OVERRIDE(void(), JobPrepared);
    DEFINE_SIGNAL_OVERRIDE(void(), JobFinished);

public:
    TMasterJobBase(
        NJobTrackerClient::TJobId jobId,
        const NJobTrackerClient::NProto::TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        IBootstrap* bootstrap);
    
    void Start() override;

    bool IsStarted() const noexcept override;

    void Abort(const TError& error) override;

    NJobTrackerClient::TJobId GetId() const override;

    NJobTrackerClient::TOperationId GetOperationId() const override;

    NJobAgent::EJobType GetType() const override;

    bool IsUrgent() const override;

    const NJobTrackerClient::NProto::TJobSpec& GetSpec() const override;

    const TString& GetJobTrackerAddress() const override;

    int GetPortCount() const override;

    NJobAgent::EJobState GetState() const override;

    NJobAgent::EJobPhase GetPhase() const override;

    int GetSlotIndex() const override;

    NNodeTrackerClient::NProto::TNodeResources GetResourceUsage() const override;

    bool IsGpuRequested() const override;

    std::vector<int> GetPorts() const override;

    void SetPorts(const std::vector<int>&) override;

    void SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& /*newUsage*/) override;

    bool ResourceUsageOverdrafted() const override;

    NJobTrackerClient::NProto::TJobResult GetResult() const override;

    void SetResult(const NJobTrackerClient::NProto::TJobResult& /*result*/) override;

    double GetProgress() const override;

    void SetProgress(double value) override;

    i64 GetStderrSize() const override;

    void SetStderrSize(i64 value) override;

    void SetStderr(const TString& /*value*/) override;

    void SetFailContext(const TString& /*value*/) override;

    void SetProfile(const NJobAgent::TJobProfile& /*value*/) override;

    void SetCoreInfos(NCoreDump::TCoreInfos /*value*/) override;

    const NJobAgent::TChunkCacheStatistics& GetChunkCacheStatistics() const override;

    NYson::TYsonString GetStatistics() const override;

    void SetStatistics(const NYson::TYsonString& /*statistics*/) override;

    void BuildOrchid(NYTree::TFluentMap /*fluent*/) const override;

    TInstant GetStartTime() const override;

    NJobAgent::TTimeStatistics GetTimeStatistics() const override;

    TInstant GetStatisticsLastSendTime() const override;

    void ResetStatisticsLastSendTime() override;

    std::vector<TChunkId> DumpInputContext() override;

    std::optional<TString> GetStderr() override;

    std::optional<TString> GetFailContext() override;

    NApi::TPollJobShellResponse PollJobShell(
        const NJobProberClient::TJobShellDescriptor& /*jobShellDescriptor*/,
        const NYson::TYsonString& /*parameters*/) override;

    void OnJobProxySpawned() override;

    void PrepareArtifact(
        const TString& /*artifactName*/,
        const TString& /*pipePath*/) override;

    void OnArtifactPreparationFailed(
        const TString& /*artifactName*/,
        const TString& /*artifactPath*/,
        const TError& /*error*/) override;

    void OnArtifactsPrepared() override;

    void OnJobPrepared() override;

    void HandleJobReport(NJobAgent::TNodeJobReport&&) override;

    void ReportSpec() override;

    void ReportStderr() override;

    void ReportFailContext() override;

    void ReportProfile() override;

    bool GetStored() const override;

protected:
    const NJobTrackerClient::TJobId JobId_;
    const NJobTrackerClient::NProto::TJobSpec JobSpec_;
    const TString JobTrackerAddress_;
    const TDataNodeConfigPtr Config_;
    const TInstant StartTime_;
    IBootstrap* const Bootstrap_;
    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    NLogging::TLogger Logger;

    NNodeTrackerClient::NProto::TNodeResources ResourceLimits_;

    NJobAgent::EJobState JobState_ = NJobAgent::EJobState::Waiting;
    NJobAgent::EJobPhase JobPhase_ = NJobAgent::EJobPhase::Created;

    double Progress_ = 0.0;
    ui64 JobStderrSize_ = 0;

    TString Stderr_;

    TFuture<void> JobFuture_;

    NJobTrackerClient::NProto::TJobResult Result_;

    bool Started_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    virtual void DoRun() = 0;

    void GuardedRun();

    void SetCompleted();

    void SetFailed(const TError& error);

    void SetAborted(const TError& error);

    IChunkPtr FindLocalChunk(TChunkId chunkId, int mediumIndex);

    IChunkPtr GetLocalChunkOrThrow(TChunkId chunkId, int mediumIndex);

private:
    void DoSetFinished(NJobAgent::EJobState finalState, const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
