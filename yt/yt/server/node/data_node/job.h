#pragma once

#include "job_info.h"
#include "public.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TMasterJobSensors
{
    NProfiling::TCounter AdaptivelyRepairedChunksCounter;
    NProfiling::TCounter TotalRepairedChunksCounter;
    NProfiling::TCounter FailedRepairChunksCounter;
};

////////////////////////////////////////////////////////////////////////////////

class TMasterJobBase
    : public NJobAgent::TResourceHolder
{
public:
    DEFINE_SIGNAL(void(const NClusterNode::TJobResources& resourcesDelta), ResourcesUpdated);
    DEFINE_SIGNAL(void(), JobPrepared);
    DEFINE_SIGNAL(void(), JobFinished);

public:
    TMasterJobBase(
        NJobTrackerClient::TJobId jobId,
        const NJobTrackerClient::NProto::TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const NClusterNode::TJobResources& resourceLimits,
        const NClusterNode::TJobResourceAttributes& resourceAttributes,
        IBootstrap* bootstrap);

    void Start();

    bool IsStarted() const noexcept;

    void Abort(const TError& error);

    NJobTrackerClient::TJobId GetId() const noexcept override;

    NJobAgent::EJobType GetType() const;

    bool IsUrgent() const;

    const TString& GetJobTrackerAddress() const;

    NJobAgent::EJobState GetState() const;

    NClusterNode::TJobResources GetResourceUsage() const;

    NJobTrackerClient::NProto::TJobResult GetResult() const;

    TInstant GetStartTime() const;

    TBriefJobInfo GetBriefInfo() const;

protected:
    IBootstrap* const Bootstrap_;
    const TDataNodeConfigPtr Config_;

    const NJobTrackerClient::TJobId JobId_;
    const NJobTrackerClient::NProto::TJobSpec JobSpec_;
    const TString JobTrackerAddress_;

    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const ITypedNodeMemoryTrackerPtr MemoryUsageTracker_;

    const TInstant StartTime_;

    bool Started_ = false;

    NJobAgent::EJobState JobState_ = NJobAgent::EJobState::Waiting;

    TFuture<void> JobFuture_;

    NJobTrackerClient::NProto::TJobResult Result_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    virtual void DoRun() = 0;

    virtual TFuture<void> ReleaseCumulativeResources();

    void GuardedRun();

    void SetCompleted();

    void SetFailed(const TError& error);

    void SetAborted(const TError& error);

    IChunkPtr FindLocalChunk(TChunkId chunkId, int mediumIndex);

    IChunkPtr GetLocalChunkOrThrow(TChunkId chunkId, int mediumIndex);

private:
    void DoSetFinished(NJobAgent::EJobState finalState, const TError& error);

    void OnResourcesAcquired() noexcept override;
};

DEFINE_REFCOUNTED_TYPE(TMasterJobBase)

////////////////////////////////////////////////////////////////////////////////

TMasterJobBasePtr CreateJob(
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    TString jobTrackerAddress,
    const NClusterNode::TJobResources& resourceLimits,
    const NClusterNode::TJobResourceAttributes& resourceAttributes,
    IBootstrap* bootstrap,
    const TMasterJobSensors& sensors);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
