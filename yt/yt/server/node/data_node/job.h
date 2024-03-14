#pragma once

#include "job_info.h"
#include "public.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/server/lib/chunk_server/proto/job_common.pb.h>

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
        NChunkServer::TJobId jobId,
        const NChunkServer::NProto::TJobSpec& jobSpec,
        TString jobTrackerAddress,
        const NClusterNode::TJobResources& resourceLimits,
        IBootstrap* bootstrap);

    NChunkServer::TJobId GetId() const noexcept;
    TGuid GetIdAsGuid() const noexcept override;
    NJobAgent::EJobType GetType() const;
    bool IsUrgent() const;
    const TString& GetJobTrackerAddress() const;

    bool IsStarted() const noexcept;
    NJobAgent::EJobState GetState() const;
    TInstant GetStartTime() const;
    NClusterNode::TJobResources GetResourceUsage() const;
    NChunkServer::NProto::TJobResult GetResult() const;
    TBriefJobInfo GetBriefInfo() const;

    void Start();
    void Abort(const TError& error);

protected:
    IBootstrap* const Bootstrap_;
    const TDataNodeConfigPtr Config_;

    const NChunkServer::TJobId JobId_;
    const NChunkServer::NProto::TJobSpec JobSpec_;
    const TString JobTrackerAddress_;

    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    const ITypedNodeMemoryTrackerPtr MemoryUsageTracker_;

    const TInstant StartTime_;
    bool Started_ = false;
    NJobAgent::EJobState JobState_ = NJobAgent::EJobState::Waiting;

    TFuture<void> JobFuture_;
    NChunkServer::NProto::TJobResult Result_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    virtual TFuture<void> DoRun() = 0;
    TFuture<void> GuardedRun();

    void SetCompleted();
    void SetFailed(const TError& error);
    void SetAborted(const TError& error);

    virtual TFuture<void> ReleaseCumulativeResources();

    IChunkPtr FindLocalChunk(TChunkId chunkId, int mediumIndex);
    IChunkPtr GetLocalChunkOrThrow(TChunkId chunkId, int mediumIndex);

private:
    void DoSetFinished(NJobAgent::EJobState finalState, const TError& error);

    void OnResourcesAcquired() noexcept override;
};

DEFINE_REFCOUNTED_TYPE(TMasterJobBase)

////////////////////////////////////////////////////////////////////////////////

TMasterJobBasePtr CreateJob(
    NChunkServer::TJobId jobId,
    NChunkServer::NProto::TJobSpec&& jobSpec,
    TString jobTrackerAddress,
    const NClusterNode::TJobResources& resourceLimits,
    IBootstrap* bootstrap,
    const TMasterJobSensors& sensors);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
