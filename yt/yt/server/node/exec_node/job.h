#pragma once

#include "private.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TControllerAgentDescriptor
{
    TString Address;
    NScheduler::TIncarnationId IncarnationId;

    bool operator==(const TControllerAgentDescriptor& other) const noexcept;
    bool operator!=(const TControllerAgentDescriptor& other) const noexcept;

    bool Empty() const noexcept;

    operator bool() const noexcept;
};

////////////////////////////////////////////////////////////////////////////////

TJobPtr CreateJob(
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::TOperationId operationId,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    IBootstrap* bootstrap,
    TControllerAgentDescriptor agentDescriptor);

////////////////////////////////////////////////////////////////////////////////

void FillSchedulerJobStatus(NJobTrackerClient::NProto::TJobStatus* jobStatus, const TJobPtr& schedulerJob);

////////////////////////////////////////////////////////////////////////////////

using TJobFactory = TCallback<TJobPtr(
    NJobTrackerClient::TJobId jobid,
    NJobTrackerClient::TOperationId operationId,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    const NExecNode::TControllerAgentDescriptor& agentInfo)>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

template <>
struct THash<NYT::NExecNode::TControllerAgentDescriptor>
{
    size_t operator () (const NYT::NExecNode::TControllerAgentDescriptor& descriptor) const;
};
