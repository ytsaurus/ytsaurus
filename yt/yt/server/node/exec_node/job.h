#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TControllerAgentDescriptor
{
    NRpc::TAddressWithNetwork Address;
    NScheduler::TIncarnationId IncarnationId;

    bool operator==(const TControllerAgentDescriptor& other) const
    {
        return other.Address == Address && other.IncarnationId == IncarnationId;
    }

    bool Empty() const noexcept
    {
        return *this == TControllerAgentDescriptor{};
    }

    operator bool() const noexcept
    {
        return !Empty();
    }
};

////////////////////////////////////////////////////////////////////////////////

NJobAgent::IJobPtr CreateSchedulerJob(
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::TOperationId operationId,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    IBootstrap* bootstrap,
    TControllerAgentDescriptor agentDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

template <>
struct THash<NYT::NExecNode::TControllerAgentDescriptor>
{
    size_t operator () (const NYT::NExecNode::TControllerAgentDescriptor& descriptor) const
    {
        size_t hash = THash<decltype(descriptor.Address)>{}(descriptor.Address);
        NYT::HashCombine(hash, descriptor.IncarnationId);

        return hash;
    }
};
