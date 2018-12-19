#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/job_agent/public.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

NJobAgent::IJobPtr CreateUserJob(
    NJobTrackerClient::TJobId jobId,
    const NJobTrackerClient::TOperationId& operationId,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
