#pragma once

#include "public.h"

#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

NJobAgent::IJobPtr CreateUserJob(
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::TOperationId operationId,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
