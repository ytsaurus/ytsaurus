#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/server/node/job_agent/public.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

NJobAgent::IJobPtr CreateChunkJob(
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    TDataNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

