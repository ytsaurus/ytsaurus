#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/job_agent/public.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

NJobAgent::IJobPtr CreateChunkJob(
    const NJobTrackerClient::TJobId& jobId,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

