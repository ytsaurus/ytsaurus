#pragma once

#include "public.h"

#include <ytlib/job_tracker_client/job.pb.h>

#include <ytlib/node_tracker_client/node.pb.h>

#include <server/job_agent/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NJobAgent::IJobPtr CreateRemovalJob(
    const NJobTrackerClient::TJobId& jobId,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

NJobAgent::IJobPtr CreateReplicationJob(
    const NJobTrackerClient::TJobId& jobId,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

