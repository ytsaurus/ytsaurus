#pragma once

#include "public.h"

#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TMasterJobSensors
{
    NProfiling::TCounter AdaptivelyRepairedChunksCounter;
    NProfiling::TCounter TotalRepairedChunksCounter;
    NProfiling::TCounter FailedRepairChunksCounter;
};

////////////////////////////////////////////////////////////////////////////////

TMasterJobBasePtr CreateJob(
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec,
    TString jobTrackerAddress,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap,
    const TMasterJobSensors& sensors);

////////////////////////////////////////////////////////////////////////////////

using TJobFactory = TCallback<TMasterJobBasePtr(
    NJobTrackerClient::TJobId jobId,
    const TString& jobTrackerAddress,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec)>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
