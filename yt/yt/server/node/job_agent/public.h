#pragma once

#include <yt/server/lib/job_agent/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::TOperationId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;
using NJobTrackerClient::EJobPhase;

DECLARE_REFCOUNTED_STRUCT(IJob)

DECLARE_REFCOUNTED_CLASS(TGpuManager)
DECLARE_REFCOUNTED_CLASS(TGpuManagerConfig)
DECLARE_REFCOUNTED_CLASS(TMappedMemoryControllerConfig)
DECLARE_REFCOUNTED_CLASS(TJobController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
