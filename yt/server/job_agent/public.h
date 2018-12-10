#pragma once

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

} // namespace NProto

using NJobTrackerClient::TJobId;
using NJobTrackerClient::TOperationId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;
using NJobTrackerClient::EJobPhase;

DECLARE_REFCOUNTED_STRUCT(IJob)

DECLARE_REFCOUNTED_CLASS(TGpuManager)
DECLARE_REFCOUNTED_CLASS(TJobController)
DECLARE_REFCOUNTED_CLASS(TStatisticsReporter)
DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TJobControllerConfig)
DECLARE_REFCOUNTED_CLASS(TStatisticsReporterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
