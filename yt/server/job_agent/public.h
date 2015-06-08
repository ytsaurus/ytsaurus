#pragma once

#include <core/misc/public.h>

#include <ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

} // namespace NProto

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;
using NJobTrackerClient::EJobPhase;

DECLARE_REFCOUNTED_STRUCT(IJob)

DECLARE_REFCOUNTED_CLASS(TJobController)
DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TJobControllerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
