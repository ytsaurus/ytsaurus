#pragma once

#include <core/misc/public.h>

#include <ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;
using NJobTrackerClient::EJobPhase;

struct IJob;
typedef TIntrusivePtr<IJob> IJobPtr;

class TJobController;
typedef TIntrusivePtr<TJobController> TJobTrackerPtr;

class TResourceLimitsConfig;
typedef TIntrusivePtr<TResourceLimitsConfig> TResourceLimitsConfigPtr;

class TJobControllerConfig;
typedef TIntrusivePtr<TJobControllerConfig> TJobControllerConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
