#pragma once

#include <yt/yt/flow/library/cpp/companion/client/public.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

using NCompanion::ECompanionComputationType;
using NCompanion::ECompanionResourceCommand;
using NCompanion::ECompanionResourceExecuteStatus;
using NCompanion::ECompanionResponseStatus;

class TPipeline;

DECLARE_REFCOUNTED_CLASS(TCompanionMonitoring);
DECLARE_REFCOUNTED_CLASS(TCompanionProfiler);
DECLARE_REFCOUNTED_CLASS(TComputationCounters);
DECLARE_REFCOUNTED_CLASS(TCompanionServer);
DECLARE_REFCOUNTED_STRUCT(TCompanionServerContext);
DECLARE_REFCOUNTED_CLASS(TJob);
DECLARE_REFCOUNTED_CLASS(TJobRegistry);
DECLARE_REFCOUNTED_CLASS(TResourceStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
