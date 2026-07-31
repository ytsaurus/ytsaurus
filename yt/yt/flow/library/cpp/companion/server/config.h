#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/companion/config.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Parses the worker-provided YT_FLOW_COMPANION_CONFIG environment variable
//! (text YSON of #TCompanionExecutionConfig) and validates the companion
//! startup contract; throws with an actionable message on violation.
NCompanion::TCompanionExecutionConfigPtr LoadCompanionExecutionConfigFromEnv();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
