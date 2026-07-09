#pragma once

#include <yt/yt/core/logging/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Prints the slow-build warning to stderr when the current process runs a slow build.
//! Called from Initialize(), before logging is configured — stderr only.
//! Suppressed by `YT_FLOW_SUPPRESS_DEBUG_BUILD_WARNING=1` in the environment.
void MaybeWarnSlowBuild();

//! Re-emits the same warning into |logger| so it lands in the configured log files.
//! Call after the log manager has been configured. Honors the same suppression env-var.
void MaybeLogSlowBuild(const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
