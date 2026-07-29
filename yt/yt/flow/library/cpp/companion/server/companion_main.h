#pragma once

#include "public.h"

#include "pipeline.h"

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! The companion binary entry point (a #TProgram): reads YT_FLOW_COMPANION_CONFIG
//! and serves the registered pipeline; the standard signal handlers terminate the
//! process on SIGTERM. Never returns; |int| is for symmetry with |main|.
int RunCompanionMain(int argc, const char** argv, TPipeline pipeline);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
