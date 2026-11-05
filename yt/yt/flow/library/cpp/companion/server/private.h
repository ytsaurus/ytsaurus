#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, CompanionServerLogger, "FlowCompanionServer");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
