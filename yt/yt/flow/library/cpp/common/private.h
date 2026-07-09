#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, FlowStateLogger, "FlowState");
YT_DEFINE_GLOBAL(const NLogging::TLogger, PersistedStateLogger, "PersistedState");
YT_DEFINE_GLOBAL(const NLogging::TLogger, FlowAuthenticatorLogger, "FlowAuthenticator");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
