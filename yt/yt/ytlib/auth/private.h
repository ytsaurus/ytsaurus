#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, TvmBridgeLogger, "TvmBridge");
YT_DEFINE_GLOBAL(const NLogging::TLogger, NativeAuthLogger, "NativeAuth");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
