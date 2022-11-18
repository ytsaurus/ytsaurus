#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger TvmBridgeLogger("TvmBridge");
inline const NLogging::TLogger NativeAuthLogger("NativeAuth");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
