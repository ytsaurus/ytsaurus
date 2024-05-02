#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger SolomonLogger("Solomon");

inline static const TString IsSolomonPullHeaderName = "X-YT-IsSolomonPull";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
