#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NQueryClient {

constexpr int MaxExpressionDepth = 50;

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger QueryClientLogger("QueryClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

