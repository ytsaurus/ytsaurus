#pragma once

#include <yt/yt/core/rpc/public.h>

namespace NYT::NAdmin {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration PingNodeDefaultTimeout = TDuration::Seconds(10);

constexpr int PingNodeChainMax = 10;

constexpr int PingNodePayloadMax = 10_MB;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAdmin
