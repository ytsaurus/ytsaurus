#pragma once

#include <yt/yt/core/bus/public.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

struct TBusNetworkCounters;
using TBusNetworkCountersPtr = TIntrusivePtr<TBusNetworkCounters>;

DECLARE_REFCOUNTED_CLASS(TMultiplexingBandConfig)
DECLARE_REFCOUNTED_CLASS(TTcpDispatcherConfig)
DECLARE_REFCOUNTED_CLASS(TTcpDispatcherDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TTcpBusConfig)
DECLARE_REFCOUNTED_CLASS(TTcpBusServerConfig)
DECLARE_REFCOUNTED_CLASS(TTcpBusClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

