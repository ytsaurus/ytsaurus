#pragma once

#include <yt/yt/core/bus/public.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMessageDirection,
    (Incoming)
    (Outcoming)
);

struct TBusNetworkCounters;
using TBusNetworkCountersPtr = TIntrusivePtr<TBusNetworkCounters>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

