#pragma once

#include "public.h"

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IMulticellThrottler
    : public TRefCounted
{
    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetThrottler(NObjectClient::TCellTag cellTag) = 0;
    virtual void Reconfigure(NConcurrency::TThroughputThrottlerConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMulticellThrottler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
