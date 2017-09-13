#pragma once

#include "public.h"

#include <yt/ytlib/api/config.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration UpdatePeriod;
    TDuration AvailabilityPeriod;
    TDuration BackoffPeriod;

    TDiscoveryServiceConfig()
    {
        RegisterParameter("update_period", UpdatePeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("availability_period", AvailabilityPeriod)
            .Default(TDuration::Seconds(15))
            .GreaterThan(UpdatePeriod);
        RegisterParameter("backoff_period", BackoffPeriod)
            .Default(TDuration::Seconds(60))
            .GreaterThan(AvailabilityPeriod);
    }
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
