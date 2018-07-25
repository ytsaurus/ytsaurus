#pragma once

#include "public.h"

#include <yt/client/api/config.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TApiServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    bool VerboseLogging;

    TApiServiceConfig()
    {
        RegisterParameter("verbose_logging", VerboseLogging)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TApiServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration LivenessUpdatePeriod;
    TDuration ProxyUpdatePeriod;
    TDuration AvailabilityPeriod;
    TDuration BackoffPeriod;

    TDiscoveryServiceConfig()
    {
        RegisterParameter("liveness_update_period", LivenessUpdatePeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("proxy_update_period", ProxyUpdatePeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("availability_period", AvailabilityPeriod)
            .Default(TDuration::Seconds(15))
            .GreaterThan(LivenessUpdatePeriod);
        RegisterParameter("backoff_period", BackoffPeriod)
            .Default(TDuration::Seconds(60))
            .GreaterThan(AvailabilityPeriod);
    }
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
