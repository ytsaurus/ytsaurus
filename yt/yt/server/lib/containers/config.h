#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TPortoExecutorConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration RetriesTimeout;
    TDuration PollPeriod;
    TDuration ApiTimeout;
    TDuration ApiDiskTimeout;
    bool EnableNetworkIsolation;

    TPortoExecutorConfig()
    {
        RegisterParameter("retries_timeout", RetriesTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("poll_period", PollPeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("api_timeout", ApiTimeout)
            .Default(TDuration::Minutes(5));
        RegisterParameter("api_disk_timeout", ApiDiskTimeout)
            .Default(TDuration::Minutes(30));
        RegisterParameter("enable_network_isolation", EnableNetworkIsolation)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TPortoExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
