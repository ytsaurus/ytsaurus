#pragma once

#include "public.h"

#include <yt/core/bus/config.h>

#include <yt/ytlib/api/client.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyConnectionConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::vector<TString> Addresses;
    TDuration PingPeriod;
    TDuration TimestampProviderRpcTimeout;
    TDuration TimestampProviderUpdatePeriod;
    NBus::TTcpBusConfigPtr BusClient;

    TRpcProxyConnectionConfig()
    {
        RegisterParameter("addresses", Addresses)
            .NonEmpty();
        RegisterParameter("ping_period", PingPeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("timestamp_provider_rpc_timeout", TimestampProviderRpcTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("timestamp_provider_update_period", TimestampProviderUpdatePeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("bus_client", BusClient)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyClientConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TRpcProxyClientConfig()
    {
        // This constructor intentionally left blank.
    }
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
