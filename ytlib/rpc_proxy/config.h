#pragma once

#include "public.h"

#include <yt/core/bus/config.h>

#include <yt/ytlib/api/client.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TString Domain;
    std::vector<TString> Addresses;
    TDuration PingPeriod;
    TDuration ProxyListUpdatePeriod;
    int MaxProxyListUpdateAttempts;
    TDuration RpcTimeout;
    TDuration TimestampProviderUpdatePeriod;
    TDuration DefaultTransactionTimeout;
    TDuration DefaultPingPeriod;
    NBus::TTcpBusConfigPtr BusClient;

    TConnectionConfig()
    {
        RegisterParameter("domain", Domain)
            .Default("yt.yandex-team.ru");
        RegisterParameter("addresses", Addresses)
            .NonEmpty();
        RegisterParameter("ping_period", PingPeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("proxy_list_update_period", ProxyListUpdatePeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("max_proxy_list_update_attempts", MaxProxyListUpdateAttempts)
            .Default(7);
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("timestamp_provider_update_period", TimestampProviderUpdatePeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("default_transaction_timeout", DefaultTransactionTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("default_ping_period", DefaultPingPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("bus_client", BusClient)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
