#pragma once

#include "public.h"

#include <yt/core/bus/tcp/config.h>

#include <yt/core/http/config.h>

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/config.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NApi::TConnectionConfig
{
public:
    TString Domain;
    TNullable<TString> ClusterUrl;
    std::vector<TString> Addresses;
    TDuration PingPeriod;
    TDuration ProxyListUpdatePeriod;
    int MaxProxyListUpdateAttempts;
    TDuration RpcTimeout;
    TDuration TimestampProviderUpdatePeriod;
    TDuration DefaultTransactionTimeout;
    TDuration DefaultPingPeriod;
    NBus::TTcpBusConfigPtr BusClient;
    NHttp::TClientConfigPtr HttpClient;
    bool SendLegacyUserIP;

    TConnectionConfig()
    {
        RegisterParameter("domain", Domain)
            .Default("yt.yandex-team.ru");
        RegisterParameter("cluster_url", ClusterUrl)
            .Default();
        RegisterParameter("addresses", Addresses)
            .Default();
        RegisterPostprocessor([this] {
            if (!ClusterUrl && Addresses.empty()) {
                THROW_ERROR_EXCEPTION("Either \"cluster_url\" or \"addresses\" must be specified");
            }
        });
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
        RegisterParameter("http_client", HttpClient)
            .DefaultNew();
        // COMPAT(prime)
        RegisterParameter("send_legacy_user_ip", SendLegacyUserIP)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
