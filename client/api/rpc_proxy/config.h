#pragma once

#include "public.h"

#include <yt/core/bus/tcp/config.h>

#include <yt/core/http/config.h>

#include <yt/client/api/client.h>
#include <yt/client/api/config.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NApi::TConnectionConfig
{
public:
    std::optional<TString> ClusterUrl;
    std::optional<TString> ProxyRole;
    std::vector<TString> Addresses;
    //! Number of open connection to rpc proxies.
    int ChannelPoolSize;
    TDuration ChannelPoolRebalanceInterval;

    TDuration PingPeriod;
    TDuration ProxyListUpdatePeriod;
    TDuration ProxyListRetryPeriod;
    TDuration MaxProxyListRetryPeriod;
    int MaxProxyListUpdateAttempts;
    TDuration RpcTimeout;
    TDuration TimestampProviderUpdatePeriod;
    TDuration DefaultTransactionTimeout;
    TDuration DefaultSelectRowsTimeout;
    TDuration DefaultPingPeriod;
    NBus::TTcpBusConfigPtr BusClient;
    NHttp::TClientConfigPtr HttpClient;
    bool EnableProxyDiscovery;

    TConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
