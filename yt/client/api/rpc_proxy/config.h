#pragma once

#include "public.h"

#include <yt/core/bus/tcp/config.h>

#include <yt/core/http/config.h>

#include <yt/client/api/client.h>
#include <yt/client/api/config.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NApi::TConnectionConfig
{
public:
    TNullable<TString> ClusterUrl;
    TNullable<TString> ProxyRole;
    std::vector<TString> Addresses;
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
    bool EnableStickyTransactionPool;
    bool EnableProxyDiscovery;

    TConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
