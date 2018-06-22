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
    TDuration DefaultPingPeriod;
    NBus::TTcpBusConfigPtr BusClient;
    NHttp::TClientConfigPtr HttpClient;
    bool SendLegacyUserIP;
    bool DiscoverProxiesFromCypress;

    TConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
