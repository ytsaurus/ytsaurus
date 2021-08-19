#pragma once

#include "public.h"

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NApi::TConnectionConfig
{
public:
    std::optional<TString> ClusterUrl;
    std::optional<TString> ProxyRole;
    std::optional<std::vector<TString>> ProxyAddresses;
    NRpc::TServiceDiscoveryEndpointsConfigPtr ProxyEndpoints;
    std::vector<NRe2::TRe2Ptr> ProxyHostOrder;

    NRpc::TDynamicChannelPoolConfigPtr DynamicChannelPool;

    TDuration PingPeriod;
    TDuration ProxyListUpdatePeriod;
    TDuration ProxyListRetryPeriod;
    TDuration MaxProxyListRetryPeriod;
    int MaxProxyListUpdateAttempts;

    TDuration RpcTimeout;
    std::optional<TDuration> RpcAcknowledgementTimeout;

    TDuration TimestampProviderLatestTimestampUpdatePeriod;

    TDuration DefaultTransactionTimeout;
    TDuration DefaultSelectRowsTimeout;
    TDuration DefaultTotalStreamingTimeout;
    TDuration DefaultStreamingStallTimeout;
    TDuration DefaultPingPeriod;

    NBus::TTcpBusConfigPtr BusClient;
    TDuration IdleChannelTtl;

    NHttp::TClientConfigPtr HttpClient;

    NCompression::ECodec RequestCodec;
    NCompression::ECodec ResponseCodec;

    bool EnableLegacyRpcCodecs;

    bool EnableRetries;
    NRpc::TRetryingChannelConfigPtr RetryingChannel;

    i64 ModifyRowsBatchCapacity;

    // COMPAT(akozhikhov): drop this later.
    bool EnableMultiLookup;

    TConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
