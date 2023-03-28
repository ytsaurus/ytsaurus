#include "rpc.h"
#include "options.h"

#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <util/string/strip.h>

#include <util/system/env.h>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

std::pair<TStringBuf, TStringBuf> ExtractClusterAndProxyRole(TStringBuf clusterUrl) {
    TStringBuf cluster;
    TStringBuf proxyRole;
    clusterUrl.Split('/', cluster, proxyRole);
    return {cluster, proxyRole};
}

void SetClusterUrl(TConfig& config, TStringBuf clusterUrl)
{
    auto [cluster, proxyRole] = ExtractClusterAndProxyRole(clusterUrl);
    if (!proxyRole.empty()) {
        Y_ENSURE(config.GetProxyRole().empty(), "ProxyRole specified in both: config and url");
        config.SetProxyRole(ToString(proxyRole));
    }
    config.SetClusterName(ToString(cluster));
}

NYT::NCompression::ECodec GetResponseCodecFromProto(const ECompressionCodec& protoCodec)
{
    switch (protoCodec) {
        case ECompressionCodec::None:
            return NYT::NCompression::ECodec::None;
        case ECompressionCodec::Lz4:
            return NYT::NCompression::ECodec::Lz4;
    }
    Y_UNREACHABLE();
}

NApi::IClientPtr CreateClient(const TConfig& config, const NApi::TClientOptions& options)
{
    auto ytConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    ytConfig->SetDefaults();
    ytConfig->ClusterUrl = config.GetClusterName();

    if (!config.GetProxyRole().empty()) {
        ytConfig->ProxyRole = config.GetProxyRole();
    }
    if (0 != config.GetChannelPoolSize()) {
        ytConfig->DynamicChannelPool->MaxPeerCount = config.GetChannelPoolSize();
    }
    if (0 != config.GetChannelPoolRebalanceIntervalSeconds()) {
        ytConfig->DynamicChannelPool->RandomPeerEvictionPeriod = TDuration::Seconds(config.GetChannelPoolRebalanceIntervalSeconds());
    }
    if (0 != config.GetModifyRowsBatchCapacity()) {
        ytConfig->ModifyRowsBatchCapacity = config.GetModifyRowsBatchCapacity();
    }

#define SET_TIMEOUT_OPTION(name) \
    if (0 != config.Get##name()) ytConfig->name = TDuration::MilliSeconds(config.Get ## name())

    SET_TIMEOUT_OPTION(DefaultTransactionTimeout);
    SET_TIMEOUT_OPTION(DefaultSelectRowsTimeout);
    SET_TIMEOUT_OPTION(DefaultLookupRowsTimeout);
    SET_TIMEOUT_OPTION(DefaultTotalStreamingTimeout);
    SET_TIMEOUT_OPTION(DefaultStreamingStallTimeout);
    SET_TIMEOUT_OPTION(DefaultPingPeriod);

#undef SET_TIMEOUT_OPTION

    ytConfig->ResponseCodec = GetResponseCodecFromProto(config.GetResponseCodec());

    ytConfig->EnableRetries = config.GetEnableRetries();

    if (config.HasRetryBackoffTime()) {
        ytConfig->RetryingChannel->RetryBackoffTime = TDuration::MilliSeconds(config.GetRetryBackoffTime());
    }
    if (config.HasRetryAttempts()) {
        ytConfig->RetryingChannel->RetryAttempts = config.GetRetryAttempts();
    }
    if (config.HasRetryTimeout()) {
        ytConfig->RetryingChannel->RetryTimeout = TDuration::MilliSeconds(config.GetRetryTimeout());
    }
    ytConfig->Postprocess();
    return NApi::NRpcProxy::CreateConnection(ytConfig)->CreateClient(options);
}

NApi::IClientPtr CreateClient(const TConfig& config)
{
    return CreateClient(config, GetClientOpsFromEnvStatic());
}

NApi::IClientPtr CreateClient(TStringBuf clusterUrl)
{
    return CreateClient(clusterUrl, GetClientOpsFromEnvStatic());
}

NApi::IClientPtr CreateClient(TStringBuf cluster, TStringBuf proxyRole)
{
    TConfig config;
    config.SetClusterName(ToString(cluster));
    if (!proxyRole.empty()) {
        config.SetProxyRole(ToString(proxyRole));
    }
    return CreateClient(config);
}

NApi::IClientPtr CreateClient()
{
    return CreateClient(Strip(GetEnv("YT_PROXY")));
}

NApi::IClientPtr CreateClient(TStringBuf clusterUrl, const NApi::TClientOptions& options)
{
    TConfig config;
    SetClusterUrl(config, clusterUrl);
    return CreateClient(config, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
