#include "rpc.h"
#include "options.h"

#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <util/string/strip.h>
#include <util/system/env.h>

namespace {

TString ExpandClusterName(TString cluster) {
    Y_ENSURE(!cluster.empty(), "No cluster name specified!");
    if (cluster.find('.') == TString::npos && cluster.find(':') == TString::npos && cluster != "localhost") {
        cluster += ".yt.yandex.net";
    }
    return cluster;
}

} // namespace

namespace NYT::NClient::NHedging::NRpc {

void SetClusterUrl(TConfig& config, TStringBuf clusterUrl) {
    TStringBuf cluster;
    TStringBuf proxyRole;
    clusterUrl.Split('/', cluster, proxyRole);
    if (!proxyRole.empty()) {
        Y_ENSURE(config.GetProxyRole().empty(), "ProxyRole specified in both: config and url");
        config.SetProxyRole(ToString(proxyRole));
    }
    config.SetClusterName(ToString(cluster));
}

NYT::NApi::IClientPtr CreateClient(const TConfig& config, const NYT::NApi::TClientOptions& options) {
    auto ytConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    ytConfig->SetDefaults();
    ytConfig->ClusterUrl = ExpandClusterName(config.GetClusterName());

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
    SET_TIMEOUT_OPTION(DefaultTotalStreamingTimeout);
    SET_TIMEOUT_OPTION(DefaultStreamingStallTimeout);
    SET_TIMEOUT_OPTION(DefaultPingPeriod);

#undef SET_TIMEOUT_OPTION

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
    return NYT::NApi::NRpcProxy::CreateConnection(ytConfig)->CreateClient(options);
}

NYT::NApi::IClientPtr CreateClient(const TConfig& config) {
    return CreateClient(config, GetClientOpsFromEnvStatic());
}

NYT::NApi::IClientPtr CreateClient(TStringBuf clusterUrl) {
    return CreateClient(clusterUrl, GetClientOpsFromEnvStatic());
}

NYT::NApi::IClientPtr CreateClient(TStringBuf cluster, TStringBuf proxyRole) {
    TConfig config;
    config.SetClusterName(ToString(cluster));
    if (!proxyRole.empty()) {
        config.SetProxyRole(ToString(proxyRole));
    }
    return CreateClient(config);
}

NYT::NApi::IClientPtr CreateClient() {
    return CreateClient(Strip(GetEnv("YT_PROXY")));
}

NYT::NApi::IClientPtr CreateClient(TStringBuf clusterUrl, const NYT::NApi::TClientOptions& options) {
    TConfig config;
    SetClusterUrl(config, clusterUrl);
    return CreateClient(config, options);
}

} // namespace NYT::NClient::NHedging::NRpc
