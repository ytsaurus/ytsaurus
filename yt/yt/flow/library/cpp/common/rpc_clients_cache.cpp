#include "rpc_clients_cache.h"

#include "job_proxy_client.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/cache/cache_base.h>
#include <yt/yt/client/cache/config.h>
#include <yt/yt/client/cache/rpc.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/system/atomic_object.h>

#include <util/system/env.h>

#include <optional>
#include <string>

namespace NYT::NFlow {

using namespace NApi;
using namespace NClient::NCache;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TJobProxyRoute
{
    std::string Cluster;
    std::string SocketPath;
};

std::optional<TJobProxyRoute> GetJobProxyRouteFromEnv()
{
    auto cluster = GetEnv("YT_FLOW_JOB_PROXY_CLUSTER");
    if (cluster.empty()) {
        return std::nullopt;
    }
    auto socketPath = GetEnv("YT_JOB_PROXY_SOCKET_PATH");
    THROW_ERROR_EXCEPTION_IF(socketPath.empty(), "YT_JOB_PROXY_SOCKET_PATH is absent for job proxy cluster %Qv", cluster);
    return TJobProxyRoute{std::string(cluster), std::string(socketPath)};
}

std::string NormalizeCluster(TStringBuf cluster)
{
    auto name = NClient::NCache::ExtractClusterAndProxyRole(cluster).first;
    name.SkipPrefix("https://");
    return std::string(NNet::InferYTClusterFromClusterUrlRaw(name).value_or(name));
}

bool IsJobProxyCluster(TStringBuf cluster, TStringBuf jobProxyCluster)
{
    return !jobProxyCluster.empty() && NormalizeCluster(cluster) == NormalizeCluster(jobProxyCluster);
}

void ApplyJobProxyRoute(
    const NRpcProxy::TConnectionConfigPtr& config,
    TStringBuf jobProxyCluster,
    TStringBuf jobProxySocketPath)
{
    THROW_ERROR_EXCEPTION_UNLESS(!jobProxySocketPath.empty(),
        "YT_JOB_PROXY_SOCKET_PATH is absent for job proxy cluster %Qv",
        jobProxyCluster);

    // Preserve RPC timeouts, retry policy, codecs and transport bands; only replace proxy routing.
    config->ClusterUrl = NormalizeCluster(jobProxyCluster);
    config->ClusterName.reset();
    config->ProxyRole.reset();
    config->ProxyAddresses.reset();
    config->ProxyEndpoints.Reset();
    config->ProxyUnixDomainSocket = std::string(jobProxySocketPath);
    config->EnableProxyDiscovery = false;
}

NRpcProxy::TConnectionConfigPtr GetJobProxyConnectionConfig(
    const IClientPtr& client,
    TStringBuf cluster,
    TStringBuf jobProxyCluster,
    TStringBuf jobProxySocketPath)
{
    if (!IsJobProxyCluster(cluster, jobProxyCluster)) {
        return nullptr;
    }

    auto config = NYTree::ConvertTo<NRpcProxy::TConnectionConfigPtr>(client->GetConnection()->GetConfigYson());
    ApplyJobProxyRoute(config, jobProxyCluster, jobProxySocketPath);
    return config;
}

struct TConnectionEntry
{
    NYTree::INodePtr Config;
    IConnectionPtr Connection;
};

IConnectionPtr GetOrCreateConnection(
    TAtomicObject<std::vector<TConnectionEntry>>* connections,
    const NRpcProxy::TConnectionConfigPtr& config)
{
    // Connection construction postprocesses config, so keep a separate snapshot for lookup.
    auto configNode = NYTree::ConvertToNode(config);
    auto findConnection = [&] (const auto& entries) -> IConnectionPtr {
        for (const auto& entry : entries) {
            if (NYTree::AreNodesEqual(entry.Config, configNode)) {
                return entry.Connection;
            }
        }
        return nullptr;
    };
    auto connection = connections->Read(findConnection);
    if (!connection) {
        IConnectionPtr newConnection = NRpcProxy::CreateConnection(config);
        connection = connections->Transform([&] (auto& entries) {
            if (auto existing = findConnection(entries)) {
                return existing;
            }
            entries.push_back({configNode, newConnection});
            return newConnection;
        });
    }
    return connection;
}

class TRpcClientsCache
    : public TClientsCacheBase
{
public:
    TRpcClientsCache(
        const TClientsCacheConfigPtr& config,
        const TClientOptions& clientOptions,
        std::optional<TJobProxyRoute> jobProxyRoute)
        : Config_(GetClustersConfigWithNormalClusterName(config))
        , ClientOptions_(clientOptions)
        , JobProxyRoute_(std::move(jobProxyRoute))
    { }

protected:
    IClientPtr CreateClient(TStringBuf clusterUrl) override
    {
        auto config = GetConnectionConfig(Config_, clusterUrl);
        if (!JobProxyRoute_ || !IsJobProxyCluster(clusterUrl, JobProxyRoute_->Cluster)) {
            return NClient::NCache::CreateClient(config, ClientOptions_);
        }

        ApplyJobProxyRoute(config, JobProxyRoute_->Cluster, JobProxyRoute_->SocketPath);
        return GetOrCreateConnection(&Connections_, config)->CreateClient(ClientOptions_);
    }

private:
    const TClientsCacheConfigPtr Config_;
    const TClientOptions ClientOptions_;
    const std::optional<TJobProxyRoute> JobProxyRoute_;
    TAtomicObject<std::vector<TConnectionEntry>> Connections_;
};

} // namespace

IClientsCachePtr CreateClientsCache(
    const TClientsCacheConfigPtr& config,
    const TClientOptions& clientOptions)
{
    return New<TRpcClientsCache>(config, clientOptions, GetJobProxyRouteFromEnv());
}

IClientPtr GetJobProxyClient(
    const IClientPtr& client,
    TStringBuf cluster,
    TStringBuf jobProxyCluster,
    TStringBuf jobProxySocketPath)
{
    auto config = GetJobProxyConnectionConfig(client, cluster, jobProxyCluster, jobProxySocketPath);
    return config ? NRpcProxy::CreateConnection(config)->CreateClient(client->GetOptions()) : client;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
