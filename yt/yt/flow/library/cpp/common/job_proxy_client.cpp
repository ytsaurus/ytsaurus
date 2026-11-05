#include "job_proxy_client.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/cache/cache_base.h>
#include <yt/yt/client/cache/rpc.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/system/atomic_object.h>

#include <util/system/env.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NClient::NCache;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string NormalizeCluster(TStringBuf cluster)
{
    auto name = NClient::NCache::ExtractClusterAndProxyRole(cluster).first;
    name.SkipPrefix("https://");
    return std::string(NNet::InferYTClusterFromClusterUrlRaw(name).value_or(name));
}

NRpcProxy::TConnectionConfigPtr GetJobProxyConnectionConfig(
    const IClientPtr& client,
    TStringBuf cluster,
    TStringBuf jobProxyCluster,
    TStringBuf jobProxySocketPath)
{
    if (jobProxyCluster.empty()) {
        return nullptr;
    }

    auto normalizedJobProxyCluster = NormalizeCluster(jobProxyCluster);
    if (NormalizeCluster(cluster) != normalizedJobProxyCluster) {
        return nullptr;
    }

    THROW_ERROR_EXCEPTION_UNLESS(!jobProxySocketPath.empty(),
        "YT_JOB_PROXY_SOCKET_PATH is absent for job proxy cluster %Qv",
        jobProxyCluster);

    // Preserve RPC timeouts, retry policy, codecs and transport bands; only replace proxy routing.
    auto config = NYTree::ConvertTo<NRpcProxy::TConnectionConfigPtr>(client->GetConnection()->GetConfigYson());
    config->ClusterUrl = normalizedJobProxyCluster;
    config->ClusterName.reset();
    config->ProxyRole.reset();
    config->ProxyAddresses.reset();
    config->ProxyEndpoints.Reset();
    config->ProxyUnixDomainSocket = std::string(jobProxySocketPath);
    config->EnableProxyDiscovery = false;
    return config;
}

struct TJobProxyConnectionEntry
{
    NYTree::INodePtr Config;
    IConnectionPtr Connection;
};

class TJobProxyClientsCache
    : public TClientsCacheBase
{
public:
    TJobProxyClientsCache(IClientsCachePtr underlying, std::string cluster, std::string socketPath)
        : Underlying_(std::move(underlying))
        , Cluster_(std::move(cluster))
        , SocketPath_(std::move(socketPath))
    { }

protected:
    IClientPtr CreateClient(TStringBuf clusterUrl) override
    {
        auto client = Underlying_->GetClient(clusterUrl);
        auto config = GetJobProxyConnectionConfig(client, clusterUrl, Cluster_, SocketPath_);
        if (!config) {
            return client;
        }
        auto configNode = NYTree::ConvertToNode(config);
        auto findConnection = [&] (const auto& entries) -> IConnectionPtr {
            for (const auto& entry : entries) {
                if (NYTree::AreNodesEqual(entry.Config, configNode)) {
                    return entry.Connection;
                }
            }
            return nullptr;
        };
        auto connection = Connections_.Read(findConnection);
        if (!connection) {
            IConnectionPtr newConnection = NRpcProxy::CreateConnection(config);
            connection = Connections_.Transform([&] (auto& entries) {
                if (auto existing = findConnection(entries)) {
                    return existing;
                }
                entries.push_back({configNode, newConnection});
                return newConnection;
            });
        }
        return connection->CreateClient(client->GetOptions());
    }

private:
    const IClientsCachePtr Underlying_;
    const std::string Cluster_;
    const std::string SocketPath_;
    TAtomicObject<std::vector<TJobProxyConnectionEntry>> Connections_;
};

} // namespace

IClientPtr GetJobProxyClient(
    const IClientPtr& client,
    TStringBuf cluster,
    TStringBuf jobProxyCluster,
    TStringBuf jobProxySocketPath)
{
    auto config = GetJobProxyConnectionConfig(client, cluster, jobProxyCluster, jobProxySocketPath);
    return config ? NRpcProxy::CreateConnection(config)->CreateClient(client->GetOptions()) : client;
}

IClientsCachePtr CreateJobProxyClientsCache(IClientsCachePtr cache)
{
    auto cluster = GetEnv("YT_FLOW_JOB_PROXY_CLUSTER");
    if (cluster.empty()) {
        return cache;
    }
    auto socketPath = GetEnv("YT_JOB_PROXY_SOCKET_PATH");
    THROW_ERROR_EXCEPTION_IF(socketPath.empty(), "YT_JOB_PROXY_SOCKET_PATH is absent for job proxy cluster %Qv", cluster);
    return New<TJobProxyClientsCache>(std::move(cache), std::string(cluster), std::string(socketPath));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
