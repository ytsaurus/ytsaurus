#include <yt/yt/flow/library/cpp/common/job_proxy_client.h>
#include <yt/yt/flow/library/cpp/common/rpc_clients_cache.h>

#include <yt/yt/flow/library/cpp/misc/testing/env_guard.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/cache/config.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NYTree;

IClientPtr MakeClient()
{
    auto config = New<NRpcProxy::TConnectionConfig>();
    config->ClusterUrl = "hahn";
    config->ProxyRole = "heavy";
    config->ProxyAddresses = std::vector<std::string>{"remote:9013"};
    return NRpcProxy::CreateConnection(config)->CreateClient(TClientOptions::FromUser("reader"));
}

TEST(TJobProxyClientTest, PreservesDefaultAndOtherClusterClients)
{
    auto client = MakeClient();
    EXPECT_EQ(client, GetJobProxyClient(client, "hahn", "", ""));
    EXPECT_EQ(client, GetJobProxyClient(client, "arnold", "hahn", "/tmp/job.sock"));
}

TEST(TJobProxyClientTest, OverridesExplicitProxyRole)
{
    auto client = MakeClient();
    for (auto cluster : {"hahn/heavy", "http://hahn.yt.yandex.net/heavy", "https://hahn.yt.yandex.net/heavy"}) {
        auto local = GetJobProxyClient(client, cluster, "hahn", "/tmp/job.sock");
        auto config = ConvertTo<NRpcProxy::TConnectionConfigPtr>(local->GetConnection()->GetConfigYson());
        EXPECT_EQ(config->ProxyUnixDomainSocket, "/tmp/job.sock");
        EXPECT_FALSE(config->ProxyRole);
    }
}

TEST(TJobProxyClientTest, UsesSocketAndPreservesAuthentication)
{
    auto client = MakeClient();
    auto reader = GetJobProxyClient(client, "hahn", "http://hahn.yt.yandex.net/heavy", "/tmp/job.sock");
    auto config = ConvertTo<NRpcProxy::TConnectionConfigPtr>(reader->GetConnection()->GetConfigYson());
    EXPECT_EQ(config->ProxyUnixDomainSocket, "/tmp/job.sock");
    EXPECT_FALSE(config->ProxyAddresses);
    EXPECT_FALSE(config->ProxyRole);
    EXPECT_FALSE(config->EnableProxyDiscovery);
    EXPECT_EQ(reader->GetOptions().GetAuthenticatedUser(), "reader");
    auto original = ConvertTo<NRpcProxy::TConnectionConfigPtr>(client->GetConnection()->GetConfigYson());
    EXPECT_FALSE(original->ProxyUnixDomainSocket);
    EXPECT_EQ(original->ProxyRole, "heavy");
}

TEST(TJobProxyClientTest, NormalizesHttpsClusterInBothDirections)
{
    auto client = MakeClient();
    auto firstReader = GetJobProxyClient(client, "https://hahn.yt.yandex.net", "hahn", "/tmp/job.sock");
    auto secondReader = GetJobProxyClient(client, "hahn", "https://hahn.yt.yandex.net", "/tmp/job.sock");
    EXPECT_NE(firstReader, client);
    EXPECT_NE(secondReader, client);
    auto config = ConvertTo<NRpcProxy::TConnectionConfigPtr>(firstReader->GetConnection()->GetConfigYson());
    EXPECT_EQ(config->ProxyUnixDomainSocket, "/tmp/job.sock");
    EXPECT_EQ(config->ClusterUrl, "hahn");
    EXPECT_EQ(client, GetJobProxyClient(client, "https://arnold.yt.yandex.net", "hahn", ""));
}

TEST(TJobProxyClientTest, RequiresSocketForLocalCluster)
{
    EXPECT_THROW_WITH_SUBSTRING(
        GetJobProxyClient(MakeClient(), "hahn", "hahn", ""),
        "YT_JOB_PROXY_SOCKET_PATH is absent");
}

TEST(TJobProxyClientTest, ConnectionInvokerSurvivesOriginalClient)
{
    auto config = New<NRpcProxy::TConnectionConfig>();
    config->ProxyAddresses = std::vector<std::string>{"remote:9013"};
    config->EnableProxyDiscovery = false;
    auto local = GetJobProxyClient(
        NRpcProxy::CreateConnection(config)->CreateClient(),
        "hahn",
        "hahn",
        "/tmp/lifetime.sock");
    auto result = NConcurrency::WaitFor(BIND([] {
    })
            .AsyncVia(local->GetConnection()->GetInvoker())
            .Run()
            .WithTimeout(TDuration::Seconds(5)));
    EXPECT_NO_THROW(result.ThrowOnError());
}

TEST(TJobProxyClientTest, DoesNotShareDifferentSockets)
{
    auto client = MakeClient();
    auto firstReader = GetJobProxyClient(client, "hahn", "hahn", "/tmp/first.sock");
    auto secondReader = GetJobProxyClient(client, "hahn", "hahn", "/tmp/second.sock");
    EXPECT_NE(firstReader->GetConnection(), secondReader->GetConnection());
}

class TRpcClientsCacheTest
    : public ::testing::Test
{
private:
    NTesting::TEnvGuard ClusterGuard_{"YT_FLOW_JOB_PROXY_CLUSTER"};
    NTesting::TEnvGuard SocketGuard_{"YT_JOB_PROXY_SOCKET_PATH"};
};

TEST_F(TRpcClientsCacheTest, SharesLocalConnectionAndPreservesClientOptions)
{
    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "hahn");
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
    auto config = New<NClient::NCache::TClientsCacheConfig>();
    config->DefaultConnection->ProxyRole = "heavy";
    auto cache = NFlow::CreateClientsCache(
        config,
        TClientOptions::FromToken("reader-token"));
    auto first = cache->GetClient("hahn");
    for (auto cluster : {"hahn", "hahn/explicit", "http://hahn.yt.yandex.net", "https://hahn.yt.yandex.net"}) {
        auto client = cache->GetClient(cluster);
        EXPECT_EQ(client, cache->GetClient(cluster));
        EXPECT_EQ(first->GetConnection(), client->GetConnection());
        EXPECT_EQ(client->GetOptions().Token, "reader-token");
        auto connectionConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(client->GetConnection()->GetConfigYson());
        EXPECT_EQ(connectionConfig->ProxyUnixDomainSocket, "/tmp/job.sock");
        EXPECT_FALSE(connectionConfig->EnableProxyDiscovery);
        EXPECT_FALSE(connectionConfig->ProxyRole);
    }
    auto remote = cache->GetClient("arnold/explicit");
    auto connectionConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(remote->GetConnection()->GetConfigYson());
    EXPECT_FALSE(connectionConfig->ProxyUnixDomainSocket);
    EXPECT_EQ(connectionConfig->ProxyRole, "explicit");
    EXPECT_EQ(remote->GetOptions().Token, "reader-token");
    auto anotherCache = NFlow::CreateClientsCache(config, TClientOptions::FromToken("reader-token"));
    EXPECT_NE(first->GetConnection(), anotherCache->GetClient("hahn")->GetConnection());
}

TEST_F(TRpcClientsCacheTest, PreservesPerClusterConfiguration)
{
    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "hahn");
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
    auto config = New<NClient::NCache::TClientsCacheConfig>();
    auto localConfig = CloneYsonStruct(config->DefaultConnection);
    localConfig->DefaultTotalStreamingTimeout += TDuration::Seconds(1);
    config->PerClusterConnection["http://hahn.yt.yandex.net"] = localConfig;
    auto httpsConfig = CloneYsonStruct(localConfig);
    httpsConfig->DefaultTotalStreamingTimeout += TDuration::Seconds(1);
    config->PerClusterConnection["https://hahn.yt.yandex.net"] = httpsConfig;
    auto originalConfig = ConvertToNode(config);
    auto cache = NFlow::CreateClientsCache(config, TClientOptions::FromUser("reader"));
    auto first = cache->GetClient("hahn");
    auto second = cache->GetClient("http://hahn.yt.yandex.net/explicit");
    auto third = cache->GetClient("https://hahn.yt.yandex.net");
    EXPECT_EQ(first->GetConnection(), second->GetConnection());
    EXPECT_NE(first->GetConnection(), third->GetConnection());
    auto firstConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(first->GetConnection()->GetConfigYson());
    auto thirdConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(third->GetConnection()->GetConfigYson());
    EXPECT_EQ(firstConfig->DefaultTotalStreamingTimeout, localConfig->DefaultTotalStreamingTimeout);
    EXPECT_EQ(thirdConfig->DefaultTotalStreamingTimeout, httpsConfig->DefaultTotalStreamingTimeout);
    EXPECT_TRUE(AreNodesEqual(originalConfig, ConvertToNode(config)));
}

TEST_F(TRpcClientsCacheTest, SelectsJobProxyBeforeCreatingConnection)
{
    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "hahn");
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
    auto config = New<NClient::NCache::TClientsCacheConfig>();
    config->DefaultConnection->ProxyAddresses = std::vector<std::string>{};
    auto cache = NFlow::CreateClientsCache(config, TClientOptions::FromUser("reader"));
    EXPECT_TRUE(cache->GetClient("hahn"));
    EXPECT_THROW_WITH_SUBSTRING(cache->GetClient("arnold"), "\"proxy_addresses\" must not be empty");
}

TEST_F(TRpcClientsCacheTest, MatchesCoreCacheWithoutJobProxy)
{
    auto config = New<NClient::NCache::TClientsCacheConfig>();
    config->DefaultConnection->ProxyRole = "default";
    auto perCluster = CloneYsonStruct(config->DefaultConnection);
    perCluster->ClusterUrl = "localhost:9013";
    perCluster->DefaultTotalStreamingTimeout += TDuration::Seconds(1);
    config->PerClusterConnection["http://hahn.yt.yandex.net"] = perCluster;
    auto options = TClientOptions::FromUser("reader");
    auto cache = NFlow::CreateClientsCache(config, options);
    auto coreCache = NClient::NCache::CreateClientsCache(config, options);
    for (auto cluster : {"hahn", "http://hahn.yt.yandex.net/explicit", "https://hahn.yt.yandex.net", "localhost:9013", "arnold/other"}) {
        auto client = cache->GetClient(cluster);
        EXPECT_EQ(client, cache->GetClient(cluster));
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(client->GetConnection()->GetConfigYson()),
            ConvertToNode(coreCache->GetClient(cluster)->GetConnection()->GetConfigYson())));
        EXPECT_EQ(client->GetOptions().GetAuthenticatedUser(), "reader");
    }
}

TEST_F(TRpcClientsCacheTest, ConnectionInvokerSurvivesCache)
{
    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "hahn");
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
    auto client = NFlow::CreateClientsCache(
        New<NClient::NCache::TClientsCacheConfig>(),
        TClientOptions::FromUser("reader"))
        ->GetClient("hahn");
    auto result = NConcurrency::WaitFor(BIND([] {
    })
            .AsyncVia(client->GetConnection()->GetInvoker())
            .Run()
            .WithTimeout(TDuration::Seconds(5)));
    EXPECT_NO_THROW(result.ThrowOnError());
}

TEST_F(TRpcClientsCacheTest, RejectsJobProxyRouteWithoutSocket)
{
    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "hahn");
    EXPECT_THROW_WITH_SUBSTRING(
        NFlow::CreateClientsCache(New<NClient::NCache::TClientsCacheConfig>(), /*clientOptions*/ {}),
        "YT_JOB_PROXY_SOCKET_PATH is absent");
}

TEST_F(TRpcClientsCacheTest, CapturesJobProxyEnvironmentAtCreation)
{
    auto config = New<NClient::NCache::TClientsCacheConfig>();
    auto options = TClientOptions::FromUser("reader");
    auto directCache = NFlow::CreateClientsCache(config, options);
    NClient::NCache::IClientsCachePtr jobProxyCache;
    {
        NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "hahn");
        NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
        jobProxyCache = NFlow::CreateClientsCache(config, options);
        auto directConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(directCache->GetClient("hahn")->GetConnection()->GetConfigYson());
        EXPECT_FALSE(directConfig->ProxyUnixDomainSocket);
    }
    auto jobProxyConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(jobProxyCache->GetClient("hahn")->GetConnection()->GetConfigYson());
    EXPECT_EQ(jobProxyConfig->ProxyUnixDomainSocket, "/tmp/job.sock");
}

TEST_F(TRpcClientsCacheTest, IgnoresSocketWithoutJobProxyCluster)
{
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
    auto cache = NFlow::CreateClientsCache(New<NClient::NCache::TClientsCacheConfig>(), TClientOptions::FromUser("reader"));
    auto config = ConvertTo<NRpcProxy::TConnectionConfigPtr>(cache->GetClient("hahn")->GetConnection()->GetConfigYson());
    EXPECT_FALSE(config->ProxyUnixDomainSocket);
}

} // namespace
} // namespace NYT::NFlow
