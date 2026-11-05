#include <yt/yt/flow/library/cpp/common/job_proxy_client.h>

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

TEST(TJobProxyClientTest, SharesConnectionAcrossClusterSpellings)
{
    auto cacheConfig = New<NClient::NCache::TClientsCacheConfig>();
    cacheConfig->DefaultConnection = New<NRpcProxy::TConnectionConfig>();
    auto authOptions = New<NClient::NCache::TClientsCacheAuthenticationOptions>();
    authOptions->DefaultOptions = TClientOptions::FromToken("short-name-token");
    authOptions->ClusterOptions["http://hahn.yt.yandex.net"] = TClientOptions::FromToken("url-token");
    auto cache = NClient::NCache::CreateClientsCache(cacheConfig, authOptions);
    auto first = cache->GetClient("hahn");
    auto second = cache->GetClient("http://hahn.yt.yandex.net");
    ASSERT_NE(first, second);

    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "hahn");
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
    auto localCache = CreateJobProxyClientsCache(cache);
    auto firstReader = localCache->GetClient("hahn");
    auto secondReader = localCache->GetClient("http://hahn.yt.yandex.net");
    EXPECT_EQ(firstReader, localCache->GetClient("hahn"));
    EXPECT_EQ(secondReader, localCache->GetClient("http://hahn.yt.yandex.net"));
    EXPECT_EQ(cache->GetClient("arnold"), localCache->GetClient("arnold"));
    auto anotherCache = CreateJobProxyClientsCache(cache);
    EXPECT_NE(firstReader->GetConnection(), anotherCache->GetClient("hahn")->GetConnection());
    EXPECT_EQ(firstReader->GetConnection(), secondReader->GetConnection());
    EXPECT_EQ(firstReader->GetOptions().Token, "short-name-token");
    EXPECT_EQ(secondReader->GetOptions().Token, "url-token");
}

TEST(TJobProxyClientTest, PreservesDifferentConnectionSettings)
{
    auto cacheConfig = New<NClient::NCache::TClientsCacheConfig>();
    auto config = CloneYsonStruct(cacheConfig->DefaultConnection);
    config->DefaultTotalStreamingTimeout += TDuration::Seconds(1);
    cacheConfig->PerClusterConnection["https://hahn.yt.yandex.net"] = config;
    auto cache = NClient::NCache::CreateClientsCache(cacheConfig, TClientOptions::FromUser("reader"));

    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "hahn");
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
    auto localCache = CreateJobProxyClientsCache(cache);
    auto firstReader = localCache->GetClient("hahn");
    auto secondReader = localCache->GetClient("https://hahn.yt.yandex.net");
    EXPECT_NE(firstReader->GetConnection(), secondReader->GetConnection());
    auto readerConfig = ConvertTo<NRpcProxy::TConnectionConfigPtr>(secondReader->GetConnection()->GetConfigYson());
    EXPECT_EQ(readerConfig->DefaultTotalStreamingTimeout, config->DefaultTotalStreamingTimeout);
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

} // namespace
} // namespace NYT::NFlow
