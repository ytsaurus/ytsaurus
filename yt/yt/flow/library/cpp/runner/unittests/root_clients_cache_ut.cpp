#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/runner/root_clients_cache.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/testing/env_guard.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/options.h>

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/client/cache/config.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NClient::NCache;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NYPath::TRichYPath MakePipelinePath()
{
    NYPath::TRichYPath path("//tmp/pipeline");
    path.SetCluster("localhost");
    return path;
}

TRootClientsCacheOptions MakeOptions()
{
    auto config = New<TClientsCacheConfig>();
    config->SetDefaults();

    return {
        .PipelinePath = MakePipelinePath(),
        .ClientsCacheConfig = std::move(config),
        .ClientOptions = NApi::TClientOptions::FromUser("test-user"),
    };
}

//! Restores the built-in factory, which is process-wide state shared by all tests here.
class TFactoryGuard
{
public:
    explicit TFactoryGuard(TRootClientsCacheFactory factory)
    {
        SetRootClientsCacheFactory(std::move(factory));
    }

    ~TFactoryGuard()
    {
        SetRootClientsCacheFactory({});
    }
};

class TFakeClientsCache
    : public IClientsCache
{
public:
    explicit TFakeClientsCache(TRootClientsCacheOptions options)
        : Options(std::move(options))
    { }

    NApi::IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return nullptr;
    }

    const TRootClientsCacheOptions Options;
};

DEFINE_REFCOUNTED_TYPE(TFakeClientsCache)

////////////////////////////////////////////////////////////////////////////////

class TRootClientsCacheTest
    : public ::testing::Test
{
private:
    NTesting::TEnvGuard ClusterGuard_{"YT_FLOW_JOB_PROXY_CLUSTER"};
    NTesting::TEnvGuard SocketGuard_{"YT_JOB_PROXY_SOCKET_PATH"};
};

TEST_F(TRootClientsCacheTest, BuiltInFactory)
{
    auto cache = CreateRootClientsCache(MakeOptions());

    ASSERT_TRUE(cache);
    EXPECT_TRUE(cache->GetClient("localhost:1"));
}

TEST_F(TRootClientsCacheTest, PipelineClusterOfFullUrlMatchesShortName)
{
    auto client = CreateRootClientsCache(MakeOptions())->GetClient("hahn.yt.yandex.net");
    ASSERT_TRUE(client);
    const auto& clusterName = client->GetConnection()->GetClusterName();
    EXPECT_EQ(clusterName, "hahn");

    // The runner preflight and #RunPipeline() validate with this name, so both see the conflict.
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(R""""({
        computations = {
            c = {
                computation_class_name = "NYT::NFlow::TPassthroughComputation";
                group_by_schema = [
                    { name = hash; expression = "farm_hash(key)"; type = uint64; required = %true; sort_order = ascending; };
                    { name = key; type = string; sort_order = ascending; };
                ];
                key_visitor_streams = { visit = {}; };
                output_stream_ids = [t];
                external_state_managers = {
                    "/m1" = {
                        external_state_manager_class_name = "NYT::NFlow::TSimpleExternalStateManager";
                        parameters = { path = "//tmp/state"; };
                    };
                    "/m2" = {
                        external_state_manager_class_name = "NYT::NFlow::TSimpleExternalStateManager";
                        parameters = { path = "<cluster=hahn>//tmp/state"; };
                    };
                };
            };
        };
        streams = {
            t = { schema = [ { name = key; type = string; } ]; };
        };
    })""""));
    ValidatePipelineSpec(spec);
    EXPECT_THROW_WITH_SUBSTRING(
        ValidatePipelineSpec(spec, clusterName),
        "claimed for writing");
}

TEST_F(TRootClientsCacheTest, FactoryParametersWithoutFactory)
{
    auto options = MakeOptions();
    options.Parameters = ConvertToNode(TYsonString(TStringBuf("{federation=[a]}")));

    EXPECT_THROW_WITH_SUBSTRING(
        CreateRootClientsCache(options),
        "no root clients cache factory is installed");
}

TEST_F(TRootClientsCacheTest, ProxyRoleReachesPerClusterConnection)
{
    TFactoryGuard guard([] (const TRootClientsCacheOptions& options) -> IClientsCachePtr {
        return New<TFakeClientsCache>(options);
    });

    auto options = MakeOptions();
    options.ProxyRole = "flow";
    options.ClientsCacheConfig->PerClusterConnection["localhost"] = New<NApi::NRpcProxy::TConnectionConfig>();
    options.ClientsCacheConfig->PerClusterConnection["other"] = New<NApi::NRpcProxy::TConnectionConfig>();

    auto fakeCache = DynamicPointerCast<TFakeClientsCache>(CreateRootClientsCache(options));
    ASSERT_TRUE(fakeCache);

    const auto& config = fakeCache->Options.ClientsCacheConfig;
    EXPECT_EQ("flow", config->DefaultConnection->ProxyRole);
    EXPECT_EQ("flow", config->PerClusterConnection["localhost"]->ProxyRole);
    // Other clusters are reached with whatever their own connection config says.
    EXPECT_FALSE(config->PerClusterConnection["other"]->ProxyRole);
    // The caller's config is left alone.
    EXPECT_FALSE(options.ClientsCacheConfig->DefaultConnection->ProxyRole);
}

TEST_F(TRootClientsCacheTest, CustomFactory)
{
    auto parameters = ConvertToNode(TYsonString(TStringBuf("{federation=[a;b]}")));

    TFactoryGuard guard([] (const TRootClientsCacheOptions& options) -> IClientsCachePtr {
        return New<TFakeClientsCache>(options);
    });

    auto options = MakeOptions();
    options.Parameters = parameters;
    auto cache = CreateRootClientsCache(options);

    auto fakeCache = DynamicPointerCast<TFakeClientsCache>(cache);
    ASSERT_TRUE(fakeCache);
    EXPECT_EQ(options.ClientsCacheConfig, fakeCache->Options.ClientsCacheConfig);
    EXPECT_EQ("test-user", fakeCache->Options.ClientOptions.GetAuthenticatedUser());
    EXPECT_EQ(options.PipelinePath.GetPath(), fakeCache->Options.PipelinePath.GetPath());
    EXPECT_EQ(parameters, fakeCache->Options.Parameters);
}

TEST_F(TRootClientsCacheTest, RoutesAllLocalClientsThroughJobProxy)
{
    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "localhost");
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
    auto options = MakeOptions();
    options.ProxyRole = "flow";
    auto cache = CreateRootClientsCache(options);
    for (auto cluster : {"localhost", "localhost/explicit"}) {
        auto client = cache->GetClient(cluster);
        auto config = ConvertTo<NApi::NRpcProxy::TConnectionConfigPtr>(client->GetConnection()->GetConfigYson());
        EXPECT_EQ(config->ProxyUnixDomainSocket, "/tmp/job.sock");
        EXPECT_FALSE(config->EnableProxyDiscovery);
        EXPECT_EQ(client, cache->GetClient(cluster));
        EXPECT_EQ(client->GetOptions().GetAuthenticatedUser(), "test-user");
    }
    auto remote = cache->GetClient("other/explicit");
    auto config = ConvertTo<NApi::NRpcProxy::TConnectionConfigPtr>(remote->GetConnection()->GetConfigYson());
    EXPECT_FALSE(config->ProxyUnixDomainSocket);
    EXPECT_EQ(config->ProxyRole, "explicit");
}

TEST_F(TRootClientsCacheTest, RequiresJobProxySocket)
{
    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "localhost");
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH");
    EXPECT_THROW_WITH_SUBSTRING(
        CreateRootClientsCache(MakeOptions()),
        "YT_JOB_PROXY_SOCKET_PATH is absent");
}

TEST_F(TRootClientsCacheTest, RejectsCustomFactoryWithJobProxy)
{
    NTesting::TEnvGuard clusterGuard("YT_FLOW_JOB_PROXY_CLUSTER", "localhost");
    NTesting::TEnvGuard socketGuard("YT_JOB_PROXY_SOCKET_PATH", "/tmp/job.sock");
    TFactoryGuard guard([] (const TRootClientsCacheOptions&) -> IClientsCachePtr {
        ADD_FAILURE() << "Custom factory must not be called";
        return nullptr;
    });
    EXPECT_THROW_WITH_SUBSTRING(CreateRootClientsCache(MakeOptions()), "not supported with a custom root clients cache factory");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
