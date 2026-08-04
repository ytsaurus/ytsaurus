#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/runner/root_clients_cache.h>

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

TEST(TRootClientsCacheTest, BuiltInFactory)
{
    auto cache = CreateRootClientsCache(MakeOptions());

    ASSERT_TRUE(cache);
    EXPECT_TRUE(cache->GetClient("localhost:1"));
}

TEST(TRootClientsCacheTest, FactoryParametersWithoutFactory)
{
    auto options = MakeOptions();
    options.Parameters = ConvertToNode(TYsonString(TStringBuf("{federation=[a]}")));

    EXPECT_THROW_WITH_SUBSTRING(
        CreateRootClientsCache(options),
        "no root clients cache factory is installed");
}

TEST(TRootClientsCacheTest, ProxyRoleReachesPerClusterConnection)
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

TEST(TRootClientsCacheTest, CustomFactory)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
