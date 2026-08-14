#include <yt/yt/flow/library/cpp/runner/config.h>
#include <yt/yt/flow/library/cpp/runner/root_clients_cache.h>
#include <yt/yt/flow/library/cpp/runner/vanilla_launcher.h>

#include <yt/yt/flow/library/cpp/companion/config.h>

#include <yt/yt/flow/library/cpp/vanilla/spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/options.h>

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/cache/config.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/system/env.h>

#include <set>

namespace NYT::NFlow {
namespace {

using namespace NClient::NCache;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TVanillaConfigTest, AllowsCustomNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=custom})"));

    EXPECT_EQ(config->NetworkProject, std::optional<std::string>("custom"));
}

TEST(TVanillaConfigTest, EntityDisablesDefaultNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=#})"));

    EXPECT_FALSE(config->NetworkProject.has_value());
}

TEST(TVanillaConfigTest, AllowsPerTaskDockerImage)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1;docker_image="registry.example.com/image:tag"}})"));

    EXPECT_EQ(config->Worker->DockerImage, std::optional<std::string>("registry.example.com/image:tag"));
    EXPECT_FALSE(config->Controller->DockerImage.has_value());
}

// Without a network project the jobs share the exec node's network, where the fixed ports of
// co-located flow jobs collide — the out-of-the-box launch must ask YT for ports instead.
TEST(TVanillaConfigTest, DefaultsPortCountsWithoutNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=#})"));

    EXPECT_EQ(config->Controller->PortCount, std::optional<int>(2));
    EXPECT_EQ(config->Worker->PortCount, std::optional<int>(3));
}

TEST(TVanillaConfigTest, KeepsFixedPortsUnderNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=custom})"));

    EXPECT_FALSE(config->Controller->PortCount.has_value());
    EXPECT_FALSE(config->Worker->PortCount.has_value());
}

TEST(TVanillaConfigTest, ExplicitZeroPortCountKeepsFixedPorts)
{
    auto config = ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(
        R"({pool=test;controller={count=1;port_count=0};worker={count=1;port_count=0};network_project=#})"));

    EXPECT_EQ(config->Controller->PortCount, std::optional<int>(0));
    EXPECT_EQ(config->Worker->PortCount, std::optional<int>(0));
}

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf PipelineCluster = "pipeline-cluster";
// Stands for the address a runner pins when the cluster advertises an RPC proxy it cannot reach.
constexpr TStringBuf PinnedProxyAddress = "[64:ff9b::1]:9013";

NYPath::TRichYPath MakePipelinePath()
{
    NYPath::TRichYPath path("//tmp/pipeline");
    path.SetCluster(TString(PipelineCluster));
    return path;
}

TVanillaConfigPtr MakeVanillaConfig(TStringBuf extra = {})
{
    auto yson = Format("{enable=%%true; pool=test; worker={count=1}; %v}", extra);
    return ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(yson));
}

//! The clients cache a runner ends up with when its config pins the proxy address and turns
//! discovery off — the shape that lets it reach a cluster advertising an unreachable address.
IClientsCachePtr MakeCacheWithPinnedProxy()
{
    auto config = New<TClientsCacheConfig>();
    config->SetDefaults();
    config->DefaultConnection->EnableProxyDiscovery = false;
    config->DefaultConnection->ProxyAddresses = std::vector<std::string>{std::string(PinnedProxyAddress)};

    return CreateRootClientsCache({
        .PipelinePath = MakePipelinePath(),
        .ClientsCacheConfig = std::move(config),
        .ClientOptions = NApi::TClientOptions::FromUser("test-user"),
    });
}

constexpr TStringBuf StopMarker = "stopped by the test at cluster";

//! Asserts that the launcher asks for |expectedCluster|: that one is served from |underlying|,
//! any other stops the launch and names itself in the error. Stopping is also what keeps the
//! launcher from issuing a request, so these tests need no cluster to run against.
class TAssertClientsCache
    : public IClientsCache
{
public:
    TAssertClientsCache(TStringBuf expectedCluster, IClientsCachePtr underlying)
        : ExpectedCluster_(expectedCluster)
        , Underlying_(std::move(underlying))
    { }

    NApi::IClientPtr GetClient(TStringBuf clusterUrl) override
    {
        THROW_ERROR_EXCEPTION_IF(clusterUrl != ExpectedCluster_, "%v %Qv", StopMarker, clusterUrl);

        return Underlying_->GetClient(clusterUrl);
    }

private:
    const std::string ExpectedCluster_;
    const IClientsCachePtr Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TAssertClientsCache)

////////////////////////////////////////////////////////////////////////////////

// The companion is dialed on a fixed in-job port, just like rpc and monitoring: a companion
// pipeline must not have to request YT-allocated ports to run in a vanilla job.
TEST(TVanillaNodeConfigTest, CarriesCompanionPort)
{
    auto nodeConfig = BuildDefaultVanillaNodeConfig(
        MakePipelinePath(),
        /*proxyRole*/ std::nullopt,
        /*workerPortCount*/ std::nullopt);

    ASSERT_TRUE(nodeConfig->Companion);
    EXPECT_GT(nodeConfig->Companion->Port, 0);
}

// A worker on YT-allocated ports runs where fixed ones collide, so the fixed companion port
// would point at whatever neighbouring job took it. It is left out: with `port_count = 3` the
// port comes from YT_PORT_2, and with fewer the companion refuses to start.
TEST(TVanillaNodeConfigTest, OmitsCompanionPortForYtAllocatedPorts)
{
    auto nodeConfig = BuildDefaultVanillaNodeConfig(
        MakePipelinePath(),
        /*proxyRole*/ std::nullopt,
        /*workerPortCount*/ 2);

    EXPECT_FALSE(nodeConfig->Companion);
}

// The sandbox is discarded with the operation, so the job stderr — the one artifact YT retains
// for finished jobs — must carry the info-level pipeline history, not just crash traces.
TEST(TVanillaNodeConfigTest, MirrorsInfoLogToStderr)
{
    auto nodeConfig = BuildDefaultVanillaNodeConfig(
        MakePipelinePath(),
        /*proxyRole*/ std::nullopt,
        /*workerPortCount*/ std::nullopt);
    auto loggingConfig = nodeConfig->GetSingletonConfig<NLogging::TLogManagerConfig>();

    auto writersFor = [&] (TStringBuf category, NLogging::ELogLevel level) {
        std::set<std::string> writers;
        for (const auto& rule : loggingConfig->Rules) {
            if (rule->IsApplicable(category, level, NLogging::ELogFamily::PlainText)) {
                writers.insert(rule->Writers.begin(), rule->Writers.end());
            }
        }
        return writers;
    };

    EXPECT_EQ(writersFor("Worker", NLogging::ELogLevel::Info), (std::set<std::string>{"file", "stderr"}));
    // Chatty infrastructure categories: the info-level stream is dropped, errors still reach stderr.
    EXPECT_EQ(writersFor("Bus", NLogging::ELogLevel::Info), std::set<std::string>{});
    EXPECT_EQ(writersFor("Bus", NLogging::ELogLevel::Error), std::set<std::string>{"stderr"});
}

////////////////////////////////////////////////////////////////////////////////

// Ten, the scheduler default, would drop most jobs' stderrs; the runner defaults to the scheduler
// cap so every job's stderr survives the operation.
TEST(TVanillaConfigTest, RetainsStderrOfEveryJobByDefault)
{
    EXPECT_EQ(MakeVanillaConfig()->MaxStderrCount, DefaultMaxStderrCount);
    EXPECT_EQ(MakeVanillaConfig("max_stderr_count=7")->MaxStderrCount, 7);
}

TEST(TVanillaSpecTest, CarriesMaxStderrCount)
{
    auto spec = BuildVanillaOperationSpec(TVanillaSpec{.MaxStderrCount = 7});

    EXPECT_EQ(ConvertTo<int>(spec->GetChildOrThrow("max_stderr_count")), 7);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TVanillaLauncherClientsTest, TakesPipelineClientFromClientsCache)
{
    auto cache = New<TAssertClientsCache>(/*expectedCluster*/ TStringBuf(), MakeCacheWithPinnedProxy());

    // Serving nothing is enough to stop the launch, so the cache is the launcher's only way of
    // reaching the pipeline cluster — asked for under the runner's own role.
    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(MakePipelinePath(), /*proxyRole*/ "flow", MakeVanillaConfig(), cache),
        Format("%v \"pipeline-cluster/flow\"", StopMarker));
}

TEST(TVanillaLauncherClientsTest, TakesRuntimeClientFromClientsCacheUnderItsOwnRole)
{
    auto underlying = MakeCacheWithPinnedProxy();
    auto cache = New<TAssertClientsCache>(PipelineCluster, underlying);

    // Serving the pipeline cluster lets the launch reach the runtime one, which it asks for under
    // the role configured for it rather than the runner's own.
    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(
            MakePipelinePath(),
            /*proxyRole*/ {},
            MakeVanillaConfig("runtime_cluster=runtime-cluster; runtime_proxy_role=heavy"),
            cache),
        Format("%v \"runtime-cluster/heavy\"", StopMarker));

    // Those are two distinct keys in a cache that holds one client per cluster URL, so the runtime
    // cluster is reached with a client of its own rather than the pipeline's.
    EXPECT_NE(underlying->GetClient(PipelineCluster), underlying->GetClient("runtime-cluster/heavy"));
}

//! What the whole change is for: the launcher reaches the cluster over the connection the runner
//! configured. Building its own client from the cluster URL, as it used to, left proxy discovery
//! as the only way in — and a cluster whose advertised proxy address does not resolve on the
//! runner's host is then unreachable, however the runner is configured.
TEST(TVanillaLauncherClientsTest, PipelineClientUsesTheConfiguredConnection)
{
    auto underlying = MakeCacheWithPinnedProxy();
    auto cache = New<TAssertClientsCache>(PipelineCluster, underlying);

    // Stopped at the runtime cluster, i.e. only after the pipeline client had been handed over.
    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(
            MakePipelinePath(),
            /*proxyRole*/ {},
            MakeVanillaConfig("runtime_cluster=runtime-cluster"),
            cache),
        Format("%v \"runtime-cluster\"", StopMarker));

    // The cache holds one client per cluster URL, so this is the very client the launcher got.
    auto connectionConfig = ConvertTo<IMapNodePtr>(
        underlying->GetClient(PipelineCluster)->GetConnection()->GetConfigYson());
    EXPECT_FALSE(ConvertTo<bool>(connectionConfig->GetChildOrThrow("enable_proxy_discovery")));
    EXPECT_EQ(
        std::vector<std::string>{std::string(PinnedProxyAddress)},
        ConvertTo<std::vector<std::string>>(connectionConfig->GetChildOrThrow("proxy_addresses")));
}

////////////////////////////////////////////////////////////////////////////////

//! The secure vault is only assembled once the binary is uploaded, so a missing secret is reported
//! up front: the launch names the variable rather than stopping at the first cluster it asks for.
TEST(TVanillaLauncherSecretEnvTest, RejectsUnsetSecretEnvBeforeReachingCluster)
{
    auto cache = New<TAssertClientsCache>(/*expectedCluster*/ TStringBuf(), MakeCacheWithPinnedProxy());

    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(
            MakePipelinePath(),
            /*proxyRole*/ {},
            MakeVanillaConfig("secret_env=[FLOW_UT_UNSET_SECRET]"),
            cache),
        "FLOW_UT_UNSET_SECRET");
}

TEST(TVanillaLauncherSecretEnvTest, AcceptsSetSecretEnv)
{
    SetEnv("FLOW_UT_SECRET", "value");
    auto cache = New<TAssertClientsCache>(/*expectedCluster*/ TStringBuf(), MakeCacheWithPinnedProxy());

    // Stopped at the cluster, i.e. the declared secret let the launch through.
    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(
            MakePipelinePath(),
            /*proxyRole*/ {},
            MakeVanillaConfig("secret_env=[FLOW_UT_SECRET]"),
            cache),
        Format("%v \"pipeline-cluster\"", StopMarker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
