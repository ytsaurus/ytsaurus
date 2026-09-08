#include <yt/yql/providers/ytflow/gateway/yql_ytflow_config_clusters.h>
#include <yt/yql/providers/ytflow/gateway/yql_ytflow_utils.h>

#include <yt/yql/providers/ytflow/provider/yql_ytflow_configuration.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <utility>

namespace NYql::NYtflow::NPrivate {
namespace {

::testing::AssertionResult DispatchSetting(
    TYtflowConfiguration& config,
    TString name,
    TString value)
{
    TString error;
    const bool dispatched = config.Dispatch(
        NCommon::ALL_CLUSTERS,
        name,
        TMaybe<TString>(std::move(value)),
        NCommon::TSettingDispatcher::EStage::STATIC,
        [&error](const TString& message, bool) {
            error = message;
            return false;
        });

    if (!dispatched) {
        return ::testing::AssertionFailure() << error;
    }

    return ::testing::AssertionSuccess();
}

TEST(TYtConsumerRichPath, UsesCanonicalPipelinePathAndRealCluster)
{
    auto config = MakeIntrusive<TYtflowConfiguration>();
    ASSERT_TRUE(DispatchSetting(*config, "Cluster", "primary"));
    ASSERT_TRUE(DispatchSetting(
        *config,
        "PipelinePath",
        "home/test/pipeline"));

    TYtflowGatewayConfig gatewayConfig;
    auto* clusterMapping = gatewayConfig.AddClusterMapping();
    clusterMapping->SetName("primary");
    clusterMapping->SetRealName("primary-cluster");
    clusterMapping->SetProxyUrl("primary-cluster.example.com");
    const TConfigClusters configClusters(gatewayConfig);

    const auto consumerPath = MakeYtConsumerRichPath(*config, configClusters);

    ASSERT_EQ(
        "//home/test/pipeline/yql_ytflow/consumers/default_consumer",
        consumerPath.GetPath());
    ASSERT_TRUE(consumerPath.GetCluster());
    ASSERT_EQ("primary-cluster", *consumerPath.GetCluster());
}

TEST(TYtConsumerRichPath, ResolvesExplicitConsumerCluster)
{
    auto config = MakeIntrusive<TYtflowConfiguration>();
    ASSERT_TRUE(DispatchSetting(*config, "Cluster", "primary"));
    ASSERT_TRUE(DispatchSetting(
        *config,
        "PipelinePath",
        "//home/test/pipeline"));
    ASSERT_TRUE(DispatchSetting(
        *config,
        "YtConsumerPath",
        "<cluster=\"remote\";>//home/test/custom-consumer"));

    TYtflowGatewayConfig gatewayConfig;
    auto* pipelineClusterMapping = gatewayConfig.AddClusterMapping();
    pipelineClusterMapping->SetName("primary");
    pipelineClusterMapping->SetRealName("primary-cluster");
    pipelineClusterMapping->SetProxyUrl("primary-cluster.example.com");
    auto* consumerClusterMapping = gatewayConfig.AddClusterMapping();
    consumerClusterMapping->SetName("remote");
    consumerClusterMapping->SetRealName("remote-cluster");
    consumerClusterMapping->SetProxyUrl("remote-cluster.example.com");
    const TConfigClusters configClusters(gatewayConfig);

    const auto consumerPath = MakeYtConsumerRichPath(*config, configClusters);

    ASSERT_EQ("//home/test/custom-consumer", consumerPath.GetPath());
    ASSERT_TRUE(consumerPath.GetCluster());
    ASSERT_EQ("remote-cluster", *consumerPath.GetCluster());
}

} // namespace
} // namespace NYql::NYtflow::NPrivate
