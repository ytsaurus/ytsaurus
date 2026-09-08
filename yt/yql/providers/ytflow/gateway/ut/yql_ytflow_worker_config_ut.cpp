#include <yt/yql/providers/ytflow/gateway/yql_ytflow_config_clusters.h>
#include <yt/yql/providers/ytflow/gateway/yql_ytflow_utils.h>
#include <yt/yql/providers/ytflow/gateway/yql_ytflow_worker_config.h>

#include <yt/yql/providers/ytflow/provider/yql_ytflow_configuration.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/yson/node/node_io.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/flow/library/cpp/common/spec.h>


namespace NYql::NYtflow::NPrivate {
namespace {

NYT::NFlow::TDynamicPipelineSpecPtr ParseDynamicPipelineSpec(
    const TMaybe<NYT::TNode>& dynamicPipelineSpec)
{
    auto node = dynamicPipelineSpec
        ? *dynamicPipelineSpec
        : NYT::TNode::CreateMap();

    return NYT::NYTree::ConvertTo<NYT::NFlow::TDynamicPipelineSpecPtr>(
        NYT::NYson::TYsonString(NYT::NodeToYsonString(node)));
}

void AssertBalancerType(
    TMaybe<bool> useCpuAwareBalancer,
    NYT::NFlow::EJobBalancerType expectedBalancerType)
{
    auto dynamicPipelineSpecNode = SerializeUseCpuAwareBalancer(useCpuAwareBalancer);

    if (dynamicPipelineSpecNode) {
        const auto& jobManager = (*dynamicPipelineSpecNode)["job_manager"];
        ASSERT_TRUE(jobManager.HasKey("use_cpu_aware_balancer"));
        ASSERT_EQ(
            *useCpuAwareBalancer,
            jobManager["use_cpu_aware_balancer"].AsBool());

        // YQL intentionally exposes only this boolean. Other Flow balancers,
        // such as ResourceQueue, require an explicit compatibility decision.
        ASSERT_FALSE(jobManager.HasKey("balancer_type"));
    }

    auto dynamicPipelineSpec = ParseDynamicPipelineSpec(dynamicPipelineSpecNode);
    ASSERT_EQ(
        static_cast<int>(expectedBalancerType),
        static_cast<int>(dynamicPipelineSpec->JobManager->BalancerType));
}

void SetConfigSetting(
    TYtflowConfiguration& config,
    const TString& name,
    const TString& value)
{
    TString error;
    const bool dispatched = config.Dispatch(
        NCommon::ALL_CLUSTERS,
        name,
        value,
        NCommon::TSettingDispatcher::EStage::STATIC,
        [&] (const TString& message, bool) {
            error = message;
            return false;
        });
    ASSERT_TRUE(dispatched) << error;
}

NYT::NYTree::IMapNodePtr ConvertDescriptionToMap(const NYT::TNode& description)
{
    return NYT::NYTree::ConvertTo<NYT::NYTree::IMapNodePtr>(
        NYT::NYson::TYsonString(NYT::NodeToYsonString(description)));
}

} // anonymous namespace

TEST(TWorkerConfigBalancerInvariant, TrueMeansCpuAware)
{
    AssertBalancerType(true, NYT::NFlow::EJobBalancerType::CpuAware);
}

TEST(TWorkerConfigBalancerInvariant, FalseMeansGreedy)
{
    AssertBalancerType(false, NYT::NFlow::EJobBalancerType::Greedy);
}

TEST(TWorkerConfigBalancerInvariant, AbsentMeansCpuAwareByDefault)
{
    ASSERT_FALSE(SerializeUseCpuAwareBalancer({}));

    AssertBalancerType({}, NYT::NFlow::EJobBalancerType::CpuAware);
}

TEST(TPreviousOperationDiscoveryTest, MatchesCanonicalPipelineIdentity)
{
    auto config = MakeIntrusive<TYtflowConfiguration>();
    SetConfigSetting(*config, "Cluster", "control-alias");
    SetConfigSetting(*config, "PipelinePath", "pipelines/test");
    SetConfigSetting(*config, "PathPrefix", "//prefix/");

    TYtflowGatewayConfig gatewayConfig;
    auto* clusterConfig = gatewayConfig.AddClusterMapping();
    clusterConfig->SetName("control-alias");
    clusterConfig->SetRealName("control-real-name");
    TConfigClusters configClusters(gatewayConfig);

    TYqlOperationOptions operationOptions;
    auto description = MakeOperationDescription(
        operationOptions,
        *config,
        configClusters);

    ASSERT_EQ("//prefix/pipelines/test", description["yql_pipeline_path"].AsString());
    ASSERT_EQ("control-real-name", description["yql_pipeline_cluster"].AsString());

    auto descriptionMap = ConvertDescriptionToMap(description);
    EXPECT_TRUE(DoesOperationDescriptionMatchPipeline(
        descriptionMap,
        *config,
        configClusters));

    description["yql_pipeline_path"] = "//prefix/pipelines/other";
    EXPECT_FALSE(DoesOperationDescriptionMatchPipeline(
        ConvertDescriptionToMap(description),
        *config,
        configClusters));

    description["yql_pipeline_path"] = "//prefix/pipelines/test";
    description["yql_pipeline_cluster"] = "another-control-cluster";
    EXPECT_FALSE(DoesOperationDescriptionMatchPipeline(
        ConvertDescriptionToMap(description),
        *config,
        configClusters));

    description["yql_pipeline_cluster"] = "control-real-name";
    description["yql_pipeline_path"] = "pipelines/test";
    EXPECT_TRUE(DoesOperationDescriptionMatchPipeline(
        ConvertDescriptionToMap(description),
        *config,
        configClusters));
}

} // namespace NYql::NYtflow::NPrivate
