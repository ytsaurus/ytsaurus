#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/companion/companion_manager.h>
#include <yt/yt/flow/library/cpp/companion/config.h>

#include <yt/yt/core/http/config.h>
#include <yt/yt/core/https/config.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/solomon/config.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionConfigTest, CompanionProcessCountDefaultsToAuto)
{
    auto config = New<TCompanionConfig>();
    EXPECT_EQ(0, config->CompanionProcessCount);
}

TEST(TCompanionConfigTest, CompanionProcessCountParses)
{
    auto yson = NYson::TYsonString(TStringBuf(R"({
        "companion_process_count" = 4;
    })"));
    auto config = NYTree::ConvertTo<TCompanionConfigPtr>(yson);
    EXPECT_EQ(4, config->CompanionProcessCount);
}

TEST(TCompanionConfigTest, HttpSettingsDefault)
{
    auto config = New<TCompanionConfig>();
    EXPECT_TRUE(config->HttpClientConfig);
    EXPECT_TRUE(config->HttpsClientConfig);
    EXPECT_EQ(1, config->HttpPollerThreads);
}

TEST(TCompanionConfigTest, HttpSettingsParse)
{
    auto yson = NYson::TYsonString(TStringBuf(R"({
        "http_poller_threads" = 3;
        "https_client_config" = {"allow_http" = %true};
    })"));
    auto config = NYTree::ConvertTo<TCompanionConfigPtr>(yson);
    EXPECT_EQ(3, config->HttpPollerThreads);
    EXPECT_TRUE(config->HttpsClientConfig->AllowHttp);
}

TEST(TCompanionConfigTest, NonPositiveHttpPollerThreadsThrows)
{
    auto yson = NYson::TYsonString(TStringBuf(R"({"http_poller_threads" = 0;})"));
    EXPECT_ANY_THROW(NYTree::ConvertTo<TCompanionConfigPtr>(yson));
}

TEST(TCompanionConfigTest, HttpsPrivateKeyMustUseFile)
{
    for (const auto* yson : {
            R"({"https_client_config" = {"credentials" = {"private_key" = {"value" = "secret";};};};})",
            R"({"https_client_config" = {"credentials" = {"private_key" = {"environment_variable" = "PRIVATE_KEY";};};};})"})
    {
        EXPECT_THROW_WITH_SUBSTRING(
            NYTree::ConvertTo<TCompanionConfigPtr>(NYson::TYsonString(TStringBuf(yson))),
            "must use \"file_name\"");
    }

    auto config = NYTree::ConvertTo<TCompanionConfigPtr>(NYson::TYsonString(TStringBuf(
        R"({"https_client_config" = {"credentials" = {"private_key" = {"file_name" = "client.key";};};};})")));
    ASSERT_TRUE(config->HttpsClientConfig->Credentials->PrivateKey->FileName);
    EXPECT_EQ(*config->HttpsClientConfig->Credentials->PrivateKey->FileName, "client.key");
}

TEST(TCompanionConfigTest, ExecutionConfigKeepsHttpSettings)
{
    auto userConfig = NYTree::ConvertTo<TCompanionConfigPtr>(NYson::TYsonString(TStringBuf(R"({
        "port" = 12345;
        "http_poller_threads" = 3;
        "https_client_config" = {"allow_http" = %true};
    })")));
    auto config = BuildCompanionExecutionConfig(userConfig, "cluster", "//tmp/pipeline");
    EXPECT_EQ(12345, config->Port);
    EXPECT_EQ(3, config->HttpPollerThreads);
    EXPECT_TRUE(config->HttpsClientConfig->AllowHttp);
    EXPECT_EQ("cluster", config->ClusterUrl);
}

TEST(TCompanionManagerParametersTest, JobReconciliationPeriodDefaultsAndRejectsNonPositive)
{
    auto params = New<TCompanionManagerParameters>();
    EXPECT_EQ(params->JobReconciliationPeriod, TDuration::Seconds(15));

    for (const auto* yson : {R"({"job_reconciliation_period" = 0;})", R"({"job_reconciliation_period" = -1;})"}) {
        EXPECT_ANY_THROW(NYTree::ConvertTo<TCompanionManagerParametersPtr>(NYson::TYsonString(TStringBuf(yson))));
    }
}

TEST(TCompanionExecutionConfigTest, InheritsSolomonExporterConfigFromTheNode)
{
    auto exporterConfig = New<NProfiling::TSolomonExporterConfig>();
    exporterConfig->GridStep = TDuration::Seconds(7);
    exporterConfig->LingerTimeout = TDuration::Minutes(7);
    exporterConfig->WindowSize = 23;
    exporterConfig->Host = "logical-host";
    exporterConfig->ConvertCountersToRateForSolomon = false;
    exporterConfig->RenameConvertedCounters = false;
    exporterConfig->ConvertCountersToDeltaGauge = true;
    exporterConfig->EnableSolomonAggregates = false;
    exporterConfig->InstanceTags["deployment"] = "blue";

    auto config = BuildCompanionExecutionConfig(
        New<TCompanionConfig>(),
        "test-cluster",
        "//tmp/pipeline",
        exporterConfig);

    EXPECT_EQ(config->Monitoring->GridStep, exporterConfig->GridStep);
    EXPECT_EQ(config->Monitoring->LingerTimeout, exporterConfig->LingerTimeout);
    EXPECT_EQ(config->Monitoring->WindowSize, exporterConfig->WindowSize);
    EXPECT_EQ(config->Monitoring->Host, exporterConfig->Host);
    EXPECT_FALSE(config->Monitoring->ConvertCountersToRateForSolomon);
    EXPECT_FALSE(config->Monitoring->RenameConvertedCounters);
    EXPECT_TRUE(config->Monitoring->ConvertCountersToDeltaGauge);
    EXPECT_FALSE(config->Monitoring->EnableSolomonAggregates);
    EXPECT_TRUE(config->Monitoring->Enable);
    EXPECT_EQ(config->Monitoring->InstanceTags, exporterConfig->InstanceTags);
}

// A disabled node exporter also disables companion export.
TEST(TCompanionExecutionConfigTest, InheritsDisabledExporterFromTheNode)
{
    auto exporterConfig = New<NProfiling::TSolomonExporterConfig>();
    exporterConfig->Enable = false;

    auto config = BuildCompanionExecutionConfig(
        New<TCompanionConfig>(),
        "test-cluster",
        "//tmp/pipeline",
        exporterConfig);

    EXPECT_FALSE(config->Monitoring->Enable);
}

TEST(TCompanionExecutionConfigTest, KeepsExporterDefaultWithoutANodeConfig)
{
    auto config = BuildCompanionExecutionConfig(
        New<TCompanionConfig>(),
        "test-cluster",
        "//tmp/pipeline");

    EXPECT_EQ(config->Monitoring->GridStep, TDuration::Seconds(5));
    EXPECT_EQ(config->Monitoring->LingerTimeout, TDuration::Minutes(5));
    EXPECT_EQ(config->Monitoring->WindowSize, 12);
    // Match the node default.
    EXPECT_TRUE(config->Monitoring->EnableSolomonAggregates);
    EXPECT_TRUE(config->Monitoring->Enable);
    EXPECT_TRUE(config->Monitoring->InstanceTags.empty());
}

// Support current and legacy wire configs.
TEST(TCompanionExecutionConfigTest, MonitoringBlockRoundTrips)
{
    auto exporterConfig = New<NProfiling::TSolomonExporterConfig>();
    exporterConfig->GridStep = TDuration::Seconds(7);
    exporterConfig->LingerTimeout = TDuration::Minutes(7);
    auto config = BuildCompanionExecutionConfig(
        New<TCompanionConfig>(),
        "test-cluster",
        "//tmp/pipeline",
        exporterConfig);

    auto restored = NYTree::ConvertTo<TCompanionExecutionConfigPtr>(NYson::ConvertToYsonString(config));
    EXPECT_EQ(
        NYson::ConvertToYsonString(restored->Monitoring),
        NYson::ConvertToYsonString(config->Monitoring));

    auto legacy = NYTree::ConvertTo<TCompanionExecutionConfigPtr>(
        NYson::TYsonString(TStringBuf(R"({"port" = 1; "cluster_url" = "c"; "pipeline_path" = "//p";})")));
    ASSERT_TRUE(legacy->Monitoring);
    EXPECT_EQ(legacy->Monitoring->GridStep, TDuration::Seconds(5));
    EXPECT_TRUE(legacy->Monitoring->EnableSolomonAggregates);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
