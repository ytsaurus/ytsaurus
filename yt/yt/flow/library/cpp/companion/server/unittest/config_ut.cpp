#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/config.h>

#include <yt/yt/flow/library/cpp/companion/server/unittest/env_guard.h>

#include <util/system/env.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TCompanionEnvConfigTest = NTesting::TCompanionEnvGuardTest;

TEST_F(TCompanionEnvConfigTest, ParsesFullConfig)
{
    SetEnv("YT_FLOW_MODE", "Worker");
    SetEnv(
        "YT_FLOW_COMPANION_CONFIG",
        R"({port=12345;cluster_url="localhost:1234";pipeline_path="//tmp/pipeline"})");

    auto config = LoadCompanionExecutionConfigFromEnv();
    EXPECT_EQ(config->Port, 12345);
    EXPECT_EQ(config->CompanionProcessCount, 0);
    EXPECT_EQ(config->ClusterUrl, "localhost:1234");
    EXPECT_EQ(config->PipelinePath, "//tmp/pipeline");
}

TEST_F(TCompanionEnvConfigTest, MissingModeThrows)
{
    SetEnv("YT_FLOW_MODE", "");
    SetEnv("YT_FLOW_COMPANION_CONFIG", "{port=12345}");
    EXPECT_THROW_WITH_SUBSTRING(
        LoadCompanionExecutionConfigFromEnv(),
        "YT_FLOW_MODE");
}

TEST_F(TCompanionEnvConfigTest, NonWorkerModeThrows)
{
    SetEnv("YT_FLOW_MODE", "Controller");
    SetEnv("YT_FLOW_COMPANION_CONFIG", "{port=12345}");
    EXPECT_THROW_WITH_SUBSTRING(
        LoadCompanionExecutionConfigFromEnv(),
        "non-worker mode");
}

TEST_F(TCompanionEnvConfigTest, MissingConfigThrows)
{
    SetEnv("YT_FLOW_MODE", "Worker");
    SetEnv("YT_FLOW_COMPANION_CONFIG", "");
    EXPECT_THROW_WITH_SUBSTRING(
        LoadCompanionExecutionConfigFromEnv(),
        "YT_FLOW_COMPANION_CONFIG");
}

TEST_F(TCompanionEnvConfigTest, ZeroPortThrows)
{
    SetEnv("YT_FLOW_MODE", "Worker");
    SetEnv("YT_FLOW_COMPANION_CONFIG", "{}");
    EXPECT_THROW_WITH_SUBSTRING(
        LoadCompanionExecutionConfigFromEnv(),
        "positive port");
}

TEST_F(TCompanionEnvConfigTest, ExplicitSingleProcessAllowed)
{
    SetEnv("YT_FLOW_MODE", "Worker");
    SetEnv("YT_FLOW_COMPANION_CONFIG", "{port=12345;companion_process_count=1}");
    EXPECT_EQ(LoadCompanionExecutionConfigFromEnv()->CompanionProcessCount, 1);
}

TEST_F(TCompanionEnvConfigTest, PreForkCountThrows)
{
    SetEnv("YT_FLOW_MODE", "Worker");
    SetEnv("YT_FLOW_COMPANION_CONFIG", "{port=12345;companion_process_count=4}");
    EXPECT_THROW_WITH_SUBSTRING(
        LoadCompanionExecutionConfigFromEnv(),
        "companion_process_count");
}

TEST_F(TCompanionEnvConfigTest, MalformedYsonThrows)
{
    SetEnv("YT_FLOW_MODE", "Worker");
    SetEnv("YT_FLOW_COMPANION_CONFIG", "{port=");
    EXPECT_THROW_WITH_SUBSTRING(
        LoadCompanionExecutionConfigFromEnv(),
        "Failed to parse YT_FLOW_COMPANION_CONFIG");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
