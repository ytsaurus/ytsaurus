#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/deploy_info.h>
#include <yt/yt/flow/library/cpp/misc/testing/env_guard.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using NTesting::TEnvGuard;

////////////////////////////////////////////////////////////////////////////////

TEST(TDeployInfoTest, Vanilla)
{
    TEnvGuard stageGuard("DEPLOY_STAGE_ID");
    TEnvGuard clusterGuard("YT_CLUSTER_NAME", "my-cluster");
    TEnvGuard operationGuard("YT_OPERATION_ID", "1-2-3-4");
    TEnvGuard jobGuard("YT_JOB_ID", "5-6-7-8");

    auto info = MakeDeployInfo();
    ASSERT_TRUE(info);
    EXPECT_EQ(info->Type, EDeployType::Vanilla);
    EXPECT_FALSE(info->YDeploy);
    ASSERT_TRUE(info->Vanilla);
    EXPECT_EQ(info->Vanilla->Cluster, "my-cluster");
    EXPECT_EQ(info->Vanilla->OperationId, "1-2-3-4");
    EXPECT_EQ(info->Vanilla->JobId, "5-6-7-8");
}

TEST(TDeployInfoTest, YDeploy)
{
    TEnvGuard stageGuard("DEPLOY_STAGE_ID", "my-stage");
    TEnvGuard operationGuard("YT_OPERATION_ID");

    auto info = MakeDeployInfo();
    ASSERT_TRUE(info);
    EXPECT_EQ(info->Type, EDeployType::Ydeploy);
    EXPECT_FALSE(info->Vanilla);
    ASSERT_TRUE(info->YDeploy);
    EXPECT_EQ(info->YDeploy->StageId, "my-stage");
    // No stage URL provider is installed in this build.
    EXPECT_EQ(info->YDeploy->StageUrl, "");
}

TEST(TDeployInfoTest, UnknownEnvironment)
{
    TEnvGuard stageGuard("DEPLOY_STAGE_ID");
    TEnvGuard operationGuard("YT_OPERATION_ID");

    EXPECT_FALSE(MakeDeployInfo());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
