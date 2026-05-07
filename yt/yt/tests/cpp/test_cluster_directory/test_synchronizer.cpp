#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/hive/cluster_directory.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

using TClusterDirectorySynchronizerTest = TApiTestBase;

TEST_F(TClusterDirectorySynchronizerTest, TestClusterConnectionMisconfiguration)
{
    auto connection = DynamicPointerCast<NNative::IConnection>(Connection_);
    ASSERT_TRUE(connection);
    auto clusterDirectorySynchronizer = connection->GetClusterDirectorySynchronizer();

    {
        auto syncFuture = WaitFor(clusterDirectorySynchronizer->Sync(/*force*/ true));
        EXPECT_NO_THROW(syncFuture.ThrowOnError());
    }

    {
        auto trySyncFuture = WaitFor(clusterDirectorySynchronizer->TrySync(/*force*/ true));
        TClusterDirectoryUpdateResult syncResult;
        EXPECT_NO_THROW(syncResult = trySyncFuture.ValueOrThrow());

        const auto& errors = syncResult.ClusterToErrorMapping;

        ASSERT_EQ(std::ssize(errors), 1);
        ASSERT_TRUE(errors.contains("primary"));

        EXPECT_NO_THROW(errors.at("primary").ThrowOnError());
    }

    {
        auto setNodeFuture = WaitFor(Client_->SetNode(
            "//sys/clusters/misconfigured_cluster",
            // Intentionally misconfigured cluster connection configuration.
            BuildYsonStringFluently()
                .BeginMap()
                .Item("queue_agent").Value(67)
                .EndMap()));
        EXPECT_NO_THROW(setNodeFuture.ThrowOnError());
    }

    {
        auto syncFuture = WaitFor(clusterDirectorySynchronizer->Sync(/*force*/ true));
        EXPECT_THROW_WITH_SUBSTRING(syncFuture.ThrowOnError(), "Error creating connection to cluster \"misconfigured_cluster\"");
    }

    {
        auto trySyncFuture = WaitFor(clusterDirectorySynchronizer->TrySync(/*force*/ true));
        TClusterDirectoryUpdateResult syncResult;
        EXPECT_NO_THROW(syncResult = trySyncFuture.ValueOrThrow());

        const auto& errors = syncResult.ClusterToErrorMapping;

        ASSERT_EQ(std::ssize(errors), 2);
        ASSERT_TRUE(errors.contains("primary"));
        ASSERT_TRUE(errors.contains("misconfigured_cluster"));

        EXPECT_NO_THROW(errors.at("primary").ThrowOnError());
        EXPECT_THROW_WITH_SUBSTRING(errors.at("misconfigured_cluster").ThrowOnError(), "Error creating connection to cluster \"misconfigured_cluster\"");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
