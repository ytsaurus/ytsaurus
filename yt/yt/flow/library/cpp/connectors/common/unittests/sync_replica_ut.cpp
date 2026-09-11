#include <yt/yt/flow/library/cpp/connectors/common/sync_replica.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

IMapNodePtr ParseReplicas(TStringBuf yson)
{
    return ConvertToNode(TYsonString(yson))->AsMap();
}

TEST(TFindEnabledSyncReplicaTest, PicksEnabledSync)
{
    auto replicas = ParseReplicas(R"({
        r1 = {
            mode = "async";
            state = "enabled";
            cluster_name = "other";
            replica_path = "//tmp/async";
        };
        r2 = {
            mode = "sync";
            state = "enabled";
            cluster_name = "primary";
            replica_path = "//tmp/sync";
        };
    })");

    auto replica = FindEnabledSyncReplica(replicas, "//tmp/table");
    EXPECT_EQ(replica.ClusterName, "primary");
    EXPECT_EQ(replica.Path, "//tmp/sync");
}

TEST(TFindEnabledSyncReplicaTest, SkipsDisabledSync)
{
    auto replicas = ParseReplicas(R"({
        r1 = {
            mode = "sync";
            state = "disabled";
            cluster_name = "primary";
            replica_path = "//tmp/disabled";
        };
    })");

    EXPECT_THROW_WITH_SUBSTRING(
        FindEnabledSyncReplica(replicas, "//tmp/table"),
        "No enabled synchronous replica");
}

TEST(TFindEnabledSyncReplicaTest, FiltersContentType)
{
    auto replicas = ParseReplicas(R"({
        r1 = {
            content_type = "queue";
            mode = "sync";
            state = "enabled";
            cluster_name = "primary";
            replica_path = "//tmp/queue";
        };
        r2 = {
            content_type = "data";
            mode = "sync";
            state = "enabled";
            cluster_name = "remote";
            replica_path = "//tmp/data";
        };
    })");

    auto replica = FindEnabledSyncReplica(replicas, "//tmp/crt", "data");
    EXPECT_EQ(replica.ClusterName, "remote");
    EXPECT_EQ(replica.Path, "//tmp/data");
}

TEST(TFindEnabledReplicaTest, PicksDeterministicallyRegardlessOfMode)
{
    auto replicas = ParseReplicas(R"({
        z_sync = {
            content_type = "data";
            mode = "sync";
            state = "enabled";
            cluster_name = "primary";
            replica_path = "//tmp/sync";
        };
        a_async = {
            content_type = "data";
            mode = "async";
            state = "enabled";
            cluster_name = "remote";
            replica_path = "//tmp/async";
        };
    })");

    auto replica = FindEnabledReplica(replicas, "//tmp/crt", "data");
    EXPECT_EQ(replica.ClusterName, "remote");
    EXPECT_EQ(replica.Path, "//tmp/async");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
