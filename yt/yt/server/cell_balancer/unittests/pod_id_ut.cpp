#include <yt/yt/server/cell_balancer/pod_id_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NCellBalancer {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSchedulerUtilsTest, CheckGetIndexFromPodId)
{
    static const std::string Cluster = "hume";
    static const std::string InstanceType = "tab";
    static const std::string Bundle = "venus212";

    EXPECT_EQ(1, FindNextInstanceId({}, Cluster, InstanceType));
    EXPECT_EQ(1, FindNextInstanceId({"sas4-5335-venus212-0aa-tab-hume"}, Cluster, InstanceType));
    EXPECT_EQ(1, FindNextInstanceId({"sas4-5335-venus212-000-tab-hume"}, Cluster, InstanceType));
    EXPECT_EQ(2, FindNextInstanceId({"sas4-5335-venus212-001-tab-hume", "trash"}, Cluster, InstanceType));

    EXPECT_EQ(4, FindNextInstanceId(
        {
            "sas4-5335-venus212-001-tab-hume",
            "sas4-5335-venus212-002-tab-hume",
            "sas4-5335-venus212-002-tab-hume",
            "sas4-5335-venus212-002-tab-hume",
            "sas4-5335-venus212-003-tab-hume",
            "sas4-5335-venus212-005-tab-hume",
        },
        Cluster,
        InstanceType));

    EXPECT_EQ(6, FindNextInstanceId(
        {
            "sas4-5335-venus212-001-tab-hume",
            GetInstancePodIdTemplate(Cluster, Bundle, InstanceType, 2),
            GetInstancePodIdTemplate(Cluster, Bundle, InstanceType, 3),
            GetInstancePodIdTemplate(Cluster, Bundle, InstanceType, 4),
            "sas4-5335-venus212-005-tab-hume",
            GetInstancePodIdTemplate(Cluster, Bundle, InstanceType, 7),
        },
        Cluster,
        InstanceType));
}

TEST(TSchedulerUtilsTest, CheckGeneratePodTemplate)
{
    EXPECT_EQ("<short-hostname>-venus212-0ab-exe-shtern", GetInstancePodIdTemplate("shtern", "venus212", "exe", 171));
    EXPECT_EQ("<short-hostname>-venus212-2710-exe-shtern", GetInstancePodIdTemplate("shtern", "venus212", "exe", 10000));
}

TEST(TSchedulerUtilsTest, GetIndexFromPodId)
{
    static const std::string Cluster = "clstr";
    static const std::string InstanceType = "tab";
    static const std::string Bundle = "test-bundle";

    EXPECT_EQ(0x555, GetIndexFromPodId("sas4-5335-test-bundle-555-tab-clstr", Cluster, InstanceType));
    EXPECT_EQ(0x123, GetIndexFromPodId("test-bundle-123-tab-clstr-8a3b", Cluster, InstanceType));
    EXPECT_EQ(0x6789, GetIndexFromPodId("test-bundle-6789-tab-clstr-8a3b", Cluster, InstanceType));
    EXPECT_EQ(std::nullopt, GetIndexFromPodId("test-bundle-123-tab-clstr-something-wrong", Cluster, InstanceType));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCellBalancer
