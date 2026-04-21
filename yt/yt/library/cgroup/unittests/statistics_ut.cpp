#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/cgroup/statistics.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NContainers::NCGroups {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

TEST(TSelfCGroupsStatisticsFetcherTest, Construction)
{
    EXPECT_NO_THROW(TSelfCGroupsStatisticsFetcher());
}

TEST(TSelfCGroupsStatisticsFetcherTest, MemoryStatistics)
{
    TSelfCGroupsStatisticsFetcher fetcher;
    auto stats = fetcher.GetMemoryStatistics();
    EXPECT_GE(stats.ResidentAnon, 0);
}

// TODO(pavook): Re-enable after bug fix on cgroups v1.
TEST(TSelfCGroupsStatisticsFetcherTest, DISABLED_CpuStatistics)
{
    TSelfCGroupsStatisticsFetcher fetcher;
    auto stats = fetcher.GetCpuStatistics();
    EXPECT_GE(stats.UserTime, TDuration::Zero());
    EXPECT_GE(stats.SystemTime, TDuration::Zero());
}

TEST(TSelfCGroupsStatisticsFetcherTest, BlockIOStatistics)
{
    TSelfCGroupsStatisticsFetcher fetcher;
    auto stats = fetcher.GetBlockIOStatistics();
    EXPECT_GE(stats.IOReadByte, 0);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NContainers::NCGroups
