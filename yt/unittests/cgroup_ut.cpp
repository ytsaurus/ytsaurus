#include "stdafx.h"

#include "framework.h"

#include <ytlib/cgroup/cgroup.h>

#ifdef _linux_
  #include <sys/wait.h>
  #include <unistd.h>
#endif

namespace NYT {
namespace NCGroup {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(CGroup, CreateDestroy)
{
    for (int i = 0; i < 2; ++i) {
        TBlockIO group("some");
        group.Create();
        group.Destroy();
    }
}

TEST(CGroup, NotExistingGroupGetTasks)
{
    TBlockIO group("wierd_name");
    EXPECT_THROW(group.GetTasks(), std::exception);
}

TEST(CGroup, DoubleCreate)
{
    TBlockIO group("wierd_name");
    group.Create();
    EXPECT_THROW(group.Create(), std::exception);
    group.Destroy();
}

TEST(CGroup, EmptyHasNoTasks)
{
    TBlockIO group("some2");
    group.Create();
    auto tasks = group.GetTasks();
    EXPECT_EQ(0, tasks.size());
    group.Destroy();
}

#ifdef _linux_

TEST(CGroup, AddCurrentProcess)
{
    TBlockIO group("some");
    group.Create();

    auto pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        group.AddCurrentProcess();
        auto tasks = group.GetTasks();
        ASSERT_EQ(1, tasks.size());
        EXPECT_EQ(getpid(), tasks[0]);
        exit(0);
    }

    auto waitedpid = waitpid(pid, nullptr, 0);

    group.Destroy();

    ASSERT_EQ(pid, waitedpid);
}

TEST(CGroup, GetCpuAccStat)
{
    TCpuAccounting group("some");
    group.Create();

    auto stats = group.GetStats();
    EXPECT_EQ(0, stats.User.count());
    EXPECT_EQ(0, stats.System.count());

    group.Destroy();
}

TEST(CGroup, GetBlockIOStat)
{
    TBlockIO group("some");
    group.Create();

    auto stats = group.GetStats();
    EXPECT_EQ(0, stats.BytesRead);
    EXPECT_EQ(0, stats.BytesWritten);
    EXPECT_EQ(0, stats.Sectors);

    group.Destroy();
}

TEST(CurrentProcessCGroup, Empty)
{
    std::vector<char> empty;
    auto result = ParseCurrentProcessCGrops(empty.data(), empty.size());
    EXPECT_TRUE(result.empty());
}

TEST(CurrentProcessCGroup, Basic)
{
    auto basic = STRINGBUF("4:blkio:/\n3:cpuacct:/\n2:freezer:/some\n1:memory:/\n");
    auto result = ParseCurrentProcessCGrops(basic.data(), basic.length());
    EXPECT_EQ("", result["blkio"]);
    EXPECT_EQ("", result["cpuacct"]);
    EXPECT_EQ("some", result["freezer"]);
    EXPECT_EQ("", result["memory"]);
    EXPECT_EQ(4, result.size());
}

TEST(CurrentProcessCGroup, Multiple)
{
    auto basic = STRINGBUF("5:cpuacct,cpu,cpuset:/daemons\n");
    auto result = ParseCurrentProcessCGrops(basic.data(), basic.length());
    EXPECT_EQ("daemons", result["cpu"]);
    EXPECT_EQ("daemons", result["cpuset"]);
    EXPECT_EQ("daemons", result["cpuacct"]);
    EXPECT_EQ(3, result.size());
}

TEST(CurrentProcessCGroup, BadInput)
{
    auto basic = STRINGBUF("xxx:cpuacct,cpu,cpuset:/daemons\n");
    EXPECT_THROW(ParseCurrentProcessCGrops(basic.data(), basic.length()), std::exception);
}

#endif

////////////////////////////////////////////////////////////////////////////////

}
} // namespace NCGroup
} // namespace NYT
