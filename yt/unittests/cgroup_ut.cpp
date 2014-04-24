#include "stdafx.h"

#include "framework.h"

#include <ytlib/cgroup/cgroup.h>

#ifndef _win_
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
        TCGroup group("/sys/fs/cgroup/blkio", "some");
        group.Create();
        group.Destroy();
    }
}

TEST(CGroup, NotExistingGroupGetTasks)
{
    TCGroup group("/sys/fs/cgroup/blkio", "wierd_name");
    EXPECT_THROW(group.GetTasks(), std::exception);
}

TEST(CGroup, DoubleCreate)
{
    TCGroup group("/sys/fs/cgroup/blkio", "wierd_name");
    group.Create();
    EXPECT_THROW(group.Create(), std::exception);
    group.Destroy();
}

TEST(CGroup, EmptyHasNoTasks)
{
    TCGroup group("/sys/fs/cgroup/blkio", "some2");
    group.Create();
    auto tasks = group.GetTasks();
    EXPECT_EQ(0, tasks.size());
    group.Destroy();
}

#ifndef _win_

TEST(CGroup, AddMyself)
{
    TCGroup group("/sys/fs/cgroup/blkio", "some");
    group.Create();

    auto pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        group.AddMyself();
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
    TCGroup group("/sys/fs/cgroup/cpuacct", "some");
    group.Create();

    auto stats = GetCpuAccStat(group.GetFullName());
    EXPECT_EQ(0, stats.user.count());
    EXPECT_EQ(0, stats.system.count());

    group.Destroy();
}

TEST(CGroup, GetBlockIOStat)
{
    TCGroup group("/sys/fs/cgroup/blkio", "some");
    group.Create();

    auto stats = GetBlockIOStat(group.GetFullName());
    EXPECT_EQ(0, stats.ReadBytes);
    EXPECT_EQ(0, stats.WriteBytes);
    EXPECT_EQ(0, stats.Sectors);

    group.Destroy();
}

#endif

////////////////////////////////////////////////////////////////////////////////

}
} // namespace NCGroup
} // namespace NYT
