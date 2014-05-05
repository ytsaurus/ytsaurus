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
        TBlockIO group("", "some");
        group.Create();
        group.Destroy();
    }
}

TEST(CGroup, NotExistingGroupGetTasks)
{
    TBlockIO group("", "wierd_name");
    EXPECT_THROW(group.GetTasks(), std::exception);
}

TEST(CGroup, DoubleCreate)
{
    TBlockIO group("", "wierd_name");
    group.Create();
    EXPECT_THROW(group.Create(), std::exception);
    group.Destroy();
}

TEST(CGroup, EmptyHasNoTasks)
{
    TBlockIO group("", "some2");
    group.Create();
    auto tasks = group.GetTasks();
    EXPECT_EQ(0, tasks.size());
    group.Destroy();
}

#ifdef _linux_

TEST(CGroup, AddCurrentProcess)
{
    TBlockIO group("", "some");
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
    TCpuAccounting group("", "some");
    group.Create();

    auto stats = group.GetStats();
    EXPECT_EQ(0, stats.User.count());
    EXPECT_EQ(0, stats.System.count());

    group.Destroy();
}

TEST(CGroup, GetBlockIOStat)
{
    TBlockIO group("", "some");
    group.Create();

    auto stats = group.GetStats();
    EXPECT_EQ(0, stats.BytesRead);
    EXPECT_EQ(0, stats.BytesWritten);
    EXPECT_EQ(0, stats.Sectors);

    group.Destroy();
}

#endif

////////////////////////////////////////////////////////////////////////////////

}
} // namespace NCGroup
} // namespace NYT
