#include "stdafx.h"

#include "framework.h"

#include <ytlib/cgroup/cgroup.h>

#include <core/misc/guid.h>
#include <core/misc/proc.h>
#include <core/misc/fs.h>

#ifdef _linux_
    #include <sys/eventfd.h>
    #include <sys/stat.h>
    #include <sys/wait.h>
    #include <unistd.h>
    #include <fcntl.h>
#endif

#include <array>

namespace NYT {
namespace NCGroup {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T CreateCGroup(const Stroka& name)
{
    T group(name);
    group.Create();
    return group;
}

////////////////////////////////////////////////////////////////////////////////


TEST(CGroup, CreateDestroy)
{
    for (int i = 0; i < 2; ++i) {
        auto group = CreateCGroup<TBlockIO>("create_destory_" + ToString(TGuid::Create()));
        group.Destroy();
    }
}

#ifdef _linux_

TEST(CGroup, NotExistingGroupGetTasks)
{
    TBlockIO group("not_existing_group_get_tasks" + ToString(TGuid::Create()));
    EXPECT_THROW(group.GetTasks(), std::exception);
}

TEST(CGroup, NotExistingRemoveAllSubcgroup)
{
    TBlockIO group("not_existing_remove_all_subcgroup" + ToString(TGuid::Create()));
    group.RemoveAllSubcgroups();
}

#endif

TEST(CGroup, DoubleCreate)
{
    TBlockIO group("double_create_" + ToString(TGuid::Create()));
    group.Create();
    group.Create();
    group.Destroy();
}

TEST(CGroup, EmptyHasNoTasks)
{
    auto group = CreateCGroup<TBlockIO>("empty_has_no_tasks_" + ToString(TGuid::Create()));
    auto tasks = group.GetTasks();
    EXPECT_EQ(0, tasks.size());
    group.Destroy();
}

#ifdef _linux_

TEST(CGroup, AddCurrentTask)
{
    auto group = CreateCGroup<TBlockIO>("add_current_task_" + ToString(TGuid::Create()));

    auto pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        group.AddCurrentTask();
        auto tasks = group.GetTasks();
        ASSERT_EQ(1, tasks.size());
        EXPECT_EQ(getpid(), tasks[0]);
        exit(0);
    }

    auto waitedpid = waitpid(pid, nullptr, 0);

    group.Destroy();

    ASSERT_EQ(pid, waitedpid);
}

TEST(CGroup, DISABLED_UnableToDestoryNotEmptyCGroup)
{
    auto group = CreateCGroup<TBlockIO>("unable_to_destroy_not_empty_cgroup_"
        + ToString(TGuid::Create()));

    auto addedEvent = eventfd(0, 0);
    auto triedRemoveEvent = eventfd(0, 0);

    auto pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        group.AddCurrentTask();

        i64 value = 1024;
        ASSERT_EQ(sizeof(value), ::write(addedEvent, &value, sizeof(value)));

        ASSERT_EQ(sizeof(value), ::read(triedRemoveEvent, &value, sizeof(value)));
        exit(0);
    }

    i64 value;
    ASSERT_EQ(sizeof(value), ::read(addedEvent, &value, sizeof(value)));
    ASSERT_DEATH(group.Destroy(), "Failed to destroy cgroup");

    value = 1;
    ASSERT_EQ(sizeof(value), ::write(triedRemoveEvent, &value, sizeof(value)));

    auto waitedpid = waitpid(pid, nullptr, 0);

    group.Destroy();
    ASSERT_EQ(pid, waitedpid);

    ASSERT_EQ(0, close(addedEvent));
    ASSERT_EQ(0, close(triedRemoveEvent));
}

TEST(CGroup, GetCpuAccStat)
{
    auto group = CreateCGroup<TCpuAccounting>("get_cpu_acc_stat_" + ToString(TGuid::Create()));

    auto stats = group.GetStatistics();
    EXPECT_EQ(0, stats.UserTime.MilliSeconds());
    EXPECT_EQ(0, stats.SystemTime.MilliSeconds());

    group.Destroy();
}

TEST(CGroup, GetBlockIOStat)
{
    auto group = CreateCGroup<TBlockIO>("get_block_io_stat_" + ToString(TGuid::Create()));

    auto stats = group.GetStatistics();
    EXPECT_EQ(0, stats.BytesRead);
    EXPECT_EQ(0, stats.BytesWritten);

    group.Destroy();
}

TEST(CGroup, GetMemoryStats)
{
    auto group = CreateCGroup<TMemory>("get_memory_stat_" + ToString(TGuid::Create()));

    auto stats = group.GetStatistics();
    EXPECT_EQ(0, stats.Rss);

    group.Destroy();
}

TEST(CGroup, UsageInBytesWithoutLimit)
{
    const i64 memoryUsage = 8 * 1024 * 1024;
    auto group = CreateCGroup<TMemory>("usage_in_bytes_without_limit_" + ToString(TGuid::Create()));

    i64 num = 1;
    auto exitBarier = ::eventfd(0, 0);
    EXPECT_TRUE(exitBarier > 0);
    auto initBarier = ::eventfd(0, 0);
    EXPECT_TRUE(initBarier > 0);

    auto pid = fork();
    if (pid == 0) {
        group.AddCurrentTask();
        volatile char* data = new char[memoryUsage];
        for (int i = 0; i < memoryUsage; ++i) {
            data[i] = 0;
        }

        YCHECK(::write(initBarier, &num, sizeof(num)) == sizeof(num));
        YCHECK(::read(exitBarier, &num, sizeof(num)) == sizeof(num));

        delete[] data;
        _exit(1);
    }

    EXPECT_TRUE(::read(initBarier, &num, sizeof(num)) == sizeof(num));

    auto statistics = group.GetStatistics();
    EXPECT_TRUE(statistics.Rss >= memoryUsage);

    EXPECT_TRUE(::write(exitBarier, &num, sizeof(num)) == sizeof(num));

    EXPECT_EQ(pid, waitpid(pid, nullptr, 0));
}

// This test proves that there is a bug in memory cgroup
TEST(CGroup, Bug)
{
    char buffer[1024];

    auto group = CreateCGroup<TMemory>("bug_" + ToString(TGuid::Create()));

    group.ForceEmpty();
    int iterations = 0;
    while (group.GetStatistics().Rss != 0) {
        sleep(1);
        ++iterations;
        if (iterations > 10) {
            ASSERT_TRUE(false);
        }
    }

    i64 num = 1;
    auto exitBarier = ::eventfd(0, 0);
    auto initBarier = ::eventfd(0, 0);

    const auto filename = NFS::CombinePaths(group.GetFullPath(), "memory.usage_in_bytes");
    int fd = ::open(~filename, 0);
    int reallyRead = ::read(fd, buffer, 1024);

    ASSERT_TRUE(reallyRead > 0);
    EXPECT_STREQ(~Stroka(buffer, reallyRead), "0\n");

    auto pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        group.AddCurrentTask();

        auto otherPid = fork();
        if (otherPid < 0) {
            YCHECK(::write(initBarier, &num, sizeof(num)) == sizeof(num));
            _exit(3);
        }

        if (otherPid == 0) {
            num = 1;
            YCHECK(::write(initBarier, &num, sizeof(num)) == sizeof(num));
            YCHECK(::read(exitBarier, &num, sizeof(num)) == sizeof(num));
            _exit(2);
        }
        waitpid(otherPid, NULL, 0);
        _exit(1);
    }

    EXPECT_TRUE(::read(initBarier, &num, sizeof(num)) == sizeof(num));

    reallyRead = ::read(fd, buffer, 1024);
    // reallyRead SHOULD BE equal to 0
    ASSERT_TRUE(reallyRead > 0);

    num = 1;
    EXPECT_TRUE(::write(exitBarier, &num, sizeof(num)) == sizeof(num));

    auto waitedpid = waitpid(pid, NULL, 0);
    EXPECT_TRUE(waitedpid == pid);
}

TEST(CGroup, RemoveAllSubcgroupsAfterLock)
{
    auto parentName = "remove_all_subcgroups_after_lock_" + ToString(TGuid::Create());
    auto parent = CreateCGroup<TFreezer>(parentName);
    TNonOwningCGroup child(parent.GetFullPath() + "/child");

    child.EnsureExistance();
    parent.Lock();
    parent.RemoveAllSubcgroups();
}

TEST(CGroup, FreezerEmpty)
{
    auto group = CreateCGroup<TFreezer>("freezer_empty_" + ToString(TGuid::Create()));

    EXPECT_EQ("THAWED", group.GetState());
}

TEST(CGroup, FreezerFreeze)
{
    auto group = CreateCGroup<TFreezer>("freezer_freeze_" + ToString(TGuid::Create()));

    auto addedEvent = eventfd(0, 0);
    auto exitEvent = eventfd(0, 0);

    auto pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        group.AddCurrentTask();

        i64 value = 1024;
        ASSERT_EQ(sizeof(value), ::write(addedEvent, &value, sizeof(value)));

        ASSERT_EQ(sizeof(value), ::read(exitEvent, &value, sizeof(value)));
        exit(0);
    }

    i64 value;
    ASSERT_EQ(sizeof(value), ::read(addedEvent, &value, sizeof(value)));
    // test here something

    group.Freeze();
    auto state = group.GetState();
    EXPECT_TRUE((state == "FREEZING") || (state == "FROZEN"));

    value = 1;
    ASSERT_EQ(sizeof(value), ::write(exitEvent, &value, sizeof(value)));

    group.Unfreeze();

    auto waitedpid = waitpid(pid, nullptr, 0);

    group.Destroy();
    ASSERT_EQ(pid, waitedpid);
}

TEST(ProcessCGroup, Empty)
{
    Stroka empty;
    auto result = ParseProcessCGroups(empty);
    EXPECT_TRUE(result.empty());
}

TEST(ProcessCGroup, Basic)
{
    Stroka basic("4:blkio:/\n3:cpuacct:/\n2:freezer:/some\n1:memory:/\n");
    auto result = ParseProcessCGroups(basic);
    EXPECT_EQ("", result["blkio"]);
    EXPECT_EQ("", result["cpuacct"]);
    EXPECT_EQ("some", result["freezer"]);
    EXPECT_EQ("", result["memory"]);
    EXPECT_EQ(4, result.size());
}

TEST(ProcessCGroup, Multiple)
{
    auto basic("5:cpuacct,cpu,cpuset:/daemons\n");
    auto result = ParseProcessCGroups(basic);
    EXPECT_EQ("daemons", result["cpu"]);
    EXPECT_EQ("daemons", result["cpuset"]);
    EXPECT_EQ("daemons", result["cpuacct"]);
    EXPECT_EQ(3, result.size());
}

TEST(ProcessCGroup, BadInput)
{
    Stroka basic("xxx:cpuacct,cpu,cpuset:/daemons\n");
    EXPECT_THROW(ParseProcessCGroups(basic), std::exception);
}

#endif // _linux_

////////////////////////////////////////////////////////////////////////////////

}
} // namespace NCGroup
} // namespace NYT
