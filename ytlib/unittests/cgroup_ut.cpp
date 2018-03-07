#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/guid.h>
#include <yt/core/misc/proc.h>

#include <util/string/vector.h>

#include <util/system/user.h>

#ifdef _linux_
    #include <sys/eventfd.h>
    #include <sys/stat.h>
    #include <sys/wait.h>
    #include <unistd.h>
    #include <fcntl.h>
    #include <sys/utsname.h>
    #include <linux/version.h>
#endif

#include <array>

namespace NYT {
namespace NCGroup {
namespace {

////////////////////////////////////////////////////////////////////////////////

static TString CGroupPrefix = GetUsername() + "/yt/unittests/";

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T CreateCGroup(const TString& name)
{
    T group(CGroupPrefix + name);
    group.Create();
    return group;
}

////////////////////////////////////////////////////////////////////////////////


TEST(TCGroup, CreateDestroy)
{
    for (int i = 0; i < 2; ++i) {
        auto group = CreateCGroup<TBlockIO>("create_destory_" + ToString(TGuid::Create()));
        group.Destroy();
    }
}

#ifdef _linux_

int GetLinuxVersion()
{
    utsname sysInfo;

    Y_VERIFY(!uname(&sysInfo), "Error while call uname: %s", LastSystemErrorText());

    TStringBuf release(sysInfo.release);
    release = release.substr(0, release.find_first_not_of(".0123456789"));

    int v1 = FromString<int>(release.NextTok('.'));
    int v2 = FromString<int>(release.NextTok('.'));
    int v3 = FromString<int>(release.NextTok('.'));
    return KERNEL_VERSION(v1, v2, v3);
}

TEST(TCGroup, NotExistingGroupGetTasks)
{
    TBlockIO group(CGroupPrefix + "not_existing_group_get_tasks" + ToString(TGuid::Create()));
    EXPECT_THROW(group.GetTasks(), std::exception);
}

TEST(TCGroup, NotExistingRemoveAllSubcgroup)
{
    TBlockIO group(CGroupPrefix + "not_existing_remove_all_subcgroup" + ToString(TGuid::Create()));
    group.RemoveAllSubcgroups();
}

#endif

TEST(TCGroup, DoubleCreate)
{
    TBlockIO group(CGroupPrefix + "double_create_" + ToString(TGuid::Create()));
    group.Create();
    group.Create();
    group.Destroy();
}

TEST(TCGroup, EmptyHasNoTasks)
{
    auto group = CreateCGroup<TBlockIO>("empty_has_no_tasks_" + ToString(TGuid::Create()));
    auto tasks = group.GetTasks();
    EXPECT_EQ(0, tasks.size());
    group.Destroy();
}

#ifdef _linux_

TEST(TCGroup, AddCurrentTask)
{
    auto group = CreateCGroup<TBlockIO>("add_current_task_" + ToString(TGuid::Create()));

    auto pid = fork();
    ASSERT_TRUE(pid >= 0);

    if (pid == 0) {
        group.AddCurrentTask();
        auto tasks = group.GetTasks();
        ASSERT_EQ(1, tasks.size());
        EXPECT_EQ(getpid(), tasks[0]);
        _exit(0);
    }

    auto waitedpid = waitpid(pid, nullptr, 0);

    group.Destroy();

    ASSERT_EQ(pid, waitedpid);
}

TEST(TCGroup, DISABLED_UnableToDestoryNotEmptyCGroup)
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
        _exit(0);
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

TEST(TCGroup, GetCpuAccStat)
{
    auto group = CreateCGroup<TCpuAccounting>("get_cpu_acc_stat_" + ToString(TGuid::Create()));

    auto stats = group.GetStatistics();
    EXPECT_EQ(0, stats.UserTime.MilliSeconds());
    EXPECT_EQ(0, stats.SystemTime.MilliSeconds());

    group.Destroy();
}

TEST(TCGroup, GetBlockIOStat)
{
    auto group = CreateCGroup<TBlockIO>("get_block_io_stat_" + ToString(TGuid::Create()));

    auto stats = group.GetStatistics();
    EXPECT_EQ(0, stats.BytesRead);
    EXPECT_EQ(0, stats.BytesWritten);

    group.Destroy();
}

TEST(TCGroup, GetMemoryStats)
{
    auto group = CreateCGroup<TMemory>("get_memory_stat_" + ToString(TGuid::Create()));

    auto stats = group.GetStatistics();
    EXPECT_EQ(0, stats.Rss);

    group.Destroy();
}

TEST(TCGroup, UsageInBytesWithoutLimit)
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
TEST(TCGroup, Bug)
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
    EXPECT_STREQ(~TString(buffer, reallyRead), "0\n");

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
        waitpid(otherPid, nullptr, 0);
        _exit(1);
    }

    EXPECT_TRUE(::read(initBarier, &num, sizeof(num)) == sizeof(num));

    reallyRead = ::read(fd, buffer, 1024);
    // XXX(ignat): I failed to determine exact version where this problem was fixed.
    // It is certanly between 3.10 and 3.18, but there are too many changes about cgroups in these versions.
    if (GetLinuxVersion() >= KERNEL_VERSION(3, 18, 0)) {
        ASSERT_TRUE(reallyRead == 0);
    } else {
        ASSERT_TRUE(reallyRead > 0);
    }

    num = 1;
    EXPECT_TRUE(::write(exitBarier, &num, sizeof(num)) == sizeof(num));

    auto waitedpid = waitpid(pid, nullptr, 0);
    EXPECT_TRUE(waitedpid == pid);
}

TEST(TCGroup, RemoveAllSubcgroupsAfterLock)
{
    auto parentName = "remove_all_subcgroups_after_lock_" + ToString(TGuid::Create());
    auto parent = CreateCGroup<TFreezer>(parentName);
    TNonOwningCGroup child(parent.GetFullPath() + "/child");

    child.EnsureExistance();
    parent.Lock();
    parent.RemoveAllSubcgroups();
}

TEST(TCGroup, FreezerEmpty)
{
    auto group = CreateCGroup<TFreezer>("freezer_empty_" + ToString(TGuid::Create()));

    EXPECT_EQ("THAWED", group.GetState());
}

TEST(TCGroup, FreezerFreeze)
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
        _exit(0);
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

TEST(TProcessCGroup, Empty)
{
    TString empty;
    auto result = ParseProcessCGroups(empty);
    EXPECT_TRUE(result.empty());
}

TEST(TProcessCGroup, Basic)
{
    TString basic("4:blkio:/\n3:cpuacct:/\n2:freezer:/some\n1:memory:/\n");
    auto result = ParseProcessCGroups(basic);
    EXPECT_EQ("", result["blkio"]);
    EXPECT_EQ("", result["cpuacct"]);
    EXPECT_EQ("some", result["freezer"]);
    EXPECT_EQ("", result["memory"]);
    EXPECT_EQ(4, result.size());
}

TEST(TProcessCGroup, Multiple)
{
    auto basic("5:cpuacct,cpu,cpuset:/daemons\n");
    auto result = ParseProcessCGroups(basic);
    EXPECT_EQ("daemons", result["cpu"]);
    EXPECT_EQ("daemons", result["cpuset"]);
    EXPECT_EQ("daemons", result["cpuacct"]);
    EXPECT_EQ(3, result.size());
}

TEST(TProcessCGroup, BadInput)
{
    TString basic("xxx:cpuacct,cpu,cpuset:/daemons\n");
    EXPECT_THROW(ParseProcessCGroups(basic), std::exception);
}

#endif // _linux_

////////////////////////////////////////////////////////////////////////////////

}
} // namespace NCGroup
} // namespace NYT
