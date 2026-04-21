#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/cgroup/process.h>

namespace NYT::NContainers {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TParseProcessCGroupsTest, V1Typical)
{
    auto result = ParseProcessCGroups(
        "12:cpu,cpuacct:/user.slice/session-1.scope\n"
        "11:memory:/user.slice/session-1.scope\n"
        "10:pids:/user.slice/session-1.scope\n"
        "9:blkio:/user.slice/session-1.scope\n"
        "1:name=systemd:/user.slice/session-1.scope\n");

    EXPECT_EQ(result["cpu"], "user.slice/session-1.scope");
    EXPECT_EQ(result["cpuacct"], "user.slice/session-1.scope");
    EXPECT_EQ(result["memory"], "user.slice/session-1.scope");
    EXPECT_EQ(result["pids"], "user.slice/session-1.scope");
    EXPECT_EQ(result["blkio"], "user.slice/session-1.scope");

    // name=systemd should be skipped.
    EXPECT_EQ(result.find("name=systemd"), result.end());
    EXPECT_EQ(result.find("systemd"), result.end());
}

TEST(TParseProcessCGroupsTest, V1Docker)
{
    auto result = ParseProcessCGroups(
        "3:cpu,cpuacct:/docker/abc123\n"
        "2:memory:/docker/abc123\n"
        "1:name=systemd:/docker/abc123\n");

    EXPECT_EQ(result["cpu"], "docker/abc123");
    EXPECT_EQ(result["cpuacct"], "docker/abc123");
    EXPECT_EQ(result["memory"], "docker/abc123");
}

TEST(TParseProcessCGroupsTest, V1RootCgroup)
{
    auto result = ParseProcessCGroups(
        "3:cpu,cpuacct:/\n"
        "2:memory:/\n");

    EXPECT_EQ(result["cpu"], "");
    EXPECT_EQ(result["memory"], "");
}

TEST(TParseProcessCGroupsTest, EmptyInput)
{
    auto result = ParseProcessCGroups("");
    EXPECT_TRUE(result.empty());
}

TEST(TParseProcessCGroupsTest, TrailingNewline)
{
    auto result = ParseProcessCGroups("3:cpu:/user.slice\n");
    EXPECT_EQ(result["cpu"], "user.slice");
}

#ifdef _linux_

TEST(TGetProcessCGroupsTest, SelfReturnsNonEmpty)
{
    auto result = GetSelfProcessCGroups();
    EXPECT_FALSE(result.empty());
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NContainers
