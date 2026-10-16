#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/containers/private.h>

namespace NYT::NContainers::NDetail {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TParsePortoPidsCGroupTest, V1)
{
    // The "/pids" controller segment is stripped, leaving the process-relative path.
    EXPECT_EQ(
        ParsePortoPidsCGroup("pids: /sys/fs/cgroup/pids/yt/foo", /*isV2*/ false),
        "/yt/foo");
}

TEST(TParsePortoPidsCGroupTest, V2)
{
    // v2 has no per-controller segment.
    EXPECT_EQ(
        ParsePortoPidsCGroup("unified: /sys/fs/cgroup/yt/foo", /*isV2*/ true),
        "/yt/foo");
}

TEST(TParsePortoPidsCGroupTest, PicksRightEntryAmongMany)
{
    TStringBuf cgroups =
        "memory: /sys/fs/cgroup/memory/yt/foo; "
        "pids: /sys/fs/cgroup/pids/yt/foo; "
        "cpu: /sys/fs/cgroup/cpu/yt/foo";
    EXPECT_EQ(ParsePortoPidsCGroup(cgroups, /*isV2*/ false), "/yt/foo");
}

TEST(TParsePortoPidsCGroupTest, V1Root)
{
    // Container sits at the pids hierarchy root: nothing remains after the
    // controller segment, normalized to "/".
    EXPECT_EQ(ParsePortoPidsCGroup("pids: /sys/fs/cgroup/pids", /*isV2*/ false), "/");
}

TEST(TParsePortoPidsCGroupTest, V2Root)
{
    EXPECT_EQ(ParsePortoPidsCGroup("unified: /sys/fs/cgroup", /*isV2*/ true), "/");
}

TEST(TParsePortoPidsCGroupTest, MissingEntryReturnsEmpty)
{
    // No "pids:" entry (e.g. v2 entry present but queried as v1).
    EXPECT_EQ(ParsePortoPidsCGroup("unified: /sys/fs/cgroup/yt/foo", /*isV2*/ false), "");
    EXPECT_EQ(ParsePortoPidsCGroup("", /*isV2*/ false), "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NContainers::NDetail
