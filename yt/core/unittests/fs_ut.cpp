#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/fs.h>

#include <util/folder/dirut.h>

namespace NYT::NFS {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFSTest, GetRealPath)
{
    auto cwd = NFs::CurrentWorkingDirectory();
    EXPECT_EQ(CombinePaths(cwd, "dir"), GetRealPath("dir"));
    EXPECT_EQ(cwd, GetRealPath("dir/.."));
    EXPECT_EQ(CombinePaths(cwd, "dir"), GetRealPath("dir/./a/b/../../."));
}

TEST(TFSTest, IsPathRelativeAndInvolvesNoTraversal)
{
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal(""));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("some"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("some/file"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("."));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("./some/file"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("./some/./file"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("./some/.."));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/../b"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/./../b"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("some/"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("some//"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("a//b"));
    EXPECT_TRUE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/b/"));

    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("/"));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("//"));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("/some"));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal(".."));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("../some"));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/../.."));
    EXPECT_FALSE(NFS::IsPathRelativeAndInvolvesNoTraversal("a/../../b"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFS

