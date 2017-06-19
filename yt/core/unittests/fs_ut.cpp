#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/fs.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NFS {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TRealPathTest, Basic)
{
    auto cwd = NFs::CurrentWorkingDirectory();
    EXPECT_EQ(CombinePaths(cwd, "dir"), GetRealPath("dir"));
    EXPECT_EQ(cwd, GetRealPath("dir/.."));
    EXPECT_EQ(CombinePaths(cwd, "dir"), GetRealPath("dir/./a/b/../../."));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFS
} // namespace NYT

