#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>
#include <yt/yt/ytlib/sequoia_client/helpers.h>

namespace NYT::NSequoiaClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TYPathTest, Correctness)
{
    auto p1 = TAbsolutePath::MakeCanonicalPathOrThrow("/");
    auto p2 = TAbsolutePath::MakeCanonicalPathOrThrow("//foo");
    auto p3 = TRelativePath::MakeCanonicalPathOrThrow("/foo");
    auto p4 = TRelativePath::MakeCanonicalPathOrThrow("");

    EXPECT_THROW_WITH_SUBSTRING(
        auto p5 = TAbsolutePath::MakeCanonicalPathOrThrow("/bar"),
        "Expected \"slash\" in YPath but found \"literal\"");
    EXPECT_THROW_WITH_SUBSTRING(
        auto p2 = TRelativePath::MakeCanonicalPathOrThrow("bar"),
        "Expected \"slash\" in YPath but found \"literal\"");

    auto raw = TRawYPath(std::string{'/', '/', 'f', '\\', '\0', 'o'});
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateAndMakeYPath(std::move(raw)),
        "Path contains a forbidden symbol");
}

TEST(TYPathTest, RemoveLastSegment)
{
    // Basic tests.
    auto path = TRelativePath::UnsafeMakeCanonicalPath("/first");
    path.RemoveLastSegment();
    EXPECT_EQ(path.Underlying(), "");
    path = TRelativePath::UnsafeMakeCanonicalPath("/first/second/third");
    path.RemoveLastSegment();
    EXPECT_EQ(path.Underlying(), "/first/second");

    // Edge cases.
    path = TRelativePath::UnsafeMakeCanonicalPath("/first/second");
    path.RemoveLastSegment();
    path.RemoveLastSegment();
    path.RemoveLastSegment();
    EXPECT_EQ(path.Underlying(), "");

    // Basic tests.
    auto absolute = TAbsolutePath::UnsafeMakeCanonicalPath("//first");
    absolute.RemoveLastSegment();
    EXPECT_EQ(absolute.Underlying(), "/");
    absolute = TAbsolutePath::UnsafeMakeCanonicalPath("//first/second/third");
    absolute.RemoveLastSegment();
    EXPECT_EQ(absolute.Underlying(), "//first/second");

    // Edge cases.
    absolute = TAbsolutePath::UnsafeMakeCanonicalPath("/");
    absolute.RemoveLastSegment();
    EXPECT_EQ(absolute.Underlying(), "/");
}

TEST(TYPathTest, Append)
{
    auto path = TRelativePath::UnsafeMakeCanonicalPath("");
    path.Append("first");
    EXPECT_EQ(path.Underlying(), "/first");
    path.Append("/second");
    EXPECT_EQ(path.Underlying(), "/first/\\/second");

    auto absolute = TRelativePath::UnsafeMakeCanonicalPath("/");
    absolute.Append("first");
    EXPECT_EQ(absolute.Underlying(), "//first");
    absolute.Append("second");
    EXPECT_EQ(absolute.Underlying(), "//first/second");
}

TEST(TYPathTest, Mangling)
{
    {
        auto p = TAbsolutePath::UnsafeMakeCanonicalPath("//foo/bar");
        auto m = p.ToMangledSequoiaPath();
        auto r = std::string{'/', '\0', 'f', 'o', 'o', '\0', 'b', 'a', 'r', '\0'};
        EXPECT_EQ(m.Underlying(), r);
        EXPECT_EQ(TAbsolutePath(m), p);
    }
    {
        auto p = TAbsolutePath::UnsafeMakeCanonicalPath(R"(//\\\/\@\&\*\[\{)");
        auto m = p.ToMangledSequoiaPath();
        auto r = std::string{'/', '\0', '\\', '/', '@', '&', '*', '[', '{', '\0'};
        EXPECT_EQ(m.Underlying(), r);
        EXPECT_EQ(TAbsolutePath(m), p);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSequoiaClient
