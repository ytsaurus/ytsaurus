#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

namespace NYT::NSequoiaClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TYPathTest, Correctness)
{
    auto p1 = TAbsoluteYPath("/");
    auto p2 = TAbsoluteYPath("//foo");
    auto p3 = TYPath("/foo");
    auto p4 = TYPath("&");

    EXPECT_THROW_WITH_SUBSTRING(
        auto p5 = TAbsoluteYPath("/bar"),
        "does not start with a valid root-designator");
    EXPECT_THROW_WITH_SUBSTRING(
        auto p2 = TYPath("bar"),
        "Expected \"slash\" in YPath but found \"literal\"");

    auto raw = std::string{'/', '/', 'f', '\0', 'o'};
    EXPECT_THROW_WITH_SUBSTRING(
        TAbsoluteYPathBuf(TStringBuf(raw)),
        "Path contains forbidden symbol");
}

TEST(TYPathTest, RootDesignator)
{
    {
        auto p = TAbsoluteYPath("//foo");
        auto [r, s] = p.GetRootDesignator();
        EXPECT_TRUE(std::holds_alternative<TSlashRootDesignatorTag>(r));
        EXPECT_EQ(s, TYPath("/foo"));
    }
    {
        auto p = TAbsoluteYPath("#0-0-0-0/foo");
        auto [r, s] = p.GetRootDesignator();
        EXPECT_TRUE(std::holds_alternative<NObjectClient::TObjectId>(r));
        EXPECT_EQ(std::get<NObjectClient::TObjectId>(r), NObjectClient::TObjectId{});
        EXPECT_EQ(s, TYPath("/foo"));
    }
}

TEST(TYPathTest, Segments)
{
    auto p = TYPath("/foo/bar&/@baz");
    auto segments = p.AsSegments();
    auto it = segments.begin();
    EXPECT_EQ(*it++, TYPath("/foo"));
    EXPECT_EQ(*it++, TYPath("/bar"));
    EXPECT_EQ(*it++, TYPath("&"));
    EXPECT_EQ(*it++, TYPath("/@baz"));
    EXPECT_EQ(it, segments.end());
}

TEST(TYPathTest, GetFirstSegment)
{
    // Basic tests.
    auto path = TYPath("/first");
    EXPECT_EQ(path.GetFirstSegment(), "first");
    path = TYPath("/first/second/third");
    EXPECT_EQ(path.GetFirstSegment(), "first");

    // Edge cases.
    path = TYPath("/");
    EXPECT_EQ(path.GetFirstSegment(), "");
    path = TYPath("/weird\\/path");
    EXPECT_EQ(path.GetFirstSegment(), "weird\\/path");
    path = TYPath("/weird\\\\/path");
    EXPECT_EQ(path.GetFirstSegment(), "weird\\\\");
}

TEST(TYPathTest, RemoveLastSegment)
{
    // Basic tests.
    auto path = TYPath("/first");
    path.RemoveLastSegment();
    EXPECT_EQ(path, TYPath(""));
    path = TYPath("/first/second/third");
    path.RemoveLastSegment();
    EXPECT_EQ(path, TYPath("/first/second"));

    // Edge cases.
    path = TYPath("/first/second//");
    path.RemoveLastSegment();
    EXPECT_EQ(path, TYPath("/first/second/"));
    path = TYPath("/first/second");
    path.RemoveLastSegment();
    path.RemoveLastSegment();
    path.RemoveLastSegment();
    EXPECT_EQ(path, TYPath(""));

    // Basic tests.
    auto absolute = TAbsoluteYPath("//");
    absolute.RemoveLastSegment();
    EXPECT_EQ(absolute, TAbsoluteYPath("/"));
    absolute = TAbsoluteYPath("#123-123-123-123/something");
    absolute.RemoveLastSegment();
    EXPECT_EQ(absolute, TAbsoluteYPath("#123-123-123-123"));
    absolute = TAbsoluteYPath("//first");
    absolute.RemoveLastSegment();
    EXPECT_EQ(absolute, TAbsoluteYPath("/"));
    absolute = TAbsoluteYPath("//first/second/third");
    absolute.RemoveLastSegment();
    EXPECT_EQ(absolute, TAbsoluteYPath("//first/second"));

    // Edge cases.
    absolute = TAbsoluteYPath("/");
    absolute.RemoveLastSegment();
    EXPECT_EQ(absolute, TAbsoluteYPath("/"));
    absolute = TAbsoluteYPath("#123-123-123-123");
    absolute.RemoveLastSegment();
    EXPECT_EQ(absolute, TAbsoluteYPath("#123-123-123-123"));
}

TEST(TYPathTest, Append)
{
    auto path = TYPath("/first");
    path.Append("second");
    EXPECT_EQ(path, TYPath("/first/second"));
    path = TYPath("/first");
    path.Append("/second");
    EXPECT_EQ(path, TYPath("/first/\\/second"));

    auto absolute = TAbsoluteYPath("/");
    absolute.Append("first");
    EXPECT_EQ(absolute, TAbsoluteYPath("//first"));
    absolute = TAbsoluteYPath("#123-123-123-123");
    absolute.Append("something");
    EXPECT_EQ(absolute, TAbsoluteYPath("#123-123-123-123/something"));
    absolute = TAbsoluteYPath("//");
    absolute.Append("first");
    EXPECT_EQ(absolute, TAbsoluteYPath("//first"));
    absolute = TAbsoluteYPath("//first");
    absolute.Append("second");
    EXPECT_EQ(absolute, TAbsoluteYPath("//first/second"));
}

TEST(TYPathTest, Mangling)
{
    {
        auto p = TAbsoluteYPath("//foo/bar");
        auto m = p.ToMangledSequoiaPath();
        auto r = std::string{'/', '\0', 'f', 'o', 'o', '\0', 'b', 'a', 'r', '\0'};
        EXPECT_EQ(m.Underlying(), r);
        EXPECT_EQ(TAbsoluteYPath(m), p);
    }
    {
        auto p = TAbsoluteYPath(R"(//\\\/\@\&\*\[\{)");
        auto m = p.ToMangledSequoiaPath();
        auto r = std::string{'/', '\0', '\\', '/', '@', '&', '*', '[', '{', '\0'};
        EXPECT_EQ(m.Underlying(), r);
        EXPECT_EQ(TAbsoluteYPath(m), p);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSequoiaClient
