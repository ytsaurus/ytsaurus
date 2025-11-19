#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/library/attributes/attribute_path.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAttributePathTest, IsAttributePath)
{
    EXPECT_TRUE(IsAttributePath("/test/obj"));
    EXPECT_TRUE(IsAttributePath(""));
    EXPECT_TRUE(IsAttributePath("/test/a/b"));
    EXPECT_TRUE(IsAttributePath("/test/a/b/c/d"));
    EXPECT_TRUE(IsAttributePath("<a=b>/test/c/d"));

    EXPECT_FALSE(IsAttributePath("/test/"));
    EXPECT_FALSE(IsAttributePath("/test/aa////"));
    EXPECT_FALSE(IsAttributePath("test/aa////"));
    EXPECT_FALSE(IsAttributePath("token"));
    EXPECT_FALSE(IsAttributePath("[/meta/id] == \"foo\""));
    EXPECT_FALSE(IsAttributePath("<a=b/test/e/f/g"));
}

TEST(TAttributePathTest, AreAttributesRelated)
{
    EXPECT_TRUE(AreAttributesRelated("", "/a/b"));
    EXPECT_TRUE(AreAttributesRelated("/a", "/a/b"));
    EXPECT_TRUE(AreAttributesRelated("/a/b", "/a/b"));
    EXPECT_TRUE(AreAttributesRelated("/a/b/c", "/a/b"));

    EXPECT_FALSE(AreAttributesRelated("/b", "/a/b"));
    EXPECT_FALSE(AreAttributesRelated("/b/a", "/b/b"));
    EXPECT_FALSE(AreAttributesRelated("/bb", "/b/b"));
}

TEST(TAttributePathTest, MatchAttributePathToPattern)
{
    EXPECT_EQ(EAttributePathMatchResult::None, MatchAttributePathToPattern("/spec", "/status"));
    EXPECT_EQ(EAttributePathMatchResult::None, MatchAttributePathToPattern("/status", "/spec"));
    EXPECT_EQ(EAttributePathMatchResult::None, MatchAttributePathToPattern("/status", "/spec"));

    EXPECT_EQ(EAttributePathMatchResult::Full, MatchAttributePathToPattern("/status", "/status"));
    EXPECT_EQ(EAttributePathMatchResult::Full, MatchAttributePathToPattern("/spec/*", "/spec/foo"));
    EXPECT_EQ(EAttributePathMatchResult::Full, MatchAttributePathToPattern("/spec/*", "/spec/0"));

    EXPECT_EQ(EAttributePathMatchResult::PatternIsPrefix, MatchAttributePathToPattern("/spec/*", "/spec/foo/bar"));
    EXPECT_EQ(EAttributePathMatchResult::PatternIsPrefix, MatchAttributePathToPattern("/spec/foo", "/spec/foo/bar"));

    EXPECT_EQ(EAttributePathMatchResult::PathIsPrefix, MatchAttributePathToPattern("/spec/*", "/spec"));
    EXPECT_EQ(EAttributePathMatchResult::PathIsPrefix, MatchAttributePathToPattern("/spec/foo", "/spec"));
    EXPECT_EQ(EAttributePathMatchResult::PathIsPrefix, MatchAttributePathToPattern("/spec/foo/*", "/spec"));
}

TEST(TAttributeAsteriskSplitTest, SplitPatternByAsterisk)
{
    EXPECT_EQ(TSplitResult("/spec", "/doozer/foo"), SplitPatternByAsterisk("/spec/*/doozer/foo"));
    EXPECT_EQ(TSplitResult("/spec", ""), SplitPatternByAsterisk("/spec/*"));
    EXPECT_EQ(TSplitResult("", ""), SplitPatternByAsterisk("/*"));

    EXPECT_EQ(TSplitResult("", std::nullopt), SplitPatternByAsterisk(""));
    EXPECT_EQ(TSplitResult("/", std::nullopt), SplitPatternByAsterisk("/"));
    EXPECT_EQ(TSplitResult("/a/b/c", std::nullopt), SplitPatternByAsterisk("/a/b/c"));
}

TEST(TAttributePathRootTest, GetAttribitePathRoot)
{
    EXPECT_EQ(TSplitResult("/spec", "/doozer/foo"), GetAttributePathRoot("/spec/doozer/foo"));
    EXPECT_EQ(TSplitResult("/spec/doozer", "/foo"), GetAttributePathRoot("/spec/doozer/foo", 2));
    EXPECT_EQ(TSplitResult("/spec/doozer", "/"), GetAttributePathRoot("/spec/doozer/", 2));
    EXPECT_EQ(TSplitResult("/spec/doozer", ""), GetAttributePathRoot("/spec/doozer", 2));
    EXPECT_EQ(TSplitResult("/spec/doozer/foo", ""), GetAttributePathRoot("/spec/doozer/foo", 3));

    EXPECT_EQ(TSplitResult("/spec", ""), GetAttributePathRoot("/spec"));
    EXPECT_EQ(TSplitResult("", "/*/a/b"), GetAttributePathRoot("/*/a/b"));
    EXPECT_EQ(TSplitResult("", "/a/*/b"), GetAttributePathRoot("/a/*/b", 2));
    EXPECT_EQ(TSplitResult("/a", "/*/b"), GetAttributePathRoot("/a/*/b", 1));

    EXPECT_EQ(TSplitResult("", "/"), GetAttributePathRoot("/"));
    EXPECT_EQ(TSplitResult("", "/*/*"), GetAttributePathRoot("/*/*"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
