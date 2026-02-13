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
    EXPECT_EQ(std::pair("/spec", "/doozer/foo"), SplitPatternByAsterisk("/spec/*/doozer/foo"));
    EXPECT_EQ(std::pair("/spec", ""), SplitPatternByAsterisk("/spec/*"));
    EXPECT_EQ(std::pair("", ""), SplitPatternByAsterisk("/*"));

    EXPECT_EQ(std::pair("", std::nullopt), SplitPatternByAsterisk(""));
    EXPECT_EQ(std::pair("/", std::nullopt), SplitPatternByAsterisk("/"));
    EXPECT_EQ(std::pair("/a/b/c", std::nullopt), SplitPatternByAsterisk("/a/b/c"));
}

TEST(TAttributePathRootTest, GetAttribitePathRoot)
{
    EXPECT_EQ(TSplitResult("/spec", "/doozer/foo"), GetAttributePathRoot("/spec/doozer/foo"));
    EXPECT_EQ(TSplitResult("/spec/doozer", "/foo"), GetAttributePathRoot("/spec/doozer/foo", 2));
    EXPECT_EQ(TSplitResult("/spec/doozer", "/"), GetAttributePathRoot("/spec/doozer/", 2));
    EXPECT_EQ(TSplitResult("/spec/doozer", ""), GetAttributePathRoot("/spec/doozer", 2));
    EXPECT_EQ(TSplitResult("/spec/doozer/foo", ""), GetAttributePathRoot("/spec/doozer/foo", 3));

    EXPECT_EQ(TSplitResult(std::nullopt, ""), GetAttributePathRoot("", 1));

    EXPECT_EQ(TSplitResult("/spec", ""), GetAttributePathRoot("/spec"));
    EXPECT_EQ(TSplitResult(std::nullopt, ""), GetAttributePathRoot("/*/a/b"));
    EXPECT_EQ(TSplitResult(std::nullopt, ""), GetAttributePathRoot("/a/*/b", 2));
    EXPECT_EQ(TSplitResult("/a", "/*/b"), GetAttributePathRoot("/a/*/b", 1));

    EXPECT_EQ(TSplitResult(std::nullopt, ""), GetAttributePathRoot("/"));
    EXPECT_EQ(TSplitResult(std::nullopt, ""), GetAttributePathRoot("/*/*"));
}

TEST(TTryConsumePrefixTest, TryConsumePrefix)
{
    using namespace NYPath;

    EXPECT_EQ(TSplitResult("/spec", "/foo"), TryConsumePrefix("/spec/foo", "/spec"));
    EXPECT_EQ(TSplitResult("/spec", "/a/b/c"), TryConsumePrefix("/spec/a/b/c", "/spec"));
    EXPECT_EQ(TSplitResult("/spec/a", "/b/c"), TryConsumePrefix("/spec/a/b/c", "/spec/a"));
    EXPECT_EQ(TSplitResult("/spec/a/b", "/c"), TryConsumePrefix("/spec/a/b/c", "/spec/a/b"));
    EXPECT_EQ(TSplitResult("/spec/a/b/c", ""), TryConsumePrefix("/spec/a/b/c", "/spec/a/b/c"));
    EXPECT_EQ(TSplitResult("", ""), TryConsumePrefix("", ""));
    EXPECT_EQ(TSplitResult("", "/*"), TryConsumePrefix("/*", ""));
    EXPECT_EQ(TSplitResult("/*", ""), TryConsumePrefix("/*", "/1"));
    EXPECT_EQ(TSplitResult("/spec/list/*", "/item"), TryConsumePrefix("/spec/list/*/item", "/spec/list/*"));
    EXPECT_EQ(TSplitResult("/spec/list/*", "/item"), TryConsumePrefix("/spec/list/*/item", "/spec/list/begin"));
    EXPECT_EQ(TSplitResult("/spec/list/*/x", "/y"), TryConsumePrefix("/spec/list/*/x/y", "/spec/list/100/x"));
    EXPECT_EQ(TSplitResult("/spec/l", "/*"), TryConsumePrefix("/spec/l/*", "/spec/l"));
    EXPECT_EQ(TSplitResult("/spec/foo", "/*/bar"), TryConsumePrefix("/spec/foo/*/bar", "/spec/foo"));

    EXPECT_FALSE(TryConsumePrefix("/spec/a/b", "/xxx").first.has_value());
    EXPECT_FALSE(TryConsumePrefix("/spec/a/b", "/spe").first.has_value());
    EXPECT_FALSE(TryConsumePrefix("/spec/a/b", "/spec/a/b/c").first.has_value());
    EXPECT_FALSE(TryConsumePrefix("/spec/list/*/item", "/spec/list/0/item/extra").first.has_value());
    EXPECT_FALSE(TryConsumePrefix("/spec/list/*/item", "/spec/other/0").first.has_value());
    EXPECT_FALSE(TryConsumePrefix("/spec/list/*/item", "/spec/list/foo/1").first.has_value());
    EXPECT_FALSE(TryConsumePrefix("/spec/root/*/y/*/k", "/spec/root/0/x/3").first.has_value());
    EXPECT_FALSE(TryConsumePrefix("/spec/root/*/sub/k", "/spec/root/end/sub/x").first.has_value());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
