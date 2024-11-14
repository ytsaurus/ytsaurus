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

    EXPECT_FALSE(IsAttributePath("/test/"));
    EXPECT_FALSE(IsAttributePath("/test/aa////"));
    EXPECT_FALSE(IsAttributePath("test/aa////"));
    EXPECT_FALSE(IsAttributePath("token"));
    EXPECT_FALSE(IsAttributePath("[/meta/id] == \"foo\""));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
