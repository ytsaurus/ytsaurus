#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/library/attributes/attribute_path.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAttributePathTest, IsAttributesRelated)
{
    EXPECT_TRUE(AreAttributesRelated("", "/a/b"));
    EXPECT_TRUE(AreAttributesRelated("/a", "/a/b"));
    EXPECT_TRUE(AreAttributesRelated("/a/b", "/a/b"));
    EXPECT_TRUE(AreAttributesRelated("/a/b/c", "/a/b"));

    EXPECT_FALSE(AreAttributesRelated("/b", "/a/b"));
    EXPECT_FALSE(AreAttributesRelated("/b/a", "/b/b"));
    EXPECT_FALSE(AreAttributesRelated("/bb", "/b/b"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
