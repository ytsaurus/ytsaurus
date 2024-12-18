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

    EXPECT_THROW_THAT(
        auto p5 = TAbsoluteYPath("/bar"),
        ::testing::HasSubstr("does not start with a valid root-designator"));
    EXPECT_THROW_THAT(
        auto p6 = TYPath("bar"),
        ::testing::HasSubstr("Expected \"slash\" in YPath but found \"literal\""));
}

TEST(YPathTest, RootDesignator)
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

TEST(YPathTest, Segments)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSequoiaClient
