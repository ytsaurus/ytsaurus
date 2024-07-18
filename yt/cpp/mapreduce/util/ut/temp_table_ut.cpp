#include <yt/cpp/mapreduce/util/temp_table.h>

#include <yt/cpp/mapreduce/library/mock_client/yt_mock.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;

TEST(TTempTableTest, Default)
{
    auto client = MakeIntrusive<NYT::NTesting::TClientMock>();
    {
        EXPECT_CALL(*client, Create(testing::_, testing::_, testing::_)).Times(testing::Exactly(2));
        EXPECT_CALL(*client, Remove(testing::_, testing::_));
        ASSERT_NO_THROW(auto table = NYT::TTempTable(client));
    }
}

TEST(TTempTableTest, WithExistingPath)
{
    auto client = MakeIntrusive<NYT::NTesting::TClientMock>();
    {
        EXPECT_CALL(*client, Exists("//some/path", testing::_)).WillOnce(testing::Return(true));
        EXPECT_CALL(*client, Create(testing::_, testing::_, testing::_));
        EXPECT_CALL(*client, Remove(testing::_, testing::_));
        ASSERT_NO_THROW(NYT::TTempTable(client, {}, "//some/path"));
    }
}

TEST(TTempTableTest, WithNonexistingPath)
{
    auto client = MakeIntrusive<NYT::NTesting::TClientMock>();
    {
        EXPECT_CALL(*client, Exists("//some/path", testing::_)).WillOnce(testing::Return(false));
        ASSERT_ANY_THROW(NYT::TTempTable(client, {}, "//some/path"));
    }
}

TEST(TTempTableTest, Recursive)
{
    auto client = MakeIntrusive<NYT::NTesting::TClientMock>();
    {
        EXPECT_CALL(*client, Create(testing::_, testing::_, testing::_));
        EXPECT_CALL(*client, Remove(testing::_, testing::_));
        ASSERT_NO_THROW(auto table = NYT::TTempTable(client, {}, "//some/path", NYT::TCreateOptions().Recursive(true)));
    }
}
