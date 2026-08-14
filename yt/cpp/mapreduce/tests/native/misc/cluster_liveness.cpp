#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/errors.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

TEST(CheckClusterLiveness, Default)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();

    EXPECT_NO_THROW(client->CheckClusterLiveness(
        TCheckClusterLivenessOptions()));
}

TEST(CheckClusterLiveness, NoChecksRequestedThrows)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();

    EXPECT_THROW(
        client->CheckClusterLiveness(
            TCheckClusterLivenessOptions().
                CheckCypressRoot(false).
                CheckSecondaryMasterCells(false)),
        TErrorResponse);
}

TEST(CheckClusterLiveness, NonExistentTabletCellBundleThrows)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();

    EXPECT_THROW(
        client->CheckClusterLiveness(
            TCheckClusterLivenessOptions().CheckTabletCellBundle("b")),
        TErrorResponse);
}

////////////////////////////////////////////////////////////////////////////////
