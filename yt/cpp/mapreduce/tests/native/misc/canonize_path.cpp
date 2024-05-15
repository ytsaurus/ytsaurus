#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/errors.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

TEST(CanonizeYPath, TestOkCanonization)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto canonized = client->CanonizeYPath(TRichYPath("//foo/bar[#100500]").Columns({"column"}));
    EXPECT_EQ(canonized.Path_, "//foo/bar");
    EXPECT_EQ(canonized.Columns_, TColumnNames({"column"}));
}

TEST(CanonizeYPath, TestBadCanonization)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    EXPECT_THROW(
        client->CanonizeYPath(TRichYPath("//foo/bar[#1005")),
        TErrorResponse);
}

////////////////////////////////////////////////////////////////////////////////
