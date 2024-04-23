#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/errors.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

TEST(WhoAmI, Test)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto authInfo = client->WhoAmI();
    EXPECT_EQ(authInfo.Login, "root");
}

////////////////////////////////////////////////////////////////////////////////
