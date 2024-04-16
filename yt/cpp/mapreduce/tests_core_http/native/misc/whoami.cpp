#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/errors.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(WhoAmI)
{
    Y_UNIT_TEST(Test)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto authInfo = client->WhoAmI();
        UNIT_ASSERT_EQUAL(authInfo.Login, "root");
    }
}

////////////////////////////////////////////////////////////////////////////////
