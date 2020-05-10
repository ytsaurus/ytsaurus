#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/errors.h>

#include <library/cpp/unittest/registar.h>

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
