#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/errors.h>

#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(WhoAmI)
{
    Y_UNIT_TEST(Test)
    {
        auto client = CreateTestClient();

        auto authInfo = client->WhoAmI();
        UNIT_ASSERT_EQUAL(authInfo.Login, "root");
    }
}

////////////////////////////////////////////////////////////////////////////////
