#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/errors.h>

#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(CanonizeYPath)
{
    Y_UNIT_TEST(TestOkCanonization)
    {
        auto client = CreateTestClient();

        auto canonized = client->CanonizeYPath(TRichYPath("//foo/bar[#100500]").Columns({"column"}));
        UNIT_ASSERT_EQUAL(canonized.Path_, "//foo/bar");
        UNIT_ASSERT_EQUAL(canonized.Columns_, TKeyColumns({"column"}));
    }

    Y_UNIT_TEST(TestBadCanonization)
    {
        auto client = CreateTestClient();
        UNIT_ASSERT_EXCEPTION(
            client->CanonizeYPath(TRichYPath("//foo/bar[#1005")),
            TErrorResponse);
    }
}

////////////////////////////////////////////////////////////////////////////////
