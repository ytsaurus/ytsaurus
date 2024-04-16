#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/errors.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(CanonizeYPath)
{
    Y_UNIT_TEST(TestOkCanonization)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        auto canonized = client->CanonizeYPath(TRichYPath("//foo/bar[#100500]").Columns({"column"}));
        UNIT_ASSERT_EQUAL(canonized.Path_, "//foo/bar");
        UNIT_ASSERT_EQUAL(canonized.Columns_, TColumnNames({"column"}));
    }

    Y_UNIT_TEST(TestBadCanonization)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        UNIT_ASSERT_EXCEPTION(
            client->CanonizeYPath(TRichYPath("//foo/bar[#1005")),
            TErrorResponse);
    }
}

////////////////////////////////////////////////////////////////////////////////
