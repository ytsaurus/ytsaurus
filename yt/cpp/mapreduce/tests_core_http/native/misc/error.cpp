#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>


using namespace NYT;
using namespace NYT::NTesting;

Y_UNIT_TEST_SUITE(TestErrors)
{
    Y_UNIT_TEST(TestErrorParsing)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Set(workingDir + "/vzhukh", "i protestirovano");

        try {
            // we hope to get nontrivial tree of errors
            client->Link(workingDir + "/vzhukh", workingDir + "/vzhukh/missing_path");
        } catch (const NYT::TErrorResponse& e) {
            const auto& error = e.GetError();
            UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), NYT::NClusterErrorCodes::NYTree::ResolveError);
            UNIT_ASSERT_VALUES_EQUAL(error.InnerErrors().size(), 1);
        }
    }
}
