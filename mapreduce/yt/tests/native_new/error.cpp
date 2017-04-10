#include "lib.h"

#include <library/unittest/registar.h>

#include <mapreduce/yt/http/error.h>
#include <mapreduce/yt/http/error_codes.h>


using namespace NYT;
using namespace NYT::NTesting;

SIMPLE_UNIT_TEST_SUITE(TestErrors)
{
    SIMPLE_UNIT_TEST(TestErrorParsing)
    {
        auto client = CreateTestClient();
        client->Set("//testing/vzhukh", "i protestirovano");

        try {
             // we hope to get nontrivial tree of errors
            client->Link("//testing/vzhukh", "//testing/vzhukh/missing_path");
        } catch (const NYT::TErrorResponse& e) {
            const auto& error = e.GetError();
            UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), NYT::NClusterErrorCodes::NYTree::ResolveError);
            UNIT_ASSERT_VALUES_EQUAL(error.GetAttributes().has("host"), true);
            UNIT_ASSERT_VALUES_EQUAL(error.InnerErrors().size(), 1);
        }
    }
}
