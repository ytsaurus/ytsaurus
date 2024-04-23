#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

TEST(TestErrors, TestErrorParsing)
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
        EXPECT_EQ(error.GetCode(), NYT::NClusterErrorCodes::NYTree::ResolveError);
        EXPECT_EQ(std::ssize(error.InnerErrors()), 1);
    }
}
