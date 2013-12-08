#include "stdafx.h"
#include "framework.h"

#include <core/misc/string.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

static struct TTestCase {
    const char* UnderCase;
    const char* CamelCase;
} TestCasesRaw[] = {
    { "kenny", "Kenny" },
    { "south_park", "SouthPark" },
    { "a", "A" },
    { "a_b_c", "ABC" },
    { "reed_solomon_6_3", "ReedSolomon_6_3" },
    { "l_r_c_12_2_2", "LRC_12_2_2" },
    { "0", "0" },
    { "0_1_2", "0_1_2" },
};

static std::vector<TTestCase> TestCases(
    TestCasesRaw,
    TestCasesRaw + ARRAY_SIZE(TestCasesRaw));

////////////////////////////////////////////////////////////////////////////////

TEST(TStringTest, UnderscoreCaseToCamelCase)
{
    FOREACH(auto& testCase, TestCases) {
        auto result = UnderscoreCaseToCamelCase(testCase.UnderCase);
        EXPECT_STREQ(testCase.CamelCase, result.c_str())
            << "Original: \"" << testCase.UnderCase << '"';
    }
}

TEST(TStringTest, CamelCaseToUnderscoreCase)
{
    FOREACH(auto& testCase, TestCases) {
        auto result = CamelCaseToUnderscoreCase(testCase.CamelCase);
        EXPECT_STREQ(testCase.UnderCase, result.c_str())
            << "Original: \"" << testCase.CamelCase << '"';
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
