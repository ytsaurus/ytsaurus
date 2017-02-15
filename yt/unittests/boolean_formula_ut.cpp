#include "framework.h"

#include <yt/core/misc/boolean_formula.h>
#include <yt/core/misc/error.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TBooleanFormulaTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        std::vector<Stroka>,
        bool>>
{ };

TEST_P(TBooleanFormulaTest, Test)
{
    const auto& args = GetParam();
    const auto& formula = std::get<0>(args);
    const auto& values = std::get<1>(args);
    bool expected = std::get<2>(args);
   
    auto filter = MakeBooleanFormula(formula);
    
    EXPECT_EQ(expected, filter.IsSatisfiedBy(values))
        << "formula: " << formula << std::endl
        << "true variables: " << ::testing::PrintToString(values) << std::endl
        << "expected: " << expected;
}

INSTANTIATE_TEST_CASE_P(
    TBooleanFormulaTest,
    TBooleanFormulaTest,
    ::testing::Values(
        std::make_tuple("", std::vector<Stroka>{}, true),
        std::make_tuple("", std::vector<Stroka>{"b"}, true),
        std::make_tuple("a", std::vector<Stroka>{"b"}, false),
        std::make_tuple("!a", std::vector<Stroka>{"b"}, true),
        std::make_tuple("b", std::vector<Stroka>{"b"}, true),
        std::make_tuple("a|b", std::vector<Stroka>{"b"}, true),
        std::make_tuple("a & b", std::vector<Stroka>{"b"}, false),
        std::make_tuple("(b)", std::vector<Stroka>{"b"}, true),
        std::make_tuple("a|(a|b)", std::vector<Stroka>{"b"}, true),
        std::make_tuple("(a|b)&(!a&b)", std::vector<Stroka>{"b"}, true),
        std::make_tuple("a&b", std::vector<Stroka>{"a", "b"}, true),
        std::make_tuple("(a|c)&(b|c)", std::vector<Stroka>{"a", "b"}, true),
        std::make_tuple("(a|b)&c", std::vector<Stroka>{"a", "b"}, false)
));

////////////////////////////////////////////////////////////////////////////////

class TBooleanFormulaParseErrorTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<const char*>
{ };

TEST_P(TBooleanFormulaParseErrorTest, Test)
{
    const auto& formula = GetParam();

#if 0
    // View error messages.
    try {
        MakeBooleanFormula(formula);
    } catch (const TError& error) {
        std::cout << ToString(error) << std::endl;
    }
#endif

    EXPECT_THROW(MakeBooleanFormula(formula), std::exception)
        << "formula: " << formula;
}

INSTANTIATE_TEST_CASE_P(
    TBooleanFormulaParseErrorTest,
    TBooleanFormulaParseErrorTest,
    ::testing::Values(
        "!",
        "&",
        "|",
        "(",
        ")",
        "()",
        "()|a",
        "a&()",
        "a&(",
        "a|)",
        "&a",
        "a&",
        "a!",
        "a!b",
        "a|c!",
        "a!|c",
        "a|(c!)",
        "a|(c&)",
        "a|(|c)"
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
