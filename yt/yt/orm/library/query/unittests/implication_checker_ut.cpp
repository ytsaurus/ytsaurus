#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/library/query/implication_checker.h>

#include <yt/yt/library/query/base/query_preparer.h>

using namespace NYT::NQueryClient::NAst;

namespace NYT::NOrm::NQuery::NTests {

////////////////////////////////////////////////////////////////////////////////

bool CheckImplication(TStringBuf antecedent, TStringBuf consequent)
{
    auto antecedentSource = NQueryClient::ParseSource(antecedent, NQueryClient::EParseMode::Expression);
    auto consequentSource = NQueryClient::ParseSource(consequent, NQueryClient::EParseMode::Expression);
    return NQuery::CheckImplication(
        std::get<TExpressionPtr>(antecedentSource->AstHead.Ast),
        std::get<TExpressionPtr>(consequentSource->AstHead.Ast),
        antecedentSource->AstHead.AliasMap,
        consequentSource->AstHead.AliasMap);
}

////////////////////////////////////////////////////////////////////////////////

#define EXPECT_IMPLICATION(antecedent, consequent) EXPECT_TRUE(CheckImplication(antecedent, consequent))
#define EXPECT_NO_IMPLICATION(antecedent, consequent) EXPECT_FALSE(CheckImplication(antecedent, consequent))

////////////////////////////////////////////////////////////////////////////////

TEST(TImplicationCheckerTest, Simple)
{
    EXPECT_IMPLICATION("[/x] = 5", "[/x] = 5");
    EXPECT_NO_IMPLICATION("[/x] = 5", "[/y] = 5");
}

TEST(TImplicationCheckerTest, Alias)
{
    EXPECT_IMPLICATION("([/x] as foo) > 3 AND foo > 0", "([/x] as bar) > 0");
    EXPECT_IMPLICATION("([/x] as foo) = 5 AND foo > 0", "([/x] as bar) = 5");
    EXPECT_IMPLICATION(
        "([/x] as foo) > 3 AND ([/y] as baz) = 5 AND foo > 0",
        "([/x] as bar) > 0 AND ([/y] as qux) = 5");
    EXPECT_IMPLICATION("([/x] as foo) > 5 AND (foo < 10 OR [/y] = 3)", "([/x] as bar) > 5");
    EXPECT_IMPLICATION(
        "([/x] as foo) > 0 AND foo < 100 AND [/y] = 1",
        "([/x] as bar) > 0 OR ([/y] as baz) = 1");
    EXPECT_IMPLICATION("([/x] as foo) > 1 AND foo > 2 AND foo < 10", "([/x] as bar) > 1");
    EXPECT_IMPLICATION("(([/x] as foo) = 5 OR foo = 10) AND [/y] > 0", "([/y] as bar) > 0");
    EXPECT_IMPLICATION(
        "([/x] as foo) > 5 AND [/x] < 20 AND foo != 10",
        "([/x] as bar) > 5 AND bar < 20");
    EXPECT_IMPLICATION(
        "([/x] as foo) > 10 AND ([/y] as baz) > 20 AND foo < 50 AND baz < 100",
        "([/x] as bar) > 10 AND ([/y] as qux) > 20");
    EXPECT_IMPLICATION(
        "([/x] as foo) >= 5 AND ([/y] as baz) <= 15 AND foo < baz",
        "([/x] as bar) >= 5 AND ([/y] as qux) <= 15");
    EXPECT_IMPLICATION(
        "([/x] as foo) = 10 AND ([/y] as baz) = 20 AND ([/z] as qux) > 0",
        "([/x] as bar) = 10 AND ([/y] as quux) = 20");
    EXPECT_IMPLICATION(
        "([/x] as foo) > 0 AND ([/y] as baz) > 0 AND foo < baz",
        "([/x] as bar) > 0 AND ([/y] as qux) > 0");
    EXPECT_IMPLICATION(
        "([/x] as foo) > 5 AND foo < 15 AND ([/y] as baz) = 2",
        "([/x] as bar) > 5 AND ([/y] as qux) = 2");

    EXPECT_NO_IMPLICATION("([/x] as foo) >= 5 AND foo <= 15", "([/x] as bar) >= 10 AND bar <= 20");
}

TEST(TImplicationCheckerTest, MiscPositive)
{
    EXPECT_IMPLICATION("[/y] = 2 AND [/z] = 3 AND [/x] = 1", "[/x] = -5 OR [/y] = 2");
    EXPECT_IMPLICATION("([/y] = 2 OR [/x] = 1) AND [/z] = 3", "[/x] = 1 OR [/y] = 2");
    EXPECT_IMPLICATION("5 > [/x]", "[/x] < 10");
    EXPECT_IMPLICATION("[/y] = 0", "0 >= [/y]");
    EXPECT_IMPLICATION("[/x] > 2 AND [/x] < 7", "[/x] > 1 AND [/x] < 8");
    EXPECT_IMPLICATION("[/y] != 3 AND [/x] = 5", "[/y] < 4 OR [/x] >= 5");
    EXPECT_IMPLICATION("[/x] <= 0 OR [/x] >= 10", "[/x] < 1 OR [/x] > 9");
    EXPECT_IMPLICATION("([/x] = 1 OR [/x] = 2) AND [/y] != 0", "[/x] >= 1 AND [/x] <= 2");
    EXPECT_IMPLICATION("[/x] IN (5, 10) OR [/x] IN (10, 15)", "[/x] >= 5 AND [/x] <= 15 AND [/x] != 0");
}

TEST(TImplicationCheckerTest, MiscNegative)
{
    EXPECT_NO_IMPLICATION("[/x] = 10", "[/x] = 5");
    EXPECT_NO_IMPLICATION("[/x] > 10", "[/x] < 10");
    EXPECT_NO_IMPLICATION("[/x] <= 5", "[/x] > 5");
    EXPECT_NO_IMPLICATION("[/x] = 3 AND [/y] = 4", "[/x] = 1 OR [/y] = 2");
    EXPECT_NO_IMPLICATION("[/x] > 5 AND [/y] > 10", "[/x] > 5 AND [/y] < 10");
    EXPECT_NO_IMPLICATION("[/x] >= 30 AND [/x] <= 40", "[/x] >= 10 AND [/x] <= 20");
    EXPECT_NO_IMPLICATION("[/x] > 0", "[/x] = 5");
    EXPECT_NO_IMPLICATION("[/x] > 3", "[/x] > 5 AND [/x] < 10");
    EXPECT_NO_IMPLICATION("[/x] >= 3 AND [/x] <= 15", "[/x] >= 5 AND [/x] <= 10");
    EXPECT_NO_IMPLICATION("[/x] = 5 OR [/x] = 10", "[/x] = 5");
    EXPECT_NO_IMPLICATION("([/x] = 5 AND [/z] = 1) OR ([/y] = 10 AND [/z] = 2)", "[/x] = 5 AND [/y] = 10");
    EXPECT_NO_IMPLICATION("([/x] > 10 OR [/z] = 1) AND [/x] != 0", "[/x] > 5");
    EXPECT_NO_IMPLICATION("[/x] IN (1, 2, 3, 4, 5)", "[/x] <= 3");
    EXPECT_NO_IMPLICATION("[/x] >= 5", "[/x] > 5");
    EXPECT_NO_IMPLICATION("[/x] > 4", "[/x] >= 5");
    EXPECT_NO_IMPLICATION("[/x] = 1 OR [/x] = 2 OR [/x] = 3", "([/x] = 1 OR [/x] = 2) AND [/y] > 5");
    EXPECT_NO_IMPLICATION("[/x] > 0", "[/x] != 5");
}

TEST(TImplicationCheckerTest, LexicographicalComparision)
{
    EXPECT_IMPLICATION("([/x], [/y]) = (1, 2)", "[/x] = 1");
    EXPECT_IMPLICATION("([/x], [/y]) = (1, 2)", "[/y] = 2");
    EXPECT_IMPLICATION("([/x], [/y]) = (1, 2)", "([/y], [/x]) = (2, 1)");

    EXPECT_IMPLICATION("(1, 2) = ([/x], [/y])", "1 = [/x]");
    EXPECT_IMPLICATION("(1, 2) = ([/x], [/y])", "2 = [/y]");
    EXPECT_IMPLICATION("(1, 2) = ([/x], [/y])", "(2, 1) = ([/y], [/x])");

    EXPECT_IMPLICATION("([/x], [/y]) > (1, 2)", "[/x] >= 1");
    EXPECT_IMPLICATION("([/x], [/y]) > (1, 2)", "[/x] > 0");
    EXPECT_IMPLICATION("([/x], [/y]) > (1, 2)", "[/x] > 1 OR ([/x] = 1 AND [/y] > 2)");
    EXPECT_IMPLICATION("([/x], [/y]) >= (1, 2)", "[/x] > 1 OR ([/x] = 1 AND [/y] >= 2)");
    EXPECT_IMPLICATION("([/x], [/y]) > (1, 2)", "([/x], [/y]) > (1, 1)");
    EXPECT_IMPLICATION("([/x], [/y]) > (1, 2)", "([/x], [/y]) >= (1, 2)");
    EXPECT_IMPLICATION("([/x], [/y]) > (5, 10)", "([/x], [/y]) > (4, 99)");
    EXPECT_IMPLICATION("([/x], [/y]) >= (1, 2)", "[/x] >= 1");
}

TEST(TImplicationCheckerTest, LexicographicalComparisionThreeElement)
{
    EXPECT_IMPLICATION("([/x], [/y], [/z]) = (1, 2, 3)", "[/x] = 1");
    EXPECT_IMPLICATION("([/x], [/y], [/z]) = (1, 2, 3)", "[/y] = 2 AND [/z] = 3");
    EXPECT_IMPLICATION("([/x], [/y], [/z]) = (1, 2, 3)", "([/z], [/y], [/x]) = (3, 2, 1)");

    EXPECT_IMPLICATION("([/x], [/y], [/z]) > (1, 2, 3)", "[/x] >= 1");
    EXPECT_IMPLICATION("([/x], [/y], [/z]) > (1, 2, 3)", "([/x], [/y], [/z]) >= (1, 2, 3)");
    EXPECT_IMPLICATION("([/x], [/y], [/z]) > (1, 2, 3)", "([/x], [/y], [/z]) > (1, 2, 2)");
    EXPECT_IMPLICATION("([/x], [/y], [/z]) > (1, 2, 3)", "([/x], [/y], [/z]) > (0, 2, 3)");
    EXPECT_IMPLICATION(
        "([/x], [/y], [/z]) > (1, 2, 3)",
        "[/x] > 1 OR ([/x] = 1 AND [/y] > 2) OR ([/x] = 1 AND [/y] = 2 AND [/z] > 3)");

    EXPECT_IMPLICATION("([/x], [/y], [/z]) >= (1, 2, 3)", "([/x], [/y]) >= (1, 2)");

    EXPECT_IMPLICATION("([/x], [/y]) > (1, 2) AND [/z] = 5", "[/x] >= 1");
    EXPECT_IMPLICATION("([/x], [/y]) > (1, 2) AND [/z] = 5", "[/z] = 5");
}

TEST(TImplicationCheckerTest, LexicographicalComparisionNoImplication)
{
    EXPECT_NO_IMPLICATION("([/x], [/y]) = (1, 2)", "[/x] = 2");
    EXPECT_NO_IMPLICATION("([/x], [/y]) = (1, 2)", "[/y] = 1");

    EXPECT_NO_IMPLICATION("([/x], [/y]) > (1, 2)", "[/x] > 1");
    EXPECT_NO_IMPLICATION("([/x], [/y]) > (1, 2)", "[/y] > 2");

    EXPECT_NO_IMPLICATION("([/x], [/y]) > (1, 2)", "([/x], [/y]) > (1, 3)");
    EXPECT_NO_IMPLICATION("([/x], [/y]) > (1, 2)", "([/x], [/y]) >= (1, 3)");

    EXPECT_NO_IMPLICATION("([/x], [/y]) >= (1, 2)", "([/x], [/y]) > (1, 2)");

    EXPECT_NO_IMPLICATION("([/x], [/y], [/z]) = (1, 2, 3)", "[/x] = 2");
    EXPECT_NO_IMPLICATION("([/x], [/y], [/z]) = (1, 2, 3)", "[/y] = 1");

    EXPECT_NO_IMPLICATION("([/x], [/y], [/z]) > (1, 2, 3)", "[/z] > 3");
    EXPECT_NO_IMPLICATION("([/x], [/y], [/z]) > (1, 2, 3)", "([/x], [/y]) > (1, 2)");

    EXPECT_NO_IMPLICATION("([/x], [/y], [/z]) > (1, 2, 3)", "([/x], [/y], [/z]) > (1, 3, 3)");

    EXPECT_NO_IMPLICATION("([/x], [/y], [/z]) >= (1, 2, 3)", "([/x], [/y], [/z]) > (1, 2, 3)");
}

TEST(TImplicationCheckerTest, TrivialPredicates)
{
    EXPECT_IMPLICATION("is_null([/x])", "is_null([/x])");
    EXPECT_IMPLICATION("is_null([/x]) AND [/y] > 4", "is_null([/x])");
    EXPECT_IMPLICATION("not is_null([/x])", "not is_null([/x])");
    EXPECT_IMPLICATION("not is_null([/x])", "not is_null([/x]) OR [/y] > 4");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery::NTests
