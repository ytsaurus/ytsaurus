#include <yt/yt/orm/library/query/computed_fields_filter.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NOrm::NQuery::NTests {
namespace {

using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

auto Detector(size_t bound)
{
    return [bound] (TReferenceExpressionPtr referenceExpr) {
        return referenceExpr->Reference.ColumnName.size() <= bound;
    };
}

void ParseAndCheck(TExpressionPtr expression, const TString& source, size_t bound, bool isComputed)
{
    auto parsedExpr = ParseSource(source, NQueryClient::EParseMode::Expression);
    auto* correctExpr = std::get<TExpressionPtr>(parsedExpr->AstHead.Ast);

    ASSERT_EQ(FormatExpression({expression}), FormatExpression({correctExpr}));
    ASSERT_EQ(ContainsComputedFields(expression, Detector(bound)), isComputed);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFilterSplitterTest, JustWorks)
{
    TString filterQuery = "[c] = 5 AND [nc] = 10";
    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto splitTree = SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression);

    std::vector<TString> subTrees = {
        "[c] = 5",
        "[nc] = 10"
    };
    ASSERT_TRUE(splitTree.size() == subTrees.size());

    for (size_t i = 0; i < subTrees.size(); ++i) {
        ParseAndCheck(
            splitTree[i],
            subTrees[i],
            /*bound*/ 1,
            /*isComputed*/ i % 2 == 0);
    }
}

TEST(TFilterSplitterTest, NestedNot)
{
    TString filterQuery = "NOT (NOT (NOT [c]))";

    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto splitTree = SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression);

    ASSERT_TRUE(splitTree.size() == 1);

    ParseAndCheck(splitTree[0], "NOT [c]", 1, true);
}

TEST(TFilterSplitterTest, Complex)
{
    TString filterQuery = "NOT (NOT ([c] AND [nc]) OR [nc] AND [nc] OR NOT (([c] OR [nc]) AND NOT [c]))";

    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto splitTree = SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression);

    std::vector<TString> subTrees = {
        "[c]",
        "[nc]",
        "NOT ([nc] AND [nc])",
        "[c] OR [nc]",
        "NOT [c]"
    };
    std::vector<bool> isComputed = {true, false, false, true, true};

    ASSERT_TRUE(splitTree.size() == subTrees.size());

    for (size_t i = 0 ; i < subTrees.size(); ++i) {
        ParseAndCheck(
            splitTree[i],
            subTrees[i],
            /*bound*/ 1,
            isComputed[i]);
    }
}

TEST(TFilterSplitterTest, Function)
{
    TString filterQuery = "NOT [nc] AND numeric_to_string([c])";

    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto splitTree = SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression);

    std::vector<TString> subTrees = {
        "NOT [nc]",
        "numeric_to_string([c])"
    };

    ASSERT_TRUE(splitTree.size() == subTrees.size());

    for (size_t i = 0 ; i < subTrees.size(); ++i) {
        ParseAndCheck(
            splitTree[i],
            subTrees[i],
            /*bound*/ 1,
            /*isComputed*/ i % 2);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests
