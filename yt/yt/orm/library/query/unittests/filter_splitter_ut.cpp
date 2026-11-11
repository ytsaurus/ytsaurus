#include <yt/yt/orm/library/query/filter_splitter.h>

#include <yt/yt/orm/client/misc/error.h>

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

void ParseAndCheck(TExpressionPtr expression, const std::string& source, size_t bound, bool matchesPredicate)
{
    auto parsedExpr = ParseSource(source, NQueryClient::EParseMode::Expression);
    auto* correctExpr = std::get<TExpressionPtr>(parsedExpr->AstHead.Ast);

    ASSERT_EQ(FormatExpression({expression}), FormatExpression({correctExpr}));
    ASSERT_EQ(CheckExpressionByPredicate(expression, Detector(bound)), matchesPredicate);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFilterSplitterTest, JustWorks)
{
    TStringBuf filterQuery = "[c] = 5 AND [nc] = 10";
    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto splitTree = SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression);

    std::vector<std::string> subTrees = {
        "[c] = 5",
        "[nc] = 10"
    };
    ASSERT_TRUE(splitTree.size() == subTrees.size());

    for (size_t i = 0; i < subTrees.size(); ++i) {
        ParseAndCheck(
            splitTree[i],
            subTrees[i],
            /*bound*/ 1,
            /*matchesPredicate*/ i % 2 == 0);
    }
}

TEST(TFilterSplitterTest, AllReferencesMatchPredicate)
{
    auto filter = ParseSource("[f] OR [k]", NQueryClient::EParseMode::Expression);
    auto* filterExpr = std::get<TExpressionPtr>(filter->AstHead.Ast);

    auto [unmatched, matched] = SplitFilterByPredicate(&filter->AstHead, Detector(1), filterExpr);

    EXPECT_EQ(unmatched, nullptr);
    ASSERT_NE(matched, nullptr);
    EXPECT_EQ(FormatExpression({matched}), FormatExpression({filterExpr}));
}

TEST(TFilterSplitterTest, SplitMixedReferences)
{
    auto filter = ParseSource(
        "NOT (NOT (([q] < [value]) AND [ui64]) OR [other] AND [i64] OR NOT (([q] OR [i64]) AND NOT [f]))",
        NQueryClient::EParseMode::Expression);
    auto* filterExpr = std::get<TExpressionPtr>(filter->AstHead.Ast);

    auto [unmatched, matched] = SplitFilterByPredicate(&filter->AstHead, Detector(1), filterExpr);

    ASSERT_NE(unmatched, nullptr);
    ASSERT_NE(matched, nullptr);
    ParseAndCheck(unmatched, "[ui64] AND NOT ([other] AND [i64])", /*bound*/ 1, /*matchesPredicate*/ false);
    ParseAndCheck(matched, "NOT [f] AND (([q] < [value]) AND ([q] OR [i64]))", /*bound*/ 1, /*matchesPredicate*/ true);
}

TEST(TFilterSplitterTest, NestedNot)
{
    TStringBuf filterQuery = "NOT (NOT (NOT [c]))";

    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto splitTree = SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression);

    ASSERT_EQ(splitTree.size(), 1ull);

    ParseAndCheck(splitTree[0], "NOT [c]", 1, true);
}

TEST(TFilterSplitterTest, Complex)
{
    TStringBuf filterQuery = "NOT (NOT ([c] AND [nc]) OR [nc] AND [nc] OR NOT (([c] OR [nc]) AND NOT [c]))";

    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto splitTree = SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression);

    std::vector<std::string> subTrees = {
        "[c]",
        "[nc]",
        "NOT ([nc] AND [nc])",
        "[c] OR [nc]",
        "NOT [c]"
    };
    std::vector<bool> matchesPredicate = {true, false, false, true, true};

    ASSERT_TRUE(splitTree.size() == subTrees.size());

    for (size_t i = 0; i < subTrees.size(); ++i) {
        ParseAndCheck(
            splitTree[i],
            subTrees[i],
            /*bound*/ 1,
            matchesPredicate[i]);
    }
}

TEST(TFilterSplitterTest, Function)
{
    TStringBuf filterQuery = "NOT [nc] AND numeric_to_string([c])";

    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto splitTree = SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression);

    std::vector<std::string> subTrees = {
        "NOT [nc]",
        "numeric_to_string([c])"
    };

    ASSERT_TRUE(splitTree.size() == subTrees.size());

    for (size_t i = 0; i < subTrees.size(); ++i) {
        ParseAndCheck(
            splitTree[i],
            subTrees[i],
            /*bound*/ 1,
            /*matchesPredicate*/ i % 2);
    }
}

TEST(TFilterSplitterTest, TupleOperandIsRejected)
{
    for (TStringBuf filterQuery : {
        "([c], [nc]) AND [nc] = 10",
        "NOT ([c], [nc])",
        "NOT (([c], [nc]) OR [nc] = 10)",
    }) {
        SCOPED_TRACE(filterQuery);
        auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
        auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

        EXPECT_THROW_WITH_ERROR_CODE(
            SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression),
            NClient::EErrorCode::InvalidRequestArguments);
        EXPECT_THROW_WITH_SUBSTRING(
            SplitIntoSubTrees(&parsedQuery->AstHead, queryExpression),
            "scalar operands");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests
