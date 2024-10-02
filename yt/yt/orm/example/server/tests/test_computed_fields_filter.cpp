#include "common.h"

#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>

#include <yt/yt/orm/library/query/computed_fields_filter.h>

#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NOrm::NServer::NObjects;
using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

TEST(TFilterSplitterTest, OneEmpty)
{
    auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();

    auto queryContext = MakeQueryContext(
        transaction->GetBootstrap(),
        TObjectTypeValues::MultipolicyId,
        transaction->GetSession(),
        /*indexSchema*/ nullptr,
        /*allowAnnotations*/ false);

    TString expression = "[/meta/fqid] OR [/meta/key]";

    auto filter = ParseSource(expression, NQueryClient::EParseMode::Expression);
    auto filterExpr = std::get<TExpressionPtr>(filter->AstHead.Ast);
    queryContext->Merge(std::move(filter->AstHead));

    auto [nonComputedTree, computedTree] = NQuery::SplitFilter(
        queryContext.get(),
        MakeComputedFieldsDetector(queryContext.get()),
        filterExpr);

    ASSERT_TRUE(nonComputedTree == nullptr);
    ASSERT_TRUE(computedTree != nullptr);

    ASSERT_EQ(FormatExpression({filterExpr}), FormatExpression({computedTree}));
}

TEST(TFilterSplitterTest, Complex)
{
    auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();

    auto queryContext = MakeQueryContext(
        transaction->GetBootstrap(),
        TObjectTypeValues::MultipolicyId,
        transaction->GetSession(),
        /*indexSchema*/ nullptr,
        /*allowAnnotations*/ false);

    TString expression = "NOT (NOT (([/meta/ultimate_question_of_life] < [/spec/ui64_value]) AND [/meta/ui64_id]) OR [/spec/i64_value] AND [/meta/i64_id] "
                         "OR NOT (([/meta/ultimate_question_of_life] OR [/meta/i64_id]) AND NOT [/meta/fqid]))";

    auto filter = ParseSource(expression, NQueryClient::EParseMode::Expression);
    auto filterExpr = std::get<TExpressionPtr>(filter->AstHead.Ast);
    queryContext->Merge(std::move(filter->AstHead));

    auto [nonComputedTree, computedTree] = NQuery::SplitFilter(
        queryContext.get(),
        MakeComputedFieldsDetector(queryContext.get()),
        filterExpr);

    ASSERT_TRUE(nonComputedTree != nullptr);
    ASSERT_TRUE(computedTree != nullptr);

    TString computedPart = "NOT [/meta/fqid] AND (([/meta/ultimate_question_of_life] < [/spec/ui64_value]) AND ([/meta/ultimate_question_of_life] OR [/meta/i64_id]))";
    TString nonComputedPart = "[/meta/ui64_id] AND NOT ([/spec/i64_value] AND [/meta/i64_id])";

    auto computedPartSource = ParseSource(computedPart, NQueryClient::EParseMode::Expression);
    auto computedPartExpr = std::get<TExpressionPtr>(computedPartSource->AstHead.Ast);

    auto nonComputedPartSource = ParseSource(nonComputedPart, NQueryClient::EParseMode::Expression);
    auto nonComputedPartExpr = std::get<TExpressionPtr>(nonComputedPartSource->AstHead.Ast);

    ASSERT_EQ(FormatExpression({nonComputedPartExpr}), FormatExpression({nonComputedTree}));
    ASSERT_EQ(FormatExpression({computedPartExpr}), FormatExpression({computedTree}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
