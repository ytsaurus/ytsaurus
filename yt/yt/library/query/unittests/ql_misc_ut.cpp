#include "ql_helpers.h"

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

// Tests:
// TSelfifyEvaluatedExpressionTest

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSelfifyEvaluatedExpressionTest, HashExpression)
{
    StrictMock<TPrepareCallbacksMock> PrepareMock;

    TDataSplit alphaSplit{.TableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("a", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("b", EValueType::String),
    })};
    EXPECT_CALL(PrepareMock, GetInitialSplit("//Alpha"))
        .WillOnce(Return(MakeFuture(alphaSplit)));

    TDataSplit betaSplit{.TableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("hash", EValueType::Uint64)
            .SetExpression("farm_hash(key)")
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("key", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("value", EValueType::String),
    })};
    EXPECT_CALL(PrepareMock, GetInitialSplit("//Beta"))
        .WillOnce(Return(MakeFuture(betaSplit)));

    auto query = PreparePlanFragment(&PrepareMock, "* from [//Alpha] join [//Beta] on a + 6 = key")->Query;

    const auto& joinClause = query->JoinClauses.front();
    ASSERT_EQ(joinClause->SelfEquations.size(), 2ul);
    const auto& evaluatedEquation = joinClause->SelfEquations[0];

    ASSERT_TRUE(evaluatedEquation.Evaluated);

    auto selfifiedExpr = TSelfifyRewriter{.JoinClause = joinClause}.Visit(evaluatedEquation.Expression);

    auto* function = selfifiedExpr->As<TFunctionExpression>();
    ASSERT_TRUE(function);
    EXPECT_EQ(function->FunctionName, "farm_hash");

    EXPECT_EQ(function->Arguments.front(), joinClause->SelfEquations[1].Expression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
