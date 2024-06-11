#include "ql_helpers.h"

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/library/query/engine_api/top_collector.h>

#include <util/random/shuffle.h>

// Tests:
// TSelfifyEvaluatedExpressionTest
// TTopCollectorTest

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

char Compare(const TPIValue* first, const TPIValue* second)
{
    return first[0].Data.Int64 < second[0].Data.Int64;
}

TEST(TTopCollectorTest, Simple)
{
    const int dataLength = 100;
    const int resultLength = 20;

    auto data = std::vector<TPIValue>(dataLength);
    for (int i = 0; i < dataLength; ++i) {
        data[i].Type = EValueType::Int64;
        data[i].Data.Int64 = dataLength - i - 1;
    }

    auto topCollector = TTopCollector(
        resultLength,
        NWebAssembly::PrepareFunction(&Compare),
        /*rowSize*/ 1,
        GetDefaultMemoryChunkProvider());

    for (int i = 0; i < dataLength; ++i) {
        topCollector.AddRow(&data[i]);
    }

    auto sortResult = topCollector.GetRows();
    for (int i = 0; i < resultLength; ++i) {
        EXPECT_EQ(sortResult[i][0].Type, EValueType::Int64);
        EXPECT_EQ(sortResult[i][0].Data.Int64, i);
    }
}

TEST(TTopCollectorTest, Shuffle)
{
    const int dataLength = 2000;
    const int resultLength = 100;

    auto data = std::vector<TValue>(dataLength);
    for (int i = 0; i < dataLength; i += 2) {
        data[i].Type = EValueType::Int64;
        data[i].Data.Int64 = i;
        data[i+1].Type = EValueType::Int64;
        data[i+1].Data.Int64 = i;
    }

    for (int i = 0; i < 1000; ++i) {
        Shuffle(data.begin(), data.end());

        auto topCollector = TTopCollector(
            resultLength,
            NWebAssembly::PrepareFunction(&Compare),
            /*rowSize*/ 1,
            GetDefaultMemoryChunkProvider());

        for (int i = 0; i < dataLength; ++i) {
            topCollector.AddRow(std::bit_cast<TPIValue*>(&data[i]));
        }

        auto sortResult = topCollector.GetRows();
        for (int i = 0; i < resultLength; i += 2) {
            EXPECT_EQ(sortResult[i][0].Type, EValueType::Int64);
            EXPECT_EQ(sortResult[i][0].Data.Int64, i);
            EXPECT_EQ(sortResult[i+1][0].Type, EValueType::Int64);
            EXPECT_EQ(sortResult[i+1][0].Data.Int64, i);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
