#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/orm/client/misc/error.h>

#include <yt/yt/orm/library/query/split_filter.h>

using namespace NYT::NQueryClient::NAst;

namespace NYT::NOrm::NServer::NObjects::NTests {

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr ParseInto(TString query, TObjectsHolder* holder)
{
    auto parsed = ParseSource(query, NQueryClient::EParseMode::Expression);
    auto expr = std::get<TExpressionPtr>(parsed->AstHead.Ast);
    holder->Merge(std::move(parsed->AstHead));
    return expr;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSplitFilterTest, Split)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[type] = 3", &objectsHolder);
    auto e2 = ParseInto("[update_time] > 10", &objectsHolder);
    auto e3 = ParseInto("sum([downloads]) > 100", &objectsHolder);
    auto filterExpression = MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{e1},
        MakeExpression<TBinaryOpExpression>(
            &objectsHolder,
            NQueryClient::TSourceLocation(),
            NQueryClient::EBinaryOp::And,
            TExpressionList{e2},
            TExpressionList{e3}))[0];

    TFilterHints hints;
    hints.Having.insert(e3->As<TBinaryOpExpression>()->Lhs[0]);
    hints.JoinPredicates[e2->As<TBinaryOpExpression>()->Lhs[0]] = "r1";
    auto result = SplitFilter(filterExpression, hints, &objectsHolder);

    ASSERT_TRUE(result.Where.has_value());
    auto where = FormatExpression(*result.Where);
    EXPECT_EQ(where, "(type)=(3)");

    ASSERT_TRUE(result.Having.has_value());
    auto having = FormatExpression(*result.Having);
    EXPECT_EQ(having, "(sum(downloads))>(100)");

    ASSERT_TRUE(result.JoinPredicates.contains("r1"));
    ASSERT_EQ(result.JoinPredicates.size(), 1u);
    auto joinOnExpr = result.JoinPredicates["r1"];
    ASSERT_TRUE(joinOnExpr.has_value());
    auto joinOn = FormatExpression(*joinOnExpr);
    EXPECT_EQ(joinOn, "(update_time)>(10)");
}

TEST(TSplitFilterTest, HeterogenousBinaryOp)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[type] = 3", &objectsHolder);
    auto e2 = ParseInto("[update_time] > 10", &objectsHolder);
    auto filterExpression = MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::Or,
        TExpressionList{e1},
        TExpressionList{e2})[0];

    TFilterHints hints;
    hints.Having.insert(e2->As<TBinaryOpExpression>()->Lhs[0]);
    ASSERT_THROW(SplitFilter(filterExpression, hints, &objectsHolder), TErrorException);
}

TEST(TSplitFilterTest, HeterogenousAlias)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[type] = 3", &objectsHolder);
    auto e2 = ParseInto("[update_time] > 10", &objectsHolder);
    auto filterExpression = MakeExpression<TAliasExpression>(
        &objectsHolder,
        NQueryClient::NullSourceLocation,
        MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{e1},
        TExpressionList{e2})[0],
        "name")[0];

    TFilterHints hints;
    hints.Having.insert(e2->As<TBinaryOpExpression>()->Lhs[0]);
    ASSERT_THROW(SplitFilter(filterExpression, hints, &objectsHolder), TErrorException);
}

TEST(TSplitFilterTest, HeterogenousUnaryOp)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[type] = 3", &objectsHolder);
    auto e2 = ParseInto("[update_time] > 10", &objectsHolder);
    auto filterExpression = MakeExpression<TUnaryOpExpression>(
        &objectsHolder,
        NQueryClient::NullSourceLocation,
        NQueryClient::EUnaryOp::Minus,
        MakeExpression<TBinaryOpExpression>(
            &objectsHolder,
            NQueryClient::TSourceLocation(),
            NQueryClient::EBinaryOp::And,
            TExpressionList{e1},
            TExpressionList{e2}))[0];

    TFilterHints hints;
    hints.Having.insert(e2->As<TBinaryOpExpression>()->Lhs[0]);
    ASSERT_THROW(SplitFilter(filterExpression, hints, &objectsHolder), TErrorException);
}

TEST(TSplitFilterTest, HeterogenousFunctionArguments)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[type] = 3", &objectsHolder);
    auto e2 = ParseInto("[update_time] > 10", &objectsHolder);
    auto filterExpression = MakeExpression<TFunctionExpression>(
        &objectsHolder,
        NQueryClient::NullSourceLocation,
        "farm_hash",
        TExpressionList{e1, e2})[0];

    TFilterHints hints;
    hints.Having.insert(e2->As<TBinaryOpExpression>()->Lhs[0]);
    ASSERT_THROW(SplitFilter(filterExpression, hints, &objectsHolder), TErrorException);
}

TEST(TSplitFilterTest, WhereOnly)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[type] = 3", &objectsHolder);
    auto e2 = ParseInto("[update_time] > 10", &objectsHolder);
    auto filterExpression = MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{e1},
        TExpressionList{e2})[0];

    TFilterHints hints;
    auto result = SplitFilter(filterExpression, hints, &objectsHolder);

    ASSERT_TRUE(result.Where.has_value());
    auto where = FormatExpression(*result.Where);
    EXPECT_EQ(where, "((type)=(3))AND((update_time)>(10))");

    EXPECT_FALSE(result.Having.has_value());
    EXPECT_TRUE(result.JoinPredicates.empty());
}

TEST(TSplitFilterTest, HavingOnly)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[type] = 3", &objectsHolder);
    auto e2 = ParseInto("[update_time] > 10", &objectsHolder);
    auto filterExpression = MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{e1},
        TExpressionList{e2})[0];

    TFilterHints hints;
    hints.Having.insert(e1->As<TBinaryOpExpression>()->Lhs[0]);
    hints.Having.insert(e2->As<TBinaryOpExpression>()->Lhs[0]);
    auto result = SplitFilter(filterExpression, hints, &objectsHolder);

    EXPECT_FALSE(result.Where.has_value());
    EXPECT_TRUE(result.JoinPredicates.empty());

    ASSERT_TRUE(result.Having.has_value());
    auto having = FormatExpression(*result.Having);
    EXPECT_EQ(having, "((type)=(3))AND((update_time)>(10))");
}

TEST(TSplitFilterTest, JoinOnly)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[type] = 3", &objectsHolder);
    auto e2 = ParseInto("[update_time] > 10", &objectsHolder);
    auto filterExpression = MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{e1},
        TExpressionList{e2})[0];

    TFilterHints hints;
    hints.JoinPredicates[e1->As<TBinaryOpExpression>()->Lhs[0]] = "r1";
    hints.JoinPredicates[e2->As<TBinaryOpExpression>()->Lhs[0]] = "r1";
    auto result = SplitFilter(filterExpression, hints, &objectsHolder);

    EXPECT_FALSE(result.Where.has_value());
    EXPECT_FALSE(result.Having.has_value());

    ASSERT_EQ(result.JoinPredicates.size(), 1u);
    auto joinExpr = result.JoinPredicates.at("r1");
    auto join = FormatExpression(*joinExpr);
    EXPECT_EQ(join, "((type)=(3))AND((update_time)>(10))");
}

TEST(TSplitFilterTest, TwoJoins)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[type] = 3", &objectsHolder);
    auto e2 = ParseInto("[update_time] > 10", &objectsHolder);
    auto filterExpression = MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{e1},
        TExpressionList{e2})[0];

    TFilterHints hints;
    hints.JoinPredicates[e1->As<TBinaryOpExpression>()->Lhs[0]] = "r1";
    hints.JoinPredicates[e2->As<TBinaryOpExpression>()->Lhs[0]] = "r2";
    auto result = SplitFilter(filterExpression, hints, &objectsHolder);

    EXPECT_FALSE(result.Where.has_value());
    EXPECT_FALSE(result.Having.has_value());

    ASSERT_EQ(result.JoinPredicates.size(), 2u);
    auto joinExpr1 = result.JoinPredicates.at("r1");
    auto join1 = FormatExpression(*joinExpr1);
    EXPECT_EQ(join1, "(type)=(3)");
    auto joinExpr2 = result.JoinPredicates.at("r2");
    auto join2 = FormatExpression(*joinExpr2);
    EXPECT_EQ(join2, "(update_time)>(10)");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NTests
