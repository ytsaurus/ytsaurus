#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/orm/client/misc/error.h>

#include <yt/yt/orm/library/query/split_filter.h>

using namespace NYT::NQueryClient::NAst;

namespace NYT::NOrm::NServer::NObjects::NTests {

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr ParseInto(std::string query, TObjectsHolder* holder)
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
    auto result = SplitFilter(filterExpression, hints, &objectsHolder);

    ASSERT_TRUE(result.Where.has_value());
    auto where = FormatExpression(*result.Where);
    EXPECT_EQ(where, "((update_time)>(10))AND((type)=(3))");

    ASSERT_TRUE(result.Having.has_value());
    auto having = FormatExpression(*result.Having);
    EXPECT_EQ(having, "(sum(downloads))>(100)");
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
}

TEST(TSplitFilterTest, HavingOnly)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("sum([type]) = 3", &objectsHolder);
    auto e2 = ParseInto("max([update_time]) > 10", &objectsHolder);
    auto filterExpression = MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{e1},
        TExpressionList{e2})[0];

    TFilterHints hints;
    auto result = SplitFilter(filterExpression, hints, &objectsHolder);

    EXPECT_FALSE(result.Where.has_value());

    ASSERT_TRUE(result.Having.has_value());
    auto having = FormatExpression(*result.Having);
    EXPECT_EQ(having, "((sum(type))=(3))AND((max(update_time))>(10))");
}

TEST(TSplitFilterTest, HavingAndUnkndownInsideFunction)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("[t] = 3", &objectsHolder);
    auto e2 = ParseInto("sum([v]) > 10", &objectsHolder);
    auto andExpression = MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{e1},
        TExpressionList{e2})[0];

    auto filterExpression = MakeExpression<TFunctionExpression>(
        &objectsHolder,
        NQueryClient::NullSourceLocation,
        "some_fn",
        TExpressionList{andExpression})[0];

    TFilterHints hints;
    auto result = SplitFilter(filterExpression, hints, &objectsHolder);


    ASSERT_FALSE(result.Where.has_value());
    ASSERT_TRUE(result.Having.has_value());
    auto having = FormatExpression(*result.Having);
    EXPECT_EQ(having, "some_fn(((t)=(3))AND((sum(v))>(10)))");
}

TEST(TSplitFilterTest, TopLevelLiteral)
{
    TObjectsHolder objectsHolder;
    auto e1 = ParseInto("l.key", &objectsHolder);
    auto e2 = ParseInto("r.value", &objectsHolder);

    auto filterExpression = MakeExpression<TBinaryOpExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        MakeExpression<TBinaryOpExpression>(
            &objectsHolder,
            NQueryClient::TSourceLocation(),
            NQueryClient::EBinaryOp::And,
            MakeExpression<TInExpression>(
                &objectsHolder,
                NQueryClient::TSourceLocation(),
                TExpressionList{e1},
                TLiteralValueTupleList{TLiteralValueTuple{1}, TLiteralValueTuple{2}, TLiteralValueTuple{3}}),
            MakeExpression<TBinaryOpExpression>(
                &objectsHolder,
                NQueryClient::TSourceLocation(),
                NQueryClient::EBinaryOp::GreaterOrEqual,
                MakeExpression<TFunctionExpression>(
                    &objectsHolder,
                    NQueryClient::TSourceLocation(),
                    "sum",
                    TExpressionList{e2}),
                MakeExpression<TLiteralExpression>(
                    &objectsHolder,
                    NQueryClient::TSourceLocation(),
                    100))),
        MakeExpression<TLiteralExpression>(
            &objectsHolder,
            NQueryClient::TSourceLocation(),
            true))[0];

    TFilterHints hints;
    hints.WhereAliases.insert("l");
    auto result = SplitFilter(filterExpression, hints, &objectsHolder);

    ASSERT_TRUE(result.Having.has_value());
    auto having = FormatExpression(*result.Having);
    EXPECT_EQ(having, "(sum(r.value))>=(100)");

    ASSERT_TRUE(result.Where.has_value());
    auto where = FormatExpression(*result.Where);
    EXPECT_EQ(where, "((l.key) IN (1, 2, 3))AND(true)");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NTests
