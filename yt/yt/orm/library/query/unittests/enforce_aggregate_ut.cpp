#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/orm/client/misc/error.h>

#include <yt/yt/orm/library/query/enforce_aggregate.h>

using namespace NYT::NQueryClient::NAst;

namespace NYT::NOrm::NServer::NObjects::NTests {

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string MakeAggregatedQuery(TStringBuf query)
{
    auto parsed = ParseSource(query, NQueryClient::EParseMode::Expression);
    auto* expression = std::get<TExpressionPtr>(parsed->AstHead.Ast);
    auto* aggregatedExpression = EnforceAggregate(&parsed->AstHead, expression);
    return FormatExpression(*aggregatedExpression);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TEnforceAggregateTest, Literal)
{
    ASSERT_EQ(
        MakeAggregatedQuery("1"),
        "1");
}

TEST(TEnforceAggregateTest, ColumnReference)
{
    ASSERT_EQ(
        MakeAggregatedQuery("p.`meta.id`"),
        "first(p.`meta.id`)");
}

TEST(TEnforceAggregateTest, FunctionOfColumnReference)
{
    ASSERT_EQ(
        MakeAggregatedQuery("yson_length(any_to_yson_string(p.`meta.etc`))"),
        "yson_length(any_to_yson_string(first(p.`meta.etc`)))");
}

TEST(TEnforceAggregateTest, AggregationOfColumnReference)
{
    ASSERT_EQ(
        MakeAggregatedQuery("first(p.`meta.etc`)"),
        "first(p.`meta.etc`)");
}

TEST(TEnforceAggregateTest, AggregationInsideFunction)
{
    auto query = "(if((first(p.`meta.value`))=(1), first(p.`meta.value2`), first(p.`meta.value3`)) as result)";
    ASSERT_EQ(
        MakeAggregatedQuery(query),
        query);
}

TEST(TEnforceAggregateTest, AggregatedAndUnaggregatedInSingleExpression)
{
    ASSERT_EQ(
        MakeAggregatedQuery("sum(p.x) / p.y"),
        "(sum(p.x))/(first(p.y))");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NTests
