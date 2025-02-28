#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/orm/client/misc/error.h>

#include <yt/yt/orm/library/query/unaggregated_column_detector.h>

using namespace NYT::NQueryClient::NAst;

namespace NYT::NOrm::NServer::NObjects::NTests {

////////////////////////////////////////////////////////////////////////////////

namespace {

bool HasUnaggregatedColumn(TStringBuf query)
{
    auto parsed = ParseSource(query, NQueryClient::EParseMode::Expression);
    bool result = NObjects::HasUnaggregatedColumn(std::get<TExpressionPtr>(parsed->AstHead.Ast));
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TUnaggregatedColumnDetectorTest, Literal)
{
    ASSERT_FALSE(HasUnaggregatedColumn("1"));
}

TEST(TUnaggregatedColumnDetectorTest, ColumnReference)
{
    ASSERT_TRUE(HasUnaggregatedColumn("p.[meta.id]"));
}

TEST(TUnaggregatedColumnDetectorTest, FunctionOfColumnReference)
{
    ASSERT_TRUE(HasUnaggregatedColumn("yson_length(any_to_yson_string(p.[meta.etc]))"));
}

TEST(TUnaggregatedColumnDetectorTest, AggregationOfColumnReference)
{
    ASSERT_FALSE(HasUnaggregatedColumn("first(p.[meta.etc])"));
}

TEST(TUnaggregatedColumnDetectorTest, AggregationInsideFunction)
{
    auto query = "if(first(p.[meta.value]) = 1, first(p.[meta.value2]), first(p.[meta.value3])) as result";
    ASSERT_FALSE(HasUnaggregatedColumn(query));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NTests
