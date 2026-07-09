#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TKeyBuilderTest
    : public ::testing::Test
{
protected:
    NQueryClient::IColumnEvaluatorCachePtr EvaluatorCache_ =
        NQueryClient::CreateColumnEvaluatorCache(New<NQueryClient::TColumnEvaluatorCacheConfig>());
    NLogging::TLogger Logger;

    static TTableSchemaPtr ParseSchema(TStringBuf yson)
    {
        return ConvertTo<TTableSchemaPtr>(TYsonString(yson));
    }

    TKey Build(TStringBuf yson, const TTableSchemaPtr& schema)
    {
        return BuildKeyFromYson(TYsonString(yson), schema, EvaluatorCache_, Logger);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyBuilderTest, ListFormPassesThrough)
{
    // Positional form: ConvertTo<TKey> deserializes the list as-is, no schema lookup.
    auto schema = ParseSchema(R"([{name="a"; type="int64"; sort_order="ascending"}])");
    auto key = Build("[42]", schema);
    EXPECT_EQ(key.Underlying().GetCount(), 1);
    EXPECT_EQ(key.Underlying()[0].Type, EValueType::Int64);
    EXPECT_EQ(key.Underlying()[0].Data.Int64, 42);
}

TEST_F(TKeyBuilderTest, EmptySchemaEmptyMap)
{
    auto schema = ParseSchema(R"([])");
    auto key = Build("{}", schema);
    EXPECT_EQ(key.Underlying().GetCount(), 0);
}

TEST_F(TKeyBuilderTest, MapWithTwoSimpleColumns)
{
    auto schema = ParseSchema(R"([
        {name="a"; type="int64"; sort_order="ascending"};
        {name="b"; type="string"; sort_order="ascending"};
    ])");
    auto key = Build(R"({a=1; b="x"})", schema);
    EXPECT_EQ(key.Underlying().GetCount(), 2);
    EXPECT_EQ(key.Underlying()[0].Type, EValueType::Int64);
    EXPECT_EQ(key.Underlying()[0].Data.Int64, 1);
    EXPECT_EQ(key.Underlying()[1].Type, EValueType::String);
    EXPECT_EQ(key.Underlying()[1].AsStringBuf(), "x");
}

TEST_F(TKeyBuilderTest, MapColumnOrderIndependent)
{
    // Schema order is "a" then "b"; YSON map order is "b" then "a". Output must follow schema.
    auto schema = ParseSchema(R"([
        {name="a"; type="int64"; sort_order="ascending"};
        {name="b"; type="string"; sort_order="ascending"};
    ])");
    auto key = Build(R"({b="x"; a=1})", schema);
    EXPECT_EQ(key.Underlying()[0].Data.Int64, 1);
    EXPECT_EQ(key.Underlying()[1].AsStringBuf(), "x");
}

TEST_F(TKeyBuilderTest, MissingRequiredColumnThrows)
{
    auto schema = ParseSchema(R"([
        {name="a"; type="int64"; sort_order="ascending"};
        {name="b"; type="string"; sort_order="ascending"};
    ])");
    EXPECT_THROW_WITH_SUBSTRING(Build(R"({a=1})", schema), "Missing column");
}

TEST_F(TKeyBuilderTest, UnknownColumnThrows)
{
    auto schema = ParseSchema(R"([
        {name="a"; type="int64"; sort_order="ascending"};
    ])");
    EXPECT_THROW_WITH_SUBSTRING(Build(R"({a=1; unknown=2})", schema), "unknown");
}

TEST_F(TKeyBuilderTest, NeitherListNorMapThrows)
{
    auto schema = ParseSchema(R"([])");
    EXPECT_THROW_WITH_SUBSTRING(Build("42", schema), "must be a list or a map");
}

TEST_F(TKeyBuilderTest, ExpressionColumnComputed)
{
    // Single non-expression column ``order_id``; expression column ``hash`` precedes it. The
    // evaluator must fill ``hash`` from ``farm_hash(order_id)``. Magic value 2044001940267648219
    // is reused from schema_ut.
    auto schema = ParseSchema(R"yson([
        {name="hash"; type="uint64"; expression="farm_hash(order_id)"; sort_order="ascending"};
        {name="order_id"; type="uint64"; sort_order="ascending"};
    ])yson");
    auto key = Build(R"({order_id=456u})", schema);
    EXPECT_EQ(key.Underlying().GetCount(), 2);
    EXPECT_EQ(key.Underlying()[0].Type, EValueType::Uint64);
    EXPECT_EQ(key.Underlying()[0].Data.Uint64, 2044001940267648219ull);
    EXPECT_EQ(key.Underlying()[1].Data.Uint64, 456ull);
}

TEST_F(TKeyBuilderTest, AnyKeyExpressionViaYsonString)
{
    // The "key" column is type "any" so a tuple (int + string) is a legal value. farm_hash
    // does not accept "any" directly, so the schema funnels it through any_to_yson_string,
    // which gives the canonical YSON encoding of the value to hash.
    auto schema = ParseSchema(R"yson([
        {name="hash"; type="uint64"; expression="farm_hash(any_to_yson_string(key))"; sort_order="ascending"};
        {name="key"; type="any"; sort_order="ascending"};
    ])yson");
    auto key = Build(R"({key=[1; "value"]})", schema);
    EXPECT_EQ(key.Underlying().GetCount(), 2);
    EXPECT_EQ(key.Underlying()[0].Type, EValueType::Uint64);
    // Magic value: farm_hash over the canonical binary YSON encoding of [1; "value"]; pinned
    // here so that any silent change in YSON encoding or the farm_hash routine surfaces.
    EXPECT_EQ(key.Underlying()[0].Data.Uint64, 5074273704489249393ull);

    EXPECT_EQ(key.Underlying()[1].Type, EValueType::Any);
    // The "any" column must round-trip back to the original [1; "value"] tuple.
    auto decoded = ConvertToNode(TYsonString(key.Underlying()[1].AsStringBuf()));
    ASSERT_EQ(decoded->GetType(), ENodeType::List);
    auto items = decoded->AsList()->GetChildren();
    ASSERT_EQ(std::ssize(items), 2);
    EXPECT_EQ(items[0]->AsInt64()->GetValue(), 1);
    EXPECT_EQ(items[1]->AsString()->GetValue(), "value");

    // Differing input must produce a different hash.
    auto otherKey = Build(R"({key=[1; "other"]})", schema);
    EXPECT_NE(otherKey.Underlying()[0].Data.Uint64, key.Underlying()[0].Data.Uint64);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
