#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/select_literals.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TSelectPlaceholdersTest, BindsTypedValues)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("hash", EValueType::Uint64),
        TColumnSchema("word", EValueType::String),
        TColumnSchema("shard", EValueType::Int64),
    });

    TSelectPlaceholders placeholders;
    EXPECT_EQ(
        placeholders.BindKeyBound(*schema, ">=", "lower", MakeKey(ui64{5}, "x", i64{-3})),
        "(hash,word,shard) >= ({lower_0},{lower_1},{lower_2})");
    EXPECT_EQ(placeholders.Bind("limit", MakeUnversionedInt64Value(10)), "{limit}");

    auto map = NYTree::ConvertTo<NYTree::IMapNodePtr>(placeholders.Build());
    EXPECT_EQ(std::ssize(map->GetChildren()), 4);
    EXPECT_EQ(map->GetChildOrThrow("lower_0")->GetType(), NYTree::ENodeType::Uint64);
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("lower_0"), 5u);
    EXPECT_EQ(map->GetChildOrThrow("lower_1")->GetType(), NYTree::ENodeType::String);
    EXPECT_EQ(map->GetChildValueOrThrow<std::string>("lower_1"), "x");
    EXPECT_EQ(map->GetChildOrThrow("lower_2")->GetType(), NYTree::ENodeType::Int64);
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("lower_2"), -3);
    EXPECT_EQ(map->GetChildOrThrow("limit")->GetType(), NYTree::ENodeType::Int64);
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("limit"), 10);
}

TEST(TSelectPlaceholdersTest, BindsNull)
{
    TSelectPlaceholders placeholders;
    EXPECT_EQ(placeholders.Bind("nothing", MakeUnversionedNullValue()), "{nothing}");

    auto map = NYTree::ConvertTo<NYTree::IMapNodePtr>(placeholders.Build());
    EXPECT_EQ(map->GetChildOrThrow("nothing")->GetType(), NYTree::ENodeType::Entity);
}

//! Parses |query| and renders its AST, so that a parameterized query can be
//! compared against the same query written with literals.
std::string ParseAndFormat(
    const std::string& query,
    const NYson::TYsonString& placeholderValues = {})
{
    auto parsed = NQueryClient::ParseSource(query, NQueryClient::EParseMode::Query, placeholderValues);
    return NQueryClient::NAst::FormatQuery(
        std::get<NQueryClient::NAst::TQuery>(parsed->AstHead.Ast));
}

TEST(TSelectPlaceholdersTest, QueryParsesToTheSameAstAsLiterals)
{
    TSelectPlaceholders placeholders;
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("a", EValueType::Uint64),
        TColumnSchema("b", EValueType::String),
    });
    auto query = Format("(a,b) FROM [//t] WHERE %v ORDER BY (a,b) LIMIT %v",
        placeholders.BindKeyBound(*schema, ">", "offset", MakeKey(ui64{1}, "y")),
        placeholders.Bind("limit", MakeUnversionedInt64Value(7)));

    EXPECT_EQ(
        ParseAndFormat(query, placeholders.Build()),
        ParseAndFormat(R"((a,b) FROM [//t] WHERE (a,b) > (1u,"y") ORDER BY (a,b) LIMIT 7)"));
}

TEST(TSelectPlaceholdersTest, NullBoundParsesToNullLiteral)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("a", EValueType::Uint64),
        TColumnSchema("b", EValueType::String),
    });

    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedUint64Value(1));
    builder.AddValue(MakeUnversionedNullValue());

    TSelectPlaceholders placeholders;
    auto query = Format("(a,b) FROM [//t] WHERE %v",
        placeholders.BindKeyBound(*schema, ">", "offset", TKey(TKey::TUnderlying(builder.FinishRow()))));

    EXPECT_EQ(
        ParseAndFormat(query, placeholders.Build()),
        ParseAndFormat(R"((a,b) FROM [//t] WHERE (a,b) > (1u,null))"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
