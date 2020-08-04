#include <yt/core/test_framework/framework.h>

#include <yt/server/clickhouse_server/computed_columns.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>

#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TLogger Logger("Test");

DB::SharedContextHolder SharedContext = DB::Context::createShared();

class TComputedColumnPredicatePopulationTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<TTableSchemaPtr, TString, TString>>
{
protected:
    virtual void SetUp()
    {
        GlobalContext_ = DB::Context::createGlobal(SharedContext.get());
    }

    DB::Context CreateContext() const
    {
        YT_VERIFY(GlobalContext_);
        auto context = *GlobalContext_;
        context.makeQueryContext();
        return context;
    }

private:
    DB::SharedContextHolder SharedContext_;
    std::optional<DB::Context> GlobalContext_;
};

TEST_P(TComputedColumnPredicatePopulationTest, Test)
{
    const auto& [schema, originalPredicate, expectedPredicate] = GetParam();

    DB::ParserExpressionWithOptionalAlias parser(false);
    auto originalAst = DB::parseQuery(parser, originalPredicate, 0 /* maxQuerySize */, 0 /* maxQueryDepth */);
    auto resultAst = PopulatePredicateWithComputedColumns(originalAst, schema, CreateContext(), Logger);
    auto resultPredicate = TString(DB::serializeAST(*resultAst));
    EXPECT_EQ(expectedPredicate, resultPredicate);
}

INSTANTIATE_TEST_SUITE_P(
    Test,
    TComputedColumnPredicatePopulationTest,
    ::testing::Values(
        std::make_tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Int64, ESortOrder::Ascending)
                    .SetExpression("2 * key"),
                TColumnSchema("key", EValueType::Int64)
            }),
            "key = 5",
            "(key = 5) AND (computed_key = 10)"),
        std::make_tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Int64, ESortOrder::Ascending)
                    .SetExpression("2 * key"),
                TColumnSchema("key", EValueType::Int64)
            }),
            "(key = (7 * key)) OR (NOT (key = 5)) OR (key > 7)",
            "(key = (7 * key)) OR (NOT ((key = 5) AND (computed_key = 10))) OR (key > 7)"),
        std::make_tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("farm_hash(key)"),
                TColumnSchema("key", EValueType::String)
            }),
            "key = 'foo'",
            "(key = 'foo') AND (computed_key = 7945198393224481366)"),
        // Mistake should leave occurrence as is.
        std::make_tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key / (key - 2)"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "(key = 2) OR (3 = key) OR (key = (2 + 2))",
            "0 OR ((3 = key) AND (computed_key = 3)) OR ((key = (2 + 2)) AND (computed_key = 2))"),
        // Do not go inside subqueries.
        std::make_tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 2"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "key IN (SELECT * FROM T WHERE key = 42)",
            "key IN ((SELECT * FROM T WHERE key = 42) AS _subquery1)"),
        std::make_tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key1", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 2"),
                TColumnSchema("computed_key2", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 3"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "key = 5",
            "(key = 5) AND (computed_key1 = 10) AND (computed_key2 = 15)"),
        // TODO(max42): CHYT-438.
        // Should become "(key1 = 5) AND (key2 = 10) AND (computed_key = 15)".
        std::make_tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key1 + key2"),
                TColumnSchema("key1", EValueType::Uint64),
                TColumnSchema("key2", EValueType::Uint64)
            }),
            "(key1 = 5) AND (key2 = 10)",
            "(key1 = 5) AND (key2 = 10)"),
        // TODO(max42): CHYT-438.
        // Should become "((key, computed_key) IN ((2, 4), (3, 6))".
        std::make_tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 2"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "key IN (2, 3)",
            "key IN (2, 3)"),
        // Not a key column.
        std::make_tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Int64)
                    .SetExpression("2 * key"),
                TColumnSchema("key", EValueType::Int64)
            }),
            "key = 5",
            "key = 5"),
        std::make_tuple(New<TTableSchema>(std::vector<TColumnSchema>{}), "1", "1")
        ));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

