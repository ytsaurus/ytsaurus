#include <yt/yt/core/test_framework/framework.h>

#include <yt/chyt/server/computed_columns.h>
#include <yt/chyt/server/config.h>

#include <Interpreters/Context.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger("Test");

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

// NOTE(dakovalkov): SharedContextPart is a singletone. Creating it multiple times leads to std::terminate().
// Storing and initializing SharedContextHolder as a global variable is also a bad idea:
// DB::Context::createShared() uses some global variables from other compilation units,
// and an initialization order of such variables is unspecified.

// NOTE(dakovalkov): Can be called only once.
DB::ContextPtr InitGlobalContext()
{
    static DB::SharedContextHolder sharedContextHolder = DB::Context::createShared();
    DB::ContextMutablePtr globalContext = DB::Context::createGlobal(sharedContextHolder.get());
    ConfigurationPtr config(new Poco::Util::XMLConfiguration());
    globalContext->setConfig(config);
    globalContext->makeGlobalContext();
    return globalContext;
}

DB::ContextPtr GetGlobalContext()
{
    static DB::ContextPtr globalContext = InitGlobalContext();
    return globalContext;
}

class TComputedColumnPredicatePopulationTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<TTableSchemaPtr, TString, TString, TString>>
{
protected:
    DB::ContextPtr CreateQueryContext() const
    {
        auto context = DB::Context::createCopy(GetGlobalContext());
        context->makeQueryContext();
        return context;
    }
};

TEST_P(TComputedColumnPredicatePopulationTest, Test)
{
    const auto& [schema, originalPredicate, expectedPredicateWithIn, expectedPredicateWithDnf] = GetParam();

    DB::ParserExpressionWithOptionalAlias parser(false);
    auto originalAst = DB::parseQuery(parser, originalPredicate, 0 /*maxQuerySize*/, 0 /*maxQueryDepth*/);
    DB::PreparedSets preparedSets;
    TQuerySettingsPtr settings = New<TQuerySettings>();
    for (auto deducedStatementMode : TEnumTraits<EDeducedStatementMode>::GetDomainValues()) {
        settings->DeducedStatementMode = deducedStatementMode;
        auto resultAst = PopulatePredicateWithComputedColumns(originalAst->clone(), schema, CreateQueryContext(), preparedSets, settings, Logger);
        auto resultPredicate = TString(DB::serializeAST(*resultAst));
        if (deducedStatementMode == EDeducedStatementMode::In) {
            EXPECT_EQ(expectedPredicateWithIn, resultPredicate);
        } else {
            EXPECT_EQ(expectedPredicateWithDnf, resultPredicate);
        }
    }
}


INSTANTIATE_TEST_SUITE_P(
    Test,
    TComputedColumnPredicatePopulationTest,
    ::testing::Values(
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Int64, ESortOrder::Ascending)
                    .SetExpression("2 * key"),
                TColumnSchema("key", EValueType::Int64)
            }),
            "key = 5",
            "(key = 5) AND ((key, computed_key) IN tuple((5, 10)))",
            "(key = 5) AND ((key = 5) AND (computed_key = 10))"),
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Int64, ESortOrder::Ascending)
                    .SetExpression("2 * key"),
                TColumnSchema("key", EValueType::Int64)
            }),
            "(key = (7 * key)) OR (NOT (key = 5)) OR (key > 7)",
            "(key = (7 * key)) OR (NOT ((key = 5) AND ((key, computed_key) IN tuple((5, 10))))) OR (key > 7)",
            "(key = (7 * key)) OR (NOT ((key = 5) AND ((key = 5) AND (computed_key = 10)))) OR (key > 7)"),
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("farm_hash(key)"),
                TColumnSchema("key", EValueType::String)
            }),
            "key = 'foo'",
            "(key = 'foo') AND ((key, computed_key) IN tuple(('foo', 7945198393224481366)))",
            "(key = 'foo') AND ((key = 'foo') AND (computed_key = 7945198393224481366))"),
        // Mistake should leave occurrence as is.
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key / (key - 2)"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "(key = 2) OR (3 = key) OR (key = (2 + 2))",
            "0 OR ((3 = key) AND ((key, computed_key) IN tuple((3, 3)))) OR ((key = (2 + 2)) AND ((key, computed_key) IN tuple((4, 2))))",
            "0 OR ((3 = key) AND ((key = 3) AND (computed_key = 3))) OR ((key = (2 + 2)) AND ((key = 4) AND (computed_key = 2)))"),
        // Do not go inside subqueries.
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 2"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "key IN (SELECT * FROM T WHERE key = 42)",
            "key IN ((SELECT * FROM T WHERE key = 42) AS _subquery1)",
            "key IN ((SELECT * FROM T WHERE key = 42) AS _subquery2)"),
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key1", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 2"),
                TColumnSchema("computed_key2", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 3"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "key = 5",
            "(key = 5) AND ((key, computed_key1) IN tuple((5, 10))) AND ((key, computed_key2) IN tuple((5, 15)))",
            "(key = 5) AND ((key = 5) AND (computed_key1 = 10)) AND ((key = 5) AND (computed_key2 = 15))"),
        // TODO(max42): CHYT-438.
        // Should become "(key1 = 5) AND (key2 = 10) AND (computed_key = 15)".
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key1 + key2"),
                TColumnSchema("key1", EValueType::Uint64),
                TColumnSchema("key2", EValueType::Uint64)
            }),
            "(key1 = 5) AND (key2 = 10)",
            "(key1 = 5) AND (key2 = 10)",
            "(key1 = 5) AND (key2 = 10)"),
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key1 * 2 + key2"),
                TColumnSchema("key1", EValueType::Uint64),
                TColumnSchema("key2", EValueType::Uint64)
            }),
            "(key1, key2) = (5, 10) OR tuple(20, 42) = tuple(key2, key1)",
            "(((key1, key2) = (5, 10)) AND ((key1, key2, computed_key) IN tuple((5, 10, 20)))) OR (((20, 42) = (key2, key1)) AND ((key1, key2, computed_key) IN tuple((42, 20, 104))))",
            "(((key1, key2) = (5, 10)) AND ((key1 = 5) AND (key2 = 10) AND (computed_key = 20))) OR (((20, 42) = (key2, key1)) AND ((key1 = 42) AND (key2 = 20) AND (computed_key = 104)))"),
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 2"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "key IN (2, 3)",
            "(key IN (2, 3)) AND ((key, computed_key) IN ((2, 4), (3, 6)))",
            "(key IN (2, 3)) AND (((key = 2) AND (computed_key = 4)) OR ((key = 3) AND (computed_key = 6)))"),
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 2"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "key IN tuple(2, 3)",
            "(key IN (2, 3)) AND ((key, computed_key) IN ((2, 4), (3, 6)))",
            "(key IN (2, 3)) AND (((key = 2) AND (computed_key = 4)) OR ((key = 3) AND (computed_key = 6)))"),
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Uint64, ESortOrder::Ascending)
                    .SetExpression("key * 2"),
                TColumnSchema("key", EValueType::Uint64)
            }),
            "key IN (1)",
            "(key IN (1)) AND ((key, computed_key) IN tuple((1, 2)))",
            "(key IN (1)) AND ((key = 1) AND (computed_key = 2))"),
        // Not a key column.
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Int64)
                    .SetExpression("2 * key"),
                TColumnSchema("key", EValueType::Int64)
            }),
            "key = 5",
            "key = 5",
            "key = 5"),
        std::tuple(New<TTableSchema>(std::vector<TColumnSchema>{}), "1", "1", "1"),
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Int64, ESortOrder::Ascending)
                    .SetExpression("2 * key"),
                TColumnSchema("key", EValueType::Int64),
                TColumnSchema("value", EValueType::String),
            }),
            "(key, value) = (4, 'xyz')",
            "((key, value) = (4, 'xyz')) AND ((key, computed_key) IN tuple((4, 8)))",
            "((key, value) = (4, 'xyz')) AND ((key = 4) AND (computed_key = 8))"),
        std::tuple(
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("computed_key", EValueType::Int64, ESortOrder::Ascending)
                    .SetExpression("2 * key"),
                TColumnSchema("key", EValueType::Int64),
                TColumnSchema("value", EValueType::String),
            }),
            "((key, value) IN ((4, 'xyz'), (5, 'asd')))",
            "((key, value) IN ((4, 'xyz'), (5, 'asd'))) AND ((key, computed_key) IN ((4, 8), (5, 10)))",
            "((key, value) IN ((4, 'xyz'), (5, 'asd'))) AND (((key = 4) AND (computed_key = 8)) OR ((key = 5) AND (computed_key = 10)))")
        ));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

