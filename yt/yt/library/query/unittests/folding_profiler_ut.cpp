#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

#include <yt/yt/library/query/engine/cg_cache.h>
#include <yt/yt/library/query/engine/folding_profiler.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/coordinator.h>

#include <yt/yt/client/table_client/helpers.h>

#include <algorithm>

namespace NYT::NQueryClient {
namespace {

using namespace NTableClient;
using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

struct TAggregateTestCase
{
    ESimpleLogicalValueType Type = ESimpleLogicalValueType::Int64;
    std::vector<TSource> Tablets;
    TUnversionedValue Expected = MakeUnversionedNullValue();
};

class TAggregateStateCacheTest
    : public TQueryEvaluateTest
    , public ::testing::WithParamInterface<EExecutionBackend>
{
protected:
    void SetUp() override
    {
        TQueryEvaluateTest::SetUp();
        TCodegenCacheSingleton::TearDownForTests();
        if (GetParam() == EExecutionBackend::WebAssembly && !EnableWebAssemblyInUnitTests()) {
            GTEST_SKIP();
        }
    }

    void TearDown() override
    {
        TCodegenCacheSingleton::TearDownForTests();
        TQueryEvaluateTest::TearDown();
    }

    TQueryPtr PrepareAggregate(const std::string& expression, ESimpleLogicalValueType type)
    {
        auto schema = MakeSplit({
            TColumnSchema("a", OptionalLogicalType(SimpleLogicalType(type)), ESortOrder::Ascending),
            TColumnSchema("b", EValueType::Int64),
        });
        return Prepare(
            Format("%v as r from `//t` group by 1", expression),
            {{"//t", schema}},
            /*placeholderValues*/ {},
            {.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});
    }

    llvm::FoldingSetNodeID ProfileFront(const TQueryPtr& query)
    {
        auto front = GetDistributedQueryPattern(query).first;
        llvm::FoldingSetNodeID id;
        TCGVariables variables;
        Profile(
            front,
            &id,
            &variables,
            TJoinProfilerRegistry(
                /*executePlan*/ {},
                /*consumeSubqueryStatistics*/ {},
                /*memoryChunkProvider*/ nullptr,
                /*logger*/ {}),
            {.ExecutionBackend = GetParam()});
        return id;
    }

    void CheckResult(const TQueryPtr& query, const TAggregateTestCase& input)
    {
        auto result = RunOnCoordinator(query, {input.Tablets}, GetParam());
        ASSERT_EQ(result.Rows.Size(), 1);
        ASSERT_EQ(result.Rows[0].GetCount(), 1);
        EXPECT_EQ(result.Rows[0][0], input.Expected);
    }

    void CheckCacheReuse(
        const std::string& expression,
        const TAggregateTestCase& firstInput,
        const TAggregateTestCase& secondInput)
    {
        SCOPED_TRACE(Format("%v: %v / %v", expression, firstInput.Type, secondInput.Type));
        auto first = PrepareAggregate(expression, firstInput.Type);
        auto second = PrepareAggregate(expression, secondInput.Type);

        TCodegenCacheSingleton::TearDownForTests();
        CheckResult(second, secondInput);

        TCodegenCacheSingleton::TearDownForTests();
        CheckResult(first, firstInput);
        CheckResult(second, secondInput);

        TCodegenCacheSingleton::TearDownForTests();
        CheckResult(second, secondInput);
        CheckResult(first, firstInput);
    }

    void CheckExtremum(const std::string& expression, i64 expected)
    {
        const auto inputs = std::vector<TAggregateTestCase>{
            {
                .Type = ESimpleLogicalValueType::Int64,
                .Tablets = {{"a=-1;b=10"}, {"a=1;b=20"}},
                .Expected = MakeInt64(expected),
            },
            {
                .Type = ESimpleLogicalValueType::Uint64,
                .Tablets = {{"a=0u;b=10"}, {"a=9223372036854775808u;b=20"}},
                .Expected = MakeInt64(expected),
            },
            {
                .Type = ESimpleLogicalValueType::Double,
                .Tablets = {{"a=-2.0;b=10"}, {"a=-1.0;b=20"}},
                .Expected = MakeInt64(expected),
            },
        };
        for (int first = 0; first < std::ssize(inputs); ++first) {
            for (int second = first + 1; second < std::ssize(inputs); ++second) {
                CheckCacheReuse(expression, inputs[first], inputs[second]);
            }
        }
    }
};

TEST_P(TAggregateStateCacheTest, MergeQueryArgumentTypes)
{
    const auto types = std::vector{
        ESimpleLogicalValueType::Int64,
        ESimpleLogicalValueType::Uint64,
        ESimpleLogicalValueType::Double,
    };
    for (const auto& expression : {"avg(a)", "argmin(b,a)", "argmax(b,a)"}) {
        for (int first = 0; first < std::ssize(types); ++first) {
            for (int second = first + 1; second < std::ssize(types); ++second) {
                SCOPED_TRACE(Format("%v: %v / %v", expression, types[first], types[second]));
                auto firstFingerprint = ProfileFront(PrepareAggregate(expression, types[first]));
                auto secondFingerprint = ProfileFront(PrepareAggregate(expression, types[second]));
                EXPECT_FALSE(firstFingerprint == secondFingerprint);
            }
        }
    }
}

TEST_P(TAggregateStateCacheTest, Average)
{
    const auto inputs = std::vector<TAggregateTestCase>{
        {
            .Type = ESimpleLogicalValueType::Int64,
            .Tablets = {{"a=-2"}, {"a=-1"}},
            .Expected = MakeDouble(-1.5),
        },
        {
            .Type = ESimpleLogicalValueType::Uint64,
            .Tablets = {{"a=0u"}, {"a=9223372036854775808u"}},
            .Expected = MakeDouble(0x1p62),
        },
        {
            .Type = ESimpleLogicalValueType::Double,
            .Tablets = {{"a=1.5"}, {"a=2.5"}},
            .Expected = MakeDouble(2.0),
        },
    };
    for (int first = 0; first < std::ssize(inputs); ++first) {
        for (int second = first + 1; second < std::ssize(inputs); ++second) {
            CheckCacheReuse("avg(a)", inputs[first], inputs[second]);
        }
    }
}

TEST_P(TAggregateStateCacheTest, Argmin)
{
    CheckExtremum("argmin(b,a)", /*expected*/ 10);
}

TEST_P(TAggregateStateCacheTest, Argmax)
{
    CheckExtremum("argmax(b,a)", /*expected*/ 20);
}

TEST_P(TAggregateStateCacheTest, SameWireType)
{
    for (const auto& expression : {"avg(a)", "argmin(b,a)", "argmax(b,a)"}) {
        SCOPED_TRACE(expression);
        auto first = PrepareAggregate(expression, ESimpleLogicalValueType::Int32);
        auto second = PrepareAggregate(expression, ESimpleLogicalValueType::Int64);
        EXPECT_TRUE(ProfileFront(first) == ProfileFront(second));
    }
}

INSTANTIATE_TEST_SUITE_P(
    Backends,
    TAggregateStateCacheTest,
    ::testing::Values(EExecutionBackend::Native, EExecutionBackend::WebAssembly));

////////////////////////////////////////////////////////////////////////////////

class TComputedColumnCacheTest
    : public ::testing::TestWithParam<bool>
{
protected:
    TTableSchemaPtr MakeSchema(
        const std::vector<std::string>& columnNames,
        const std::string& expression)
    {
        std::vector<TColumnSchema> columns;
        auto computedColumn = TColumnSchema("result", EValueType::Int64)
            .SetExpression(expression);
        if (GetParam()) {
            computedColumn.SetSortOrder(ESortOrder::Ascending);
            columns.push_back(computedColumn);
        }

        for (const auto& name : columnNames) {
            columns.emplace_back(name, EValueType::Int64);
            if (GetParam()) {
                columns.back().SetSortOrder(ESortOrder::Ascending);
            }
        }

        if (!GetParam()) {
            columns.push_back(computedColumn);
        }
        return New<TTableSchema>(std::move(columns));
    }

    llvm::FoldingSetNodeID ProfileSchema(const TTableSchemaPtr& schema)
    {
        llvm::FoldingSetNodeID id;
        Profile(schema, &id);
        return id;
    }

    void CheckResult(
        const TColumnEvaluatorPtr& evaluator,
        const TTableSchemaPtr& schema,
        TStringBuf values,
        i64 expected)
    {
        auto buffer = New<TRowBuffer>();
        auto input = YsonToSchemafulRow(values, *schema, /*treatMissingAsNull*/ true);
        auto row = buffer->CaptureRow(input);
        int index = schema->GetColumnIndexOrThrow("result");

        evaluator->EvaluateKey(row, buffer, index, /*preserveColumnId*/ true);
        EXPECT_EQ(row[index], MakeUnversionedInt64Value(expected, index));

        row[index] = MakeUnversionedNullValue(index);
        evaluator->EvaluateKeys(row, buffer, /*preserveColumnsIds*/ true);
        EXPECT_EQ(row[index], MakeUnversionedInt64Value(expected, index));
    }

    void CheckCacheReuse(
        const std::vector<TTableSchemaPtr>& schemas,
        TStringBuf values,
        i64 expected)
    {
        for (const auto& schema : schemas) {
            auto evaluator = TColumnEvaluator::Create(
                schema,
                GetBuiltinTypeInferrers(),
                GetBuiltinFunctionProfilers());
            CheckResult(evaluator, schema, values, expected);
        }

        for (int first = 0; first < std::ssize(schemas); ++first) {
            auto cache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
            CheckResult(cache->Find(schemas[first]), schemas[first], values, expected);

            for (int second = 0; second < std::ssize(schemas); ++second) {
                SCOPED_TRACE(Format("%v -> %v", *schemas[first], *schemas[second]));
                if (first != second) {
                    EXPECT_FALSE(ProfileSchema(schemas[first]) == ProfileSchema(schemas[second]));
                }
                CheckResult(cache->Find(schemas[second]), schemas[second], values, expected);
            }
            EXPECT_EQ(cache->GetSize(), std::ssize(schemas));
        }
    }
};

TEST_P(TComputedColumnCacheTest, ReferencePermutation)
{
    CheckCacheReuse(
        {MakeSchema({"a", "b"}, "a-b"), MakeSchema({"b", "a"}, "a-b")},
        "a=7;b=2",
        /*expected*/ 5);
}

TEST_P(TComputedColumnCacheTest, RepeatedReferencePermutation)
{
    CheckCacheReuse(
        {MakeSchema({"a", "b"}, "a-b+a"), MakeSchema({"b", "a"}, "a-b+a")},
        "a=7;b=2",
        /*expected*/ 12);
}

TEST_P(TComputedColumnCacheTest, ThreeReferencePermutations)
{
    auto names = std::vector<std::string>{"a", "b", "c"};
    std::vector<TTableSchemaPtr> schemas;
    do {
        schemas.push_back(MakeSchema(names, "a*100+b*10+c"));
    } while (std::next_permutation(names.begin(), names.end()));

    CheckCacheReuse(schemas, "a=7;b=2;c=3", /*expected*/ 723);
}

TEST_P(TComputedColumnCacheTest, NonAdjacentReferencePermutation)
{
    CheckCacheReuse(
        {MakeSchema({"a", "unused", "b"}, "a-b"), MakeSchema({"b", "unused", "a"}, "a-b")},
        "a=7;b=2;unused=100",
        /*expected*/ 5);
}

TEST_P(TComputedColumnCacheTest, EscapedReferenceNames)
{
    auto firstName = std::string("a\0x", /*count*/ 3);
    auto secondName = std::string("a\0y", /*count*/ 3);
    auto expression = std::string("`a\\x00x`-`a\\x00y`");
    CheckCacheReuse(
        {
            MakeSchema({firstName, secondName}, expression),
            MakeSchema({secondName, firstName}, expression),
        },
        "\"a\\x00x\"=7;\"a\\x00y\"=2",
        /*expected*/ 5);
}

TEST_P(TComputedColumnCacheTest, SingleReferencePosition)
{
    CheckCacheReuse(
        {MakeSchema({"a", "b"}, "a"), MakeSchema({"b", "a"}, "a")},
        "a=7;b=2",
        /*expected*/ 7);
}

TEST_P(TComputedColumnCacheTest, UnreferencedColumnNames)
{
    auto first = MakeSchema({"a", "b", "unused"}, "a-b");
    auto second = MakeSchema({"a", "b", "renamed"}, "a-b");
    EXPECT_TRUE(ProfileSchema(first) == ProfileSchema(second));

    auto cache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
    auto firstEvaluator = cache->Find(first);
    auto secondEvaluator = cache->Find(second);
    EXPECT_EQ(firstEvaluator, secondEvaluator);
    CheckResult(firstEvaluator, first, "a=7;b=2", /*expected*/ 5);
    CheckResult(secondEvaluator, second, "a=7;b=2", /*expected*/ 5);
}

INSTANTIATE_TEST_SUITE_P(
    ColumnKinds,
    TComputedColumnCacheTest,
    ::testing::Bool(),
    [] (const ::testing::TestParamInfo<bool>& info) {
        return info.param ? "Key" : "Value";
    });

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
