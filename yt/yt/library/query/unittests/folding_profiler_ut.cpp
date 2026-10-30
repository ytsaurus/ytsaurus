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

struct TQueryTestCase
{
    std::string Query;
    TSource ExpectedRows;
};

class TCodegenCacheTest
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

    TQueryPtr PrepareQuery(const std::string& query, const TSplitMap& splits)
    {
        return Prepare(
            query,
            splits,
            /*placeholderValues*/ {},
            {.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});
    }

    llvm::FoldingSetNodeID ProfileQuery(const TQueryPtr& query)
    {
        auto registry = TJoinProfilerRegistry(
            /*executePlan*/ {},
            /*consumeSubqueryStatistics*/ {},
            /*memoryChunkProvider*/ nullptr,
            /*logger*/ {});
        for (int index = 0; index < std::ssize(query->JoinClauses); ++index) {
            registry.InsertJoinProfilerOrThrow(index, MakeNullJoinSubqueryProfiler());
        }
        llvm::FoldingSetNodeID id;
        TCGVariables variables;
        Profile(query, &id, &variables, registry, {.ExecutionBackend = GetParam()});
        return id;
    }

    template <class TFirstCheck, class TSecondCheck>
    void CheckCacheReuse(const TFirstCheck& checkFirst, const TSecondCheck& checkSecond)
    {
        TCodegenCacheSingleton::TearDownForTests();
        checkSecond();

        TCodegenCacheSingleton::TearDownForTests();
        checkFirst();
        checkSecond();

        TCodegenCacheSingleton::TearDownForTests();
        checkSecond();
        checkFirst();
    }

    void CheckResult(
        const TQueryTestCase& testCase,
        const TQueryPtr& query,
        const TSplitMap& splits,
        const std::vector<TSource>& sources)
    {
        auto expected = YsonToRows(testCase.ExpectedRows, MakeSplit(query->GetTableSchema()->Columns()));
        EvaluateWithQueryStatistics(
            testCase.Query,
            splits,
            sources,
            ResultMatcher(expected),
            {.SyntaxVersion = 2, .ExecutionBackend = GetParam()});
    }

    void CheckCacheReuse(
        const TQueryTestCase& first,
        const TQueryTestCase& second,
        const TSplitMap& splits,
        const std::vector<TSource>& sources,
        bool sameKey = false)
    {
        SCOPED_TRACE(Format("%v -> %v", first.Query, second.Query));
        auto firstQuery = PrepareQuery(first.Query, splits);
        auto secondQuery = PrepareQuery(second.Query, splits);
        EXPECT_EQ(ProfileQuery(firstQuery) == ProfileQuery(secondQuery), sameKey);

        CheckCacheReuse(
            [&] {
                CheckResult(first, firstQuery, splits, sources);
            },
            [&] {
                CheckResult(second, secondQuery, splits, sources);
            });
    }

    void CheckCacheReuse(
        const TQueryTestCase& first,
        const TQueryTestCase& second,
        const TDataSplit& split,
        const TSource& source,
        bool sameKey = false)
    {
        CheckCacheReuse(first, second, {{"//t", split}}, {source}, sameKey);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TAggregateTestCase
{
    ESimpleLogicalValueType Type = ESimpleLogicalValueType::Int64;
    std::vector<TSource> Tablets;
    TUnversionedValue Expected = MakeUnversionedNullValue();
};

class TAggregateStateCacheTest
    : public TCodegenCacheTest
{
protected:
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

        TCodegenCacheTest::CheckCacheReuse(
            [&] {
                CheckResult(first, firstInput);
            },
            [&] {
                CheckResult(second, secondInput);
            });
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

class TJoinCacheTest
    : public TCodegenCacheTest
{
protected:
    TSplitMap MakeSplits(EValueType keyType = EValueType::Int64)
    {
        return {
            {"//left", MakeSplit({{"a", keyType}, {"b", keyType}})},
            {"//right", MakeSplit({
                {"k", keyType},
                {"l", keyType},
                {"m", keyType},
                {"v", EValueType::Int64},
            })},
        };
    }
};

TEST_P(TJoinCacheTest, ReusedEquationRoot)
{
    CheckCacheReuse(
        {
            .Query = "r.v as r from `//left` l join `//right` r on (l.a+l.b, l.a) = (r.k, r.l)",
            .ExpectedRows = {"r=10"},
        },
        {
            .Query = "r.v as r from `//left` l join `//right` r on (l.a+l.b, l.b) = (r.k, r.l)",
            .ExpectedRows = {"r=20"},
        },
        MakeSplits(),
        {{"a=1;b=2"}, {"k=3;l=1;v=10", "k=3;l=2;v=20"}});
}

TEST_P(TJoinCacheTest, EquationRootOrder)
{
    CheckCacheReuse(
        {
            .Query = "r.v as r from `//left` l join `//right` r on (l.a+l.b, l.a, l.b) = (r.k, r.l, r.m)",
            .ExpectedRows = {"r=10"},
        },
        {
            .Query = "r.v as r from `//left` l join `//right` r on (l.a+l.b, l.b, l.a) = (r.k, r.l, r.m)",
            .ExpectedRows = {"r=20"},
        },
        MakeSplits(),
        {{"a=1;b=2"}, {"k=3;l=1;m=2;v=10", "k=3;l=2;m=1;v=20"}});
}

TEST_P(TJoinCacheTest, LeftJoinReusedEquationRoot)
{
    CheckCacheReuse(
        {
            .Query = "r.v as r from `//left` l left join `//right` r on (l.a+l.b, l.a) = (r.k, r.l)",
            .ExpectedRows = {"r=10"},
        },
        {
            .Query = "r.v as r from `//left` l left join `//right` r on (l.a+l.b, l.b) = (r.k, r.l)",
            .ExpectedRows = {"r=#"},
        },
        MakeSplits(),
        {{"a=1;b=2"}, {"k=3;l=1;v=10"}});
}

TEST_P(TJoinCacheTest, StringEquationRoot)
{
    CheckCacheReuse(
        {
            .Query = "r.v as r from `//left` l join `//right` r on (concat(l.a,l.b), l.a) = (r.k, r.l)",
            .ExpectedRows = {"r=10"},
        },
        {
            .Query = "r.v as r from `//left` l join `//right` r on (concat(l.a,l.b), l.b) = (r.k, r.l)",
            .ExpectedRows = {"r=20"},
        },
        MakeSplits(EValueType::String),
        {{"a=p;b=q"}, {"k=pq;l=p;v=10", "k=pq;l=q;v=20"}});
}

TEST_P(TJoinCacheTest, ReusedRootAcrossJoinGroup)
{
    auto splits = MakeSplits();
    splits["//right2"] = MakeSplit({{"k", EValueType::Int64}, {"v", EValueType::Int64}});
    CheckCacheReuse(
        {
            .Query = "r.v+r2.v as r from `//left` l join `//right` r on (l.a+l.b, l.a) = (r.k, r.l) "
                "join `//right2` r2 on l.a = r2.k",
            .ExpectedRows = {"r=110"},
        },
        {
            .Query = "r.v+r2.v as r from `//left` l join `//right` r on (l.a+l.b, l.a) = (r.k, r.l) "
                "join `//right2` r2 on l.b = r2.k",
            .ExpectedRows = {"r=210"},
        },
        splits,
        {{"a=1;b=2"}, {"k=3;l=1;v=10"}, {"k=1;v=100", "k=2;v=200"}});
}

TEST_P(TJoinCacheTest, EquationArity)
{
    auto splits = MakeSplits();
    auto first = PrepareQuery("r.v as r from `//left` l join `//right` r on l.a = r.k", splits);
    auto second = PrepareQuery("r.v as r from `//left` l join `//right` r on (l.a, l.a) = (r.k, r.l)", splits);
    ASSERT_EQ(std::ssize(first->JoinClauses), 1);
    ASSERT_EQ(std::ssize(second->JoinClauses), 1);
    EXPECT_EQ(std::ssize(first->JoinClauses[0]->SelfEquations), 1);
    EXPECT_EQ(std::ssize(second->JoinClauses[0]->SelfEquations), 2);
    EXPECT_FALSE(ProfileQuery(first) == ProfileQuery(second));
}

TEST_P(TJoinCacheTest, EquationCountBoundaries)
{
    auto splits = MakeSplits();
    splits["//right2"] = MakeSplit({
        {"k", EValueType::Int64},
        {"l", EValueType::Int64},
        {"v", EValueType::Int64},
    });
    auto first = PrepareQuery(
        "r.v+r2.v as r from `//left` l join `//right` r on (l.a, l.a) = (r.k, r.l) "
        "join `//right2` r2 on l.a = r2.k",
        splits);
    auto second = PrepareQuery(
        "r.v+r2.v as r from `//left` l join `//right` r on l.a = r.k "
        "join `//right2` r2 on (l.a, l.a) = (r2.k, r2.l)",
        splits);
    for (const auto& query : {first, second}) {
        ASSERT_EQ(std::ssize(query->JoinClauses), 2);
        auto groups = GetJoinGroups(query->JoinClauses, query->Schema.GetRenamedSchema());
        ASSERT_EQ(std::ssize(groups), 1);
        EXPECT_EQ(groups[0], 2);
    }
    EXPECT_EQ(std::ssize(first->JoinClauses[0]->SelfEquations), 2);
    EXPECT_EQ(std::ssize(first->JoinClauses[1]->SelfEquations), 1);
    EXPECT_EQ(std::ssize(second->JoinClauses[0]->SelfEquations), 1);
    EXPECT_EQ(std::ssize(second->JoinClauses[1]->SelfEquations), 2);
    EXPECT_FALSE(ProfileQuery(first) == ProfileQuery(second));
}

TEST_P(TJoinCacheTest, LiteralValues)
{
    CheckCacheReuse(
        {
            .Query = "r.v as r from `//left` l join `//right` r on (l.a+1, l.a) = (r.k, r.l)",
            .ExpectedRows = {"r=10"},
        },
        {
            .Query = "r.v as r from `//left` l join `//right` r on (l.a+2, l.a) = (r.k, r.l)",
            .ExpectedRows = {"r=20"},
        },
        MakeSplits(),
        {{"a=1;b=2"}, {"k=2;l=1;v=10", "k=3;l=1;v=20"}},
        /*sameKey*/ true);
}

TEST_P(TJoinCacheTest, DifferentExpressions)
{
    CheckCacheReuse(
        {
            .Query = "r.v as r from `//left` l join `//right` r on l.a+l.b = r.k",
            .ExpectedRows = {"r=10"},
        },
        {
            .Query = "r.v as r from `//left` l join `//right` r on l.a-l.b = r.k",
            .ExpectedRows = {"r=20"},
        },
        MakeSplits(),
        {{"a=1;b=2"}, {"k=3;v=10", "k=-1;v=20"}});
}

INSTANTIATE_TEST_SUITE_P(
    Backends,
    TJoinCacheTest,
    ::testing::Values(EExecutionBackend::Native, EExecutionBackend::WebAssembly));

////////////////////////////////////////////////////////////////////////////////

class TOrderByCacheTest
    : public TCodegenCacheTest
{
protected:
    TDataSplit MakeInputSplit(EValueType type = EValueType::Int64)
    {
        return MakeSplit({{"a", type}, {"b", type}});
    }
};

TEST_P(TOrderByCacheTest, ReusedExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by a+b, a limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        {
            .Query = "a, b from `//t` order by a+b, b limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, ExpressionRootOrder)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by a+b, a, b limit 2",
            .ExpectedRows = {"a=1;b=2", "a=2;b=1"},
        },
        {
            .Query = "a, b from `//t` order by a+b, b, a limit 2",
            .ExpectedRows = {"a=2;b=1", "a=1;b=2"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, DescendingReusedExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by a+b desc, a desc limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        {
            .Query = "a, b from `//t` order by a+b desc, b desc limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, Offset)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by a+b, a offset 1 limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        {
            .Query = "a, b from `//t` order by a+b, b offset 1 limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, StringExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by concat(a,b), a limit 1",
            .ExpectedRows = {"a=a;b=aa"},
        },
        {
            .Query = "a, b from `//t` order by concat(a,b), b limit 1",
            .ExpectedRows = {"a=aa;b=a"},
        },
        MakeInputSplit(EValueType::String),
        {"a=a;b=aa", "a=aa;b=a"});
}

TEST_P(TOrderByCacheTest, NullableExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by a+b, a limit 1",
            .ExpectedRows = {"a=#;b=1"},
        },
        {
            .Query = "a, b from `//t` order by a+b, b limit 1",
            .ExpectedRows = {"a=2;b=#"},
        },
        MakeInputSplit(),
        {"a=#;b=1", "a=2;b=#"});
}

TEST_P(TOrderByCacheTest, ReusedCompoundExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by (a+b)*(a-b), a+b limit 1",
            .ExpectedRows = {"a=3;b=-1"},
        },
        {
            .Query = "a, b from `//t` order by (a+b)*(a-b), a-b limit 1",
            .ExpectedRows = {"a=3;b=1"},
        },
        MakeInputSplit(),
        {"a=3;b=1", "a=3;b=-1"});
}

TEST_P(TOrderByCacheTest, DifferentDirections)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by a+b, a limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        {
            .Query = "a, b from `//t` order by a+b, a desc limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, LiteralValues)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by abs(a-1), b limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        {
            .Query = "a, b from `//t` order by abs(a-2), b limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"},
        /*sameKey*/ true);
}

TEST_P(TOrderByCacheTest, OrderItemCount)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` order by a+b, a limit 2",
            .ExpectedRows = {"a=1;b=2", "a=2;b=1"},
        },
        {
            .Query = "a, b from `//t` order by a+b, a, b limit 2",
            .ExpectedRows = {"a=1;b=2", "a=2;b=1"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByReusedExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by a+b, a limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by a+b, b limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByExpressionRootOrder)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by a+b, a, b limit 2",
            .ExpectedRows = {"a=1;b=2", "a=2;b=1"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by a+b, b, a limit 2",
            .ExpectedRows = {"a=2;b=1", "a=1;b=2"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByDescendingReusedExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by a+b desc, a desc limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by a+b desc, b desc limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByOffset)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by a+b, a offset 1 limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by a+b, b offset 1 limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByStringExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by concat(a,b), a limit 1",
            .ExpectedRows = {"a=a;b=aa"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by concat(a,b), b limit 1",
            .ExpectedRows = {"a=aa;b=a"},
        },
        MakeInputSplit(EValueType::String),
        {"a=a;b=aa", "a=aa;b=a"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByNullableExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by a+b, a limit 1",
            .ExpectedRows = {"a=#;b=1"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by a+b, b limit 1",
            .ExpectedRows = {"a=2;b=#"},
        },
        MakeInputSplit(),
        {"a=#;b=1", "a=2;b=#"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByReusedCompoundExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by (a+b)*(a-b), a+b limit 1",
            .ExpectedRows = {"a=3;b=-1"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by (a+b)*(a-b), a-b limit 1",
            .ExpectedRows = {"a=3;b=1"},
        },
        MakeInputSplit(),
        {"a=3;b=1", "a=3;b=-1"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByComputedKeys)
{
    CheckCacheReuse(
        {
            .Query = "x, y from `//t` group by a+1 as x, b-1 as y order by x+y, x limit 1",
            .ExpectedRows = {"x=1;y=2"},
        },
        {
            .Query = "x, y from `//t` group by a+1 as x, b-1 as y order by x+y, y limit 1",
            .ExpectedRows = {"x=2;y=1"},
        },
        MakeInputSplit(),
        {"a=0;b=3", "a=1;b=2"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByAggregatePayload)
{
    CheckCacheReuse(
        {
            .Query = "a, b, sum(v) as s from `//t` group by a, b order by a+b, a limit 1",
            .ExpectedRows = {"a=1;b=2;s=30"},
        },
        {
            .Query = "a, b, sum(v) as s from `//t` group by a, b order by a+b, b limit 1",
            .ExpectedRows = {"a=2;b=1;s=7"},
        },
        MakeSplit({{"a", EValueType::Int64}, {"b", EValueType::Int64}, {"v", EValueType::Int64}}),
        {"a=1;b=2;v=10", "a=2;b=1;v=3", "a=1;b=2;v=20", "a=2;b=1;v=4"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByDifferentDirections)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by a+b, a limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by a+b, a desc limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

TEST_P(TOrderByCacheTest, CombinedGroupByLiteralValues)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by abs(a-1), b limit 1",
            .ExpectedRows = {"a=1;b=2"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by abs(a-2), b limit 1",
            .ExpectedRows = {"a=2;b=1"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"},
        /*sameKey*/ true);
}

TEST_P(TOrderByCacheTest, CombinedGroupByOrderItemCount)
{
    CheckCacheReuse(
        {
            .Query = "a, b from `//t` group by a, b order by a+b, a limit 2",
            .ExpectedRows = {"a=1;b=2", "a=2;b=1"},
        },
        {
            .Query = "a, b from `//t` group by a, b order by a+b, a, b limit 2",
            .ExpectedRows = {"a=1;b=2", "a=2;b=1"},
        },
        MakeInputSplit(),
        {"a=1;b=2", "a=2;b=1"});
}

INSTANTIATE_TEST_SUITE_P(
    Backends,
    TOrderByCacheTest,
    ::testing::Values(EExecutionBackend::Native, EExecutionBackend::WebAssembly));

////////////////////////////////////////////////////////////////////////////////

class TArrayJoinCacheTest
    : public TCodegenCacheTest
{
protected:
    TDataSplit MakeInputSplit(const TLogicalTypePtr& elementType = SimpleLogicalType(ESimpleLogicalValueType::Int64))
    {
        auto listType = OptionalLogicalType(ListLogicalType(elementType));
        return MakeSplit({{"a", listType}, {"b", listType}});
    }
};

TEST_P(TArrayJoinCacheTest, ReusedExpressionRoot)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z",
            .ExpectedRows = {"x=1;y=2;z=1"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, b as z",
            .ExpectedRows = {"x=1;y=2;z=2"},
        },
        MakeInputSplit(),
        {"a=[1];b=[2]"});
}

TEST_P(TArrayJoinCacheTest, ExpressionRootOrder)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z, w from `//t` array join a as x, b as y, a as z, b as w",
            .ExpectedRows = {"x=1;y=2;z=1;w=2"},
        },
        {
            .Query = "x, y, z, w from `//t` array join a as x, b as y, b as z, a as w",
            .ExpectedRows = {"x=1;y=2;z=2;w=1"},
        },
        MakeInputSplit(),
        {"a=[1];b=[2]"});
}

TEST_P(TArrayJoinCacheTest, MultipleElements)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z",
            .ExpectedRows = {"x=1;y=2;z=1", "x=3;y=4;z=3"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, b as z",
            .ExpectedRows = {"x=1;y=2;z=2", "x=3;y=4;z=4"},
        },
        MakeInputSplit(),
        {"a=[1;3];b=[2;4]"});
}

TEST_P(TArrayJoinCacheTest, UnequalArrayLengths)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z",
            .ExpectedRows = {"x=1;y=2;z=1", "x=#;y=3;z=#"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, b as z",
            .ExpectedRows = {"x=1;y=2;z=2", "x=#;y=3;z=3"},
        },
        MakeInputSplit(),
        {"a=[1];b=[2;3]"});
}

TEST_P(TArrayJoinCacheTest, EmptyReferencedArray)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z",
            .ExpectedRows = {"x=#;y=2;z=#"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, b as z",
            .ExpectedRows = {"x=#;y=2;z=2"},
        },
        MakeInputSplit(),
        {"a=[];b=[2]"});
}

TEST_P(TArrayJoinCacheTest, NullReferencedArray)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z",
            .ExpectedRows = {"x=#;y=2;z=#"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, b as z",
            .ExpectedRows = {"x=#;y=2;z=2"},
        },
        MakeInputSplit(),
        {"a=#;b=[2]"});
}

TEST_P(TArrayJoinCacheTest, NullableElements)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z",
            .ExpectedRows = {"x=#;y=2;z=#", "x=3;y=#;z=3"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, b as z",
            .ExpectedRows = {"x=#;y=2;z=2", "x=3;y=#;z=#"},
        },
        MakeInputSplit(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
        {"a=[#;3];b=[2;#]"});
}

TEST_P(TArrayJoinCacheTest, StringElements)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z",
            .ExpectedRows = {"x=p;y=q;z=p"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, b as z",
            .ExpectedRows = {"x=p;y=q;z=q"},
        },
        MakeInputSplit(SimpleLogicalType(ESimpleLogicalValueType::String)),
        {"a=[p];b=[q]"});
}

TEST_P(TArrayJoinCacheTest, CompositeElements)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z",
            .ExpectedRows = {"x=[1];y=[2];z=[1]"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, b as z",
            .ExpectedRows = {"x=[1];y=[2];z=[2]"},
        },
        MakeInputSplit(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
        {"a=[[1]];b=[[2]]"});
}

TEST_P(TArrayJoinCacheTest, Predicate)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z and z=1",
            .ExpectedRows = {"x=1;y=2;z=1"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, b as z and z=1",
            .ExpectedRows = {},
        },
        MakeInputSplit(),
        {"a=[1];b=[2]"});
}

TEST_P(TArrayJoinCacheTest, LeftJoinPredicate)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` left array join a as x, b as y, a as z and z=1",
            .ExpectedRows = {"x=1;y=2;z=1"},
        },
        {
            .Query = "x, y, z from `//t` left array join a as x, b as y, b as z and z=1",
            .ExpectedRows = {"x=#;y=#;z=#"},
        },
        MakeInputSplit(),
        {"a=[1];b=[2]"});
}

TEST_P(TArrayJoinCacheTest, UnusedArrayOutputs)
{
    CheckCacheReuse(
        {
            .Query = "z from `//t` array join a as x, b as y, a as z",
            .ExpectedRows = {"z=1"},
        },
        {
            .Query = "z from `//t` array join a as x, b as y, b as z",
            .ExpectedRows = {"z=2"},
        },
        MakeInputSplit(),
        {"a=[1];b=[2]"});
}

TEST_P(TArrayJoinCacheTest, WithTableJoin)
{
    auto listType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64));
    auto splits = TSplitMap{
        {"//left", MakeSplit({{"k", EValueType::Int64}, {"a", listType}, {"b", listType}})},
        {"//right", MakeSplit({{"k", EValueType::Int64}, {"v", EValueType::Int64}})},
    };
    CheckCacheReuse(
        {
            .Query = "x, y, z, r.v as v from `//left` l join `//right` r on l.k=r.k "
                "array join l.a as x, l.b as y, l.a as z",
            .ExpectedRows = {"x=1;y=2;z=1;v=10"},
        },
        {
            .Query = "x, y, z, r.v as v from `//left` l join `//right` r on l.k=r.k "
                "array join l.a as x, l.b as y, l.b as z",
            .ExpectedRows = {"x=1;y=2;z=2;v=10"},
        },
        splits,
        {{"k=1;a=[1];b=[2]"}, {"k=1;v=10"}});
}

TEST_P(TArrayJoinCacheTest, ExpressionArity)
{
    CheckCacheReuse(
        {
            .Query = "x from `//t` array join a as x",
            .ExpectedRows = {"x=1", "x=2"},
        },
        {
            .Query = "x from `//t` array join a as x, a as unused",
            .ExpectedRows = {"x=1", "x=2"},
        },
        MakeInputSplit(),
        {"a=[1;2];b=[3]"});
}

TEST_P(TArrayJoinCacheTest, PredicateLiteralValues)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z and z=1",
            .ExpectedRows = {"x=1;y=3;z=1"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z and z=2",
            .ExpectedRows = {"x=2;y=4;z=2"},
        },
        MakeInputSplit(),
        {"a=[1;2];b=[3;4]"},
        /*sameKey*/ true);
}

TEST_P(TArrayJoinCacheTest, DifferentPredicates)
{
    CheckCacheReuse(
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z and z=1",
            .ExpectedRows = {"x=1;y=3;z=1"},
        },
        {
            .Query = "x, y, z from `//t` array join a as x, b as y, a as z and z!=1",
            .ExpectedRows = {"x=2;y=4;z=2"},
        },
        MakeInputSplit(),
        {"a=[1;2];b=[3;4]"});
}

INSTANTIATE_TEST_SUITE_P(
    Backends,
    TArrayJoinCacheTest,
    ::testing::Values(EExecutionBackend::Native, EExecutionBackend::WebAssembly));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
