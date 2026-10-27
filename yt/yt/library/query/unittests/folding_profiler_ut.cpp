#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

#include <yt/yt/library/query/engine/cg_cache.h>
#include <yt/yt/library/query/engine/folding_profiler.h>

#include <yt/yt/library/query/engine_api/coordinator.h>

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

} // namespace
} // namespace NYT::NQueryClient
