#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

#include <yt/yt/library/query/engine/cg_cache.h>

#include <optional>
#include <tuple>
#include <utility>

namespace NYT::NQueryClient {
namespace {

using namespace NTableClient;
using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

class TCaseCodegenTest
    : public TQueryEvaluateTest
    , public ::testing::WithParamInterface<std::tuple<bool, bool, EExecutionBackend>>
{
protected:
    void SetUp() override
    {
        TQueryEvaluateTest::SetUp();
        TCodegenCacheSingleton::TearDownForTests();
        if (GetBackend() == EExecutionBackend::WebAssembly && !EnableWebAssemblyInUnitTests()) {
            GTEST_SKIP();
        }
    }

    void TearDown() override
    {
        TCodegenCacheSingleton::TearDownForTests();
        TQueryEvaluateTest::TearDown();
    }

    EExecutionBackend GetBackend() const
    {
        return std::get<2>(GetParam());
    }

    std::string MakeCase(TStringBuf thenExpression, TStringBuf elseClause = "") const
    {
        bool withOperand = std::get<0>(GetParam());
        return Format(
            "case %v when %v then %v %v end",
            withOperand ? "k" : "",
            withOperand ? "1" : "k = 1",
            thenExpression,
            elseClause);
    }

    void CheckQuery(
        TStringBuf query,
        const TDataSplit& split,
        const TSource& source,
        const TDataSplit& resultSplit,
        const TSource& expected,
        std::optional<bool> useCanonicalNullRelations = {})
    {
        EvaluateWithQueryStatistics(
            query,
            TSplitMap{{"//t", split}},
            std::vector<TSource>{source},
            ResultMatcher(YsonToRows(expected, resultSplit), resultSplit.TableSchema),
            {
                .ExecutionBackend = GetBackend(),
                .UseCanonicalNullRelations = useCanonicalNullRelations.value_or(std::get<1>(GetParam())),
            });
    }

    void Check(
        TStringBuf expression,
        const TDataSplit& split,
        const TSource& source,
        const TSource& expected,
        EValueType resultType = EValueType::Boolean)
    {
        CheckQuery(
            Format("%v as r from [//t]", expression),
            split,
            source,
            MakeSplit({{"r", resultType}}),
            expected);
    }

    void CheckLogicalExpression(
        TStringBuf expression,
        TStringBuf opcode,
        bool caseOnLeft,
        const TDataSplit& split,
        const TSource& source,
        const TSource& expected)
    {
        auto otherOperand = opcode == "and" ? "true" : "false";
        Check(
            caseOnLeft
                ? Format("(%v) %v %v", expression, opcode, otherOperand)
                : Format("%v %v (%v)", otherOperand, opcode, expression),
            split,
            source,
            expected);
    }

    void CheckLogicalExpressions(
        TStringBuf expression,
        const TDataSplit& split,
        const TSource& source,
        const TSource& expected)
    {
        for (const auto& opcode : {"and", "or"}) {
            for (bool caseOnLeft : {false, true}) {
                CheckLogicalExpression(expression, opcode, caseOnLeft, split, source, expected);
            }
        }
    }

    void CheckNullRelationsCacheSeparation(
        TStringBuf query,
        const TDataSplit& split,
        const TSource& source,
        const TSource& legacyExpected,
        const TSource& canonicalExpected)
    {
        auto resultSplit = MakeSplit({{"r", EValueType::Int64}});
        bool initialMode = std::get<1>(GetParam());

        for (bool canonical : {initialMode, !initialMode, initialMode}) {
            SCOPED_TRACE(Format("UseCanonicalNullRelations: %v", canonical));
            CheckQuery(
                query,
                split,
                source,
                resultSplit,
                canonical ? canonicalExpected : legacyExpected,
                canonical);
        }
    }

    void CheckCaseResultTypesAndValues(
        TStringBuf expression,
        const TDataSplit& split,
        const TSource& source,
        const TSource& expected,
        EValueType resultType)
    {
        SCOPED_TRACE(expression);
        auto prepared = ParseAndPrepareExpression(expression, *split.TableSchema);
        const auto* caseExpression = prepared->As<TCaseExpression>();
        ASSERT_TRUE(caseExpression);
        ASSERT_EQ(caseExpression->GetWireType(), resultType);
        for (const auto& clause : caseExpression->WhenThenExpressions) {
            ASSERT_EQ(clause->Result->GetWireType(), resultType);
        }
        ASSERT_TRUE(caseExpression->DefaultExpression);
        ASSERT_EQ(caseExpression->DefaultExpression->GetWireType(), resultType);

        Check(expression, split, source, expected, resultType);
    }

    void CheckNullBranchResultTypes(bool nullElse, TStringBuf nullExpression)
    {
        auto expression = nullElse
            ? MakeCase("v", Format("else %v", nullExpression))
            : MakeCase(nullExpression, "else v");

        for (const auto& [type, value] : std::vector<std::pair<EValueType, TStringBuf>>{
            {EValueType::Int64, "7"},
            {EValueType::Uint64, "7u"},
            {EValueType::Double, "1.5"},
            {EValueType::Boolean, "%true"},
            {EValueType::String, "abc"},
            {EValueType::Any, "[1;2]"},
        }) {
            SCOPED_TRACE(Format("ResultType: %v", type));
            CheckCaseResultTypesAndValues(
                expression,
                MakeSplit({{"k", EValueType::Int64}, {"v", type}}),
                {
                    Format("k=0;v=%v", value),
                    Format("k=1;v=%v", value),
                    "k=0;v=#",
                    "k=1;v=#",
                    Format("k=#;v=%v", value),
                },
                {
                    Format("r=%v", nullElse ? "#" : value),
                    Format("r=%v", nullElse ? value : "#"),
                    "r=#",
                    "r=#",
                    Format("r=%v", nullElse ? "#" : value),
                },
                type);
        }
    }
};

INSTANTIATE_TEST_SUITE_P(
    CaseCodegen,
    TCaseCodegenTest,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Bool(),
        ::testing::Values(EExecutionBackend::Native, EExecutionBackend::WebAssembly)),
    [] (const ::testing::TestParamInfo<TCaseCodegenTest::ParamType>& info) {
        return Format(
            "%v%v%v",
            std::get<0>(info.param) ? "Simple" : "Searched",
            std::get<1>(info.param) ? "Canonical" : "Legacy",
            std::get<2>(info.param) == EExecutionBackend::Native ? "Native" : "WebAssembly");
    });

TEST_P(TCaseCodegenTest, NonNullableResultsInLogicalOperators)
{
    CheckLogicalExpressions(
        MakeCase("true", "else false"),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        {"r=%false", "r=%true", "r=%false"});
}

TEST_P(TCaseCodegenTest, NonNullableRequiredResults)
{
    CheckLogicalExpressions(
        MakeCase("b", "else false"),
        MakeSplit({
            {"k", EValueType::Int64},
            TColumnSchema("b", EValueType::Boolean).SetRequired(true),
        }),
        {"k=0;b=%true", "k=1;b=%true", "k=1;b=%false", "k=#;b=%true"},
        {"r=%false", "r=%true", "r=%false", "r=%false"});
}

TEST_P(TCaseCodegenTest, NonNullableNestedResults)
{
    CheckLogicalExpressions(
        MakeCase("case when b then true else false end", "else false"),
        MakeSplit({{"k", EValueType::Int64}, {"b", EValueType::Boolean}}),
        {"k=0;b=%true", "k=1;b=%true", "k=1;b=%false", "k=1;b=#", "k=#;b=%true"},
        {"r=%false", "r=%true", "r=%false", "r=%false", "r=%false"});
}

TEST_P(TCaseCodegenTest, NullableResultsInLogicalOperators)
{
    auto split = MakeSplit({{"k", EValueType::Int64}, {"b", EValueType::Boolean}});
    TSource source{"k=0;b=#", "k=1;b=#", "k=1;b=%true", "k=1;b=%false", "k=#;b=#"};
    CheckLogicalExpressions(
        MakeCase("b", "else false"),
        split,
        source,
        {"r=%false", "r=#", "r=%true", "r=%false", "r=%false"});
    CheckLogicalExpressions(
        MakeCase("true", "else b"),
        split,
        source,
        {"r=#", "r=%true", "r=%true", "r=%true", "r=#"});
    CheckLogicalExpressions(
        MakeCase("b"),
        split,
        source,
        {"r=#", "r=#", "r=%true", "r=%false", "r=#"});
}

TEST_P(TCaseCodegenTest, MissingElseProjection)
{
    Check(
        MakeCase("true"),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        {"r=#", "r=%true", "r=#"});
}

TEST_P(TCaseCodegenTest, NestedMissingElseProjection)
{
    Check(
        MakeCase("case when b then true end", "else false"),
        MakeSplit({{"k", EValueType::Int64}, {"b", EValueType::Boolean}}),
        {"k=0;b=#", "k=1;b=#", "k=1;b=%true", "k=1;b=%false", "k=#;b=#"},
        {"r=%false", "r=#", "r=%true", "r=#", "r=%false"});
}

TEST_P(TCaseCodegenTest, MissingElseArithmetic)
{
    auto split = MakeSplit({{"k", EValueType::Int64}});
    for (const auto& [type, literal, value] : std::vector<std::tuple<EValueType, TStringBuf, TStringBuf>>{
        {EValueType::Int64, "7", "8"},
        {EValueType::Uint64, "7u", "8u"},
        {EValueType::Double, "1.5", "2.5"},
    }) {
        SCOPED_TRACE(literal);
        auto one = type == EValueType::Uint64 ? "1u" : type == EValueType::Double ? "1.0" : "1";
        Check(
            Format("(%v) + %v", MakeCase(literal), one),
            split,
            {"k=0", "k=1", "k=#"},
            {"r=#", Format("r=%v", value), "r=#"},
            type);
    }
}

TEST_P(TCaseCodegenTest, MissingElseUnary)
{
    Check(
        Format("-(%v)", MakeCase("7")),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        {"r=#", "r=-7", "r=#"},
        EValueType::Int64);
}

TEST_P(TCaseCodegenTest, MissingElseDivision)
{
    Check(
        Format("10 / (%v)", MakeCase("2")),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        {"r=#", "r=5", "r=#"},
        EValueType::Int64);
}

TEST_P(TCaseCodegenTest, MissingElseIsNullScalarTypes)
{
    for (const auto& literal : {"7", "7u", "1.5", "true", "'abc'"}) {
        SCOPED_TRACE(literal);
        Check(
            Format("is_null(%v)", MakeCase(literal)),
            MakeSplit({{"k", EValueType::Int64}}),
            {"k=0", "k=1", "k=#"},
            {"r=%true", "r=%false", "r=%true"});
    }
}

TEST_P(TCaseCodegenTest, MissingElseNullFunctions)
{
    CheckQuery(
        Format(
            "is_null(%v) as missing, if_null(%v, 9) as fallback from [//t]",
            MakeCase("7"),
            MakeCase("8")),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        MakeSplit({{"missing", EValueType::Boolean}, {"fallback", EValueType::Int64}}),
        {"missing=%true;fallback=9", "missing=%false;fallback=8", "missing=%true;fallback=9"});
}

TEST_P(TCaseCodegenTest, ReusedMissingElse)
{
    auto expression = MakeCase("7");
    CheckQuery(
        Format(
            "%v as r, is_null(%v) as missing, if_null(%v, 9) as fallback from [//t]",
            expression,
            expression,
            expression),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#", "k=1"},
        MakeSplit({
            {"r", EValueType::Int64},
            {"missing", EValueType::Boolean},
            {"fallback", EValueType::Int64},
        }),
        {
            "r=#;missing=%true;fallback=9",
            "r=7;missing=%false;fallback=7",
            "r=#;missing=%true;fallback=9",
            "r=7;missing=%false;fallback=7",
        });
}

TEST_P(TCaseCodegenTest, MissingElseInFilter)
{
    auto expression = MakeCase("7");
    CheckQuery(
        Format("k from [//t] where is_null(%v) and is_null(%v)", expression, expression),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#", "k=2"},
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=#", "k=2"});
}

TEST_P(TCaseCodegenTest, RequiredResultWithoutElse)
{
    Check(
        Format("(%v) + 1", MakeCase("v")),
        MakeSplit({
            {"k", EValueType::Int64},
            TColumnSchema("v", EValueType::Int64).SetRequired(true),
        }),
        {"k=0;v=7", "k=1;v=7", "k=#;v=7"},
        {"r=#", "r=8", "r=#"},
        EValueType::Int64);
}

TEST_P(TCaseCodegenTest, NestedMissingElseArithmetic)
{
    auto inner = std::get<0>(GetParam())
        ? "case v when 1 then 7 end"
        : "case when v = 1 then 7 end";
    Check(
        Format("(%v) + 1", MakeCase(inner, "else 9")),
        MakeSplit({{"k", EValueType::Int64}, {"v", EValueType::Int64}}),
        {"k=1;v=0", "k=1;v=1", "k=1;v=#", "k=0;v=0", "k=#;v=0"},
        {"r=#", "r=8", "r=#", "r=10", "r=10"},
        EValueType::Int64);
}

TEST_P(TCaseCodegenTest, MissingElseInLogicalOperators)
{
    CheckLogicalExpressions(
        MakeCase("true"),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        {"r=#", "r=%true", "r=#"});
}

TEST_P(TCaseCodegenTest, NullRelationsCacheSeparation)
{
    CheckNullRelationsCacheSeparation(
        std::get<0>(GetParam())
            ? "case k when # then 7 when 1 then 8 else 9 end as r from [//t]"
            : "case when k = # then 7 when k = 1 then 8 else 9 end as r from [//t]",
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=#", "k=1", "k=2"},
        {"r=7", "r=8", "r=9"},
        {"r=9", "r=8", "r=9"});
}

TEST_P(TCaseCodegenTest, NullableWhenNullRelationsCacheSeparation)
{
    CheckNullRelationsCacheSeparation(
        std::get<0>(GetParam())
            ? "case k when v then 7 else 9 end as r from [//t]"
            : "case when k = v then 7 else 9 end as r from [//t]",
        MakeSplit({{"k", EValueType::Int64}, {"v", EValueType::Int64}}),
        {"k=#;v=#", "k=1;v=1", "k=1;v=2", "k=#;v=1", "k=1;v=#"},
        {"r=7", "r=7", "r=9", "r=9", "r=9"},
        {"r=9", "r=7", "r=9", "r=9", "r=9"});
}

TEST_P(TCaseCodegenTest, NestedNullRelationsCacheSeparation)
{
    auto inner = std::get<0>(GetParam())
        ? "case k when # then 7 else 9 end"
        : "case when k = # then 7 else 9 end";
    CheckNullRelationsCacheSeparation(
        Format("case when b then (%v) else 11 end as r from [//t]", inner),
        MakeSplit({{"k", EValueType::Int64}, {"b", EValueType::Boolean}}),
        {"k=#;b=%true", "k=1;b=%true", "k=#;b=%false", "k=#;b=#"},
        {"r=7", "r=9", "r=11", "r=11"},
        {"r=9", "r=9", "r=11", "r=11"});
}

TEST_P(TCaseCodegenTest, NullBranchScalarResults)
{
    for (const auto& [nullElse, nullExpression] : std::vector<std::pair<bool, TStringBuf>>{
        {true, "#"},
        {false, "#"},
        {true, "if(k = 0, #, #)"},
    }) {
        CheckNullBranchResultTypes(nullElse, nullExpression);
    }
}

TEST_P(TCaseCodegenTest, NumericElseCoercion)
{
    for (const auto& elseClause : {"else 2", "else 2u"}) {
        CheckCaseResultTypesAndValues(
            MakeCase("1.5", elseClause),
            MakeSplit({{"k", EValueType::Int64}}),
            {"k=0", "k=1", "k=#"},
            {"r=2.0", "r=1.5", "r=2.0"},
            EValueType::Double);
    }
}

TEST_P(TCaseCodegenTest, NumericColumnElseCoercion)
{
    if (DefaultExpressionBuilderVersion == 1) {
        GTEST_SKIP() << "Implicit coercion between numeric columns requires expression builder V2";
    }

    CheckCaseResultTypesAndValues(
        MakeCase("d1", "else i1"),
        MakeSplit({
            {"k", EValueType::Int64},
            {"i1", EValueType::Int64},
            {"d1", EValueType::Double},
        }),
        {
            "k=0;i1=2;d1=1.5",
            "k=1;i1=2;d1=1.5",
            "k=#;i1=-2;d1=1.5",
            "k=0;i1=#;d1=1.5",
            "k=1;i1=2;d1=#",
        },
        {"r=2.0", "r=1.5", "r=-2.0", "r=#", "r=#"},
        EValueType::Double);
}

TEST_P(TCaseCodegenTest, TypedNullElse)
{
    CheckCaseResultTypesAndValues(
        MakeCase("7", "else int64(#)"),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        {"r=#", "r=7", "r=#"},
        EValueType::Int64);
}

TEST_P(TCaseCodegenTest, NullOnlyResults)
{
    CheckCaseResultTypesAndValues(
        MakeCase("#", "else #"),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        {"r=#", "r=#", "r=#"},
        EValueType::Null);
}

TEST_P(TCaseCodegenTest, AllNullCaseComparison)
{
    Check(
        std::get<0>(GetParam())
            ? "case # when # then 8 else 9 end"
            : "case when k = # then 8 else 9 end",
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=#"},
        {std::get<1>(GetParam()) ? "r=9" : "r=8"},
        EValueType::Int64);
}

TEST_P(TCaseCodegenTest, NullOnlyCaseOperands)
{
    auto nullExpression = MakeCase("#", "else #");
    auto expected = std::get<1>(GetParam()) ? "r=9" : "r=8";
    Check(
        Format("case (%v) when (%v) then 8 else 9 end", nullExpression, nullExpression),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        {expected, expected, expected},
        EValueType::Int64);
}

TEST_P(TCaseCodegenTest, NullOnlyCaseOperandsWithoutElse)
{
    auto nullExpression = MakeCase("#", "else #");
    auto expected = std::get<1>(GetParam()) ? "r=#" : "r=8";
    Check(
        Format("case (%v) when (%v) then 8 end", nullExpression, nullExpression),
        MakeSplit({{"k", EValueType::Int64}}),
        {"k=0", "k=1", "k=#"},
        {expected, expected, expected},
        EValueType::Int64);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
