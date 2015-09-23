#include "stdafx.h"
#include "framework.h"

#include "ql_helpers.h"

#include <ytlib/query_client/plan_helpers.h>
#include <ytlib/query_client/query_preparer.h>
#include <ytlib/query_client/folding_profiler.h>
#include <ytlib/query_client/query_statistics.h>
#include <ytlib/query_client/column_evaluator.h>
#include <ytlib/query_client/config.h>

// Tests:
// TCompareExpressionTest
// TRefineLookupPredicateTest
// TRefinePredicateTest
// TPrepareExpressionTest
// TArithmeticTest
// TCompareWithNullTest
// TEvaluateExpressionTest
// TEvaluateAggregationTest

namespace NYT {
namespace NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompareExpressionTest
{
protected:
    bool Equal(TConstExpressionPtr lhs, TConstExpressionPtr rhs)
    {
        if (auto literalLhs = lhs->As<TLiteralExpression>()) {
            auto literalRhs = rhs->As<TLiteralExpression>();
            if (literalRhs == nullptr || literalLhs->Value != literalRhs->Value) {
                return false;
            }
        } else if (auto referenceLhs = lhs->As<TReferenceExpression>()) {
            auto referenceRhs = rhs->As<TReferenceExpression>();
            if (referenceRhs == nullptr
                || referenceLhs->ColumnName != referenceRhs->ColumnName) {
                return false;
            }
        } else if (auto functionLhs = lhs->As<TFunctionExpression>()) {
            auto functionRhs = rhs->As<TFunctionExpression>();
            if (functionRhs == nullptr
                || functionLhs->FunctionName != functionRhs->FunctionName
                || functionLhs->Arguments.size() != functionRhs->Arguments.size()) {
                return false;
            }
            for (int index = 0; index < functionLhs->Arguments.size(); ++index) {
                if (!Equal(functionLhs->Arguments[index], functionRhs->Arguments[index])) {
                    return false;
                }
            }
        } else if (auto unaryLhs = lhs->As<TUnaryOpExpression>()) {
            auto unaryRhs = rhs->As<TUnaryOpExpression>();
            if (unaryRhs == nullptr
                || unaryLhs->Opcode != unaryRhs->Opcode
                || !Equal(unaryLhs->Operand, unaryRhs->Operand)) {
                return false;
            }
        } else if (auto binaryLhs = lhs->As<TBinaryOpExpression>()) {
            auto binaryRhs = rhs->As<TBinaryOpExpression>();
            if (binaryRhs == nullptr
                || binaryLhs->Opcode != binaryRhs->Opcode
                || !Equal(binaryLhs->Lhs, binaryRhs->Lhs)
                || !Equal(binaryLhs->Rhs, binaryRhs->Rhs)) {
                return false;
            }
        } else if (auto inLhs = lhs->As<TInOpExpression>()) {
            auto inRhs = rhs->As<TInOpExpression>();
            if (inRhs == nullptr
                || inLhs->Values.Size() != inRhs->Values.Size()
                || inLhs->Arguments.size() != inRhs->Arguments.size()) {
                return false;
            }
            for (int index = 0; index < inLhs->Values.Size(); ++index) {
                if (inLhs->Values[index] != inRhs->Values[index]) {
                    return false;
                }
            }
            for (int index = 0; index < inLhs->Arguments.size(); ++index) {
                if (!Equal(inLhs->Arguments[index], inRhs->Arguments[index])) {
                    return false;
                }
            }
        } else {
            YUNREACHABLE();
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRefineLookupPredicateTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        const char*,
        const char*,
        std::vector<const char*>>>
    , public TCompareExpressionTest
{
protected:
    virtual void SetUp() override
    { }

    TConstExpressionPtr Refine(
        std::vector<TOwningKey>& lookupKeys,
        TConstExpressionPtr expr,
        const TKeyColumns& keyColumns)
    {
        std::vector<TRow> keys;
        keys.reserve(lookupKeys.size());

        for (const auto& lookupKey : lookupKeys) {
            keys.push_back(lookupKey.Get());
        }

        return RefinePredicate(
            keys,
            expr,
            keyColumns);
    }
};

TEST_P(TRefineLookupPredicateTest, Simple)
{
    const auto& args = GetParam();
    const auto& schemaString = std::get<0>(args);
    const auto& keyString = std::get<1>(args);
    const auto& predicateString = std::get<2>(args);
    const auto& refinedString = std::get<3>(args);
    const auto& keyStrings = std::get<4>(args);

    TTableSchema tableSchema;
    TKeyColumns keyColumns;
    Deserialize(tableSchema, ConvertToNode(TYsonString(schemaString)));
    Deserialize(keyColumns, ConvertToNode(TYsonString(keyString)));

    std::vector<TOwningKey> keys;
    Stroka keysString;
    for (const auto& keyString : keyStrings) {
        keys.push_back(BuildKey(keyString));
        keysString += Stroka(keysString.size() > 0 ? ", " : "") + "[" + keyString + "]";
    }

    auto predicate = PrepareExpression(predicateString, tableSchema);
    auto expected = PrepareExpression(refinedString, tableSchema);
    auto refined = Refine(keys, predicate, keyColumns);

    EXPECT_TRUE(Equal(refined, expected))
        << "schema: " << schemaString << std::endl
        << "key_columns: " << keyString << std::endl
        << "keys: " << keysString << std::endl
        << "predicate: " << predicateString << std::endl
        << "refined: " << ::testing::PrintToString(refined) << std::endl
        << "expected: " << ::testing::PrintToString(expected);
}

INSTANTIATE_TEST_CASE_P(
    TRefineLookupPredicateTest,
    TRefineLookupPredicateTest,
    ::testing::Values(
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "(k,l) in ((1,2),(3,4))",
            std::vector<const char*>{"1;3"}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"1;2"}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"1;2", "3;4"}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l,k) in ((1,2),(3,4))",
            "(l,k) in ((1,2),(3,4))",
            std::vector<const char*>{"3;1"}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l,k) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"2;1"}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l,k) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"2;1", "4;3"}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((1),(3))",
            "true",
            std::vector<const char*>{"1;2", "3;4"}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((1),(3))",
            "true",
            std::vector<const char*>{"1", "3"}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "true",
            std::vector<const char*>{"1;2", "3;4"})
));

////////////////////////////////////////////////////////////////////////////////

class TRefinePredicateTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::vector<const char*>>
    , public TCompareExpressionTest
{
protected:
    virtual void SetUp() override
    {
        ColumnEvaluatorCache_ = New<TColumnEvaluatorCache>(
            New<TColumnEvaluatorCacheConfig>(),
            CreateBuiltinFunctionRegistry());
    }

    TConstExpressionPtr Refine(
        const TKeyRange& keyRange,
        TConstExpressionPtr expr,
        const TTableSchema& tableSchema,
        const TKeyColumns& keyColumns)
    {
        return RefinePredicate(
            TRowRange(keyRange.first.Get(), keyRange.second.Get()),
            expr,
            tableSchema,
            keyColumns,
            ColumnEvaluatorCache_->Find(tableSchema, keyColumns.size()));
    }

private:
    TColumnEvaluatorCachePtr ColumnEvaluatorCache_;
};

TEST_P(TRefinePredicateTest, Simple)
{
    const auto& args = GetParam();
    const auto& schemaString = args[0];
    const auto& keyString = args[1];
    const auto& predicateString = args[2];
    const auto& refinedString = args[3];
    const auto& lowerString = args[4];
    const auto& upperString = args[5];

    TTableSchema tableSchema;
    TKeyColumns keyColumns;
    Deserialize(tableSchema, ConvertToNode(TYsonString(schemaString)));
    Deserialize(keyColumns, ConvertToNode(TYsonString(keyString)));

    auto predicate = PrepareExpression(predicateString, tableSchema);
    auto expected = PrepareExpression(refinedString, tableSchema);
    auto range = TKeyRange{BuildKey(lowerString), BuildKey(upperString)};
    auto refined = Refine(range, predicate, tableSchema, keyColumns);

    EXPECT_TRUE(Equal(refined, expected))
        << "schema: " << schemaString << std::endl
        << "key_columns: " << keyString << std::endl
        << "range: [" << lowerString << ", " << upperString << "]" << std::endl
        << "predicate: " << predicateString << std::endl
        << "refined: " << ::testing::PrintToString(refined) << std::endl
        << "expected: " << ::testing::PrintToString(expected);
}

INSTANTIATE_TEST_CASE_P(
    TRefinePredicateTest,
    TRefinePredicateTest,
    ::testing::Values(
        std::vector<const char*>{
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "(k,l) in ((1,2),(3,4))",
            _MIN_,
            _MAX_},
        std::vector<const char*>{
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "(k,l) in ((1,2))",
            "1",
            "2"},
        std::vector<const char*>{
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k) in ((2),(4))",
            "(k) in ((2),(4))",
            _MIN_,
            _MAX_},
        std::vector<const char*>{
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l) in ((2),(4))",
            "(l) in ((2),(4))",
            _MIN_,
            _MAX_},
        std::vector<const char*>{
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k) in ((2),(4))",
            "(k) in ((2))",
            "2;1",
            "3;3"},
        std::vector<const char*>{
            "[{name=k;type=int64;expression=l}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "l in ((2),(4))",
            _MIN_,
            _MAX_},
        std::vector<const char*>{
            "[{name=k;type=int64;expression=l}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "l in ((4))",
            "3;3",
            _MAX_},
        std::vector<const char*>{
            "[{name=k;type=int64;expression=l}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "l in ((2))",
            _MIN_,
            "3;3"},
        std::vector<const char*>{
            "[{name=k;type=int64;expression=l}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((0),(2),(4))",
            "l in ((2))",
            "1;1",
            "3;3"},
        std::vector<const char*>{
            "[{name=k;type=int64;expression=l}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((0),(2),(4))",
            "l in ((2))",
            "1",
            "3"},
        std::vector<const char*>{
            "[{name=k;type=int64;expression=l}; {name=l;type=int64;}; {name=m;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((0),(2),(4))",
            "l in ((2))",
            "2;2;2",
            "3;3;3"},
        std::vector<const char*>{
            "[{name=k;type=int64}; {name=l;type=int64;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "k in ((2))",
            "2;1",
            "3;3"},
        std::vector<const char*>{
            "[{name=k;type=int64}; {name=l;type=int64;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "k in ((2))",
            "2;1",
            "3;3"},
        std::vector<const char*>{
            "[{name=k;type=int64}; {name=l;type=int64;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "k in ((2),(4))",
            "2;1",
            "4;5"},
        std::vector<const char*>{
            "[{name=k;type=int64}; {name=l;type=int64;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "k in ((4))",
            "2;3",
            "4;5"},
        std::vector<const char*>{
            "[{name=k;type=int64}; {name=l;type=int64;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "k in ((2))",
            "2;1",
            "4;3"},
        std::vector<const char*>{
            "[{name=k;type=int64}; {name=l;type=int64;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "k in ((2))",
            "2",
            "3"},
        std::vector<const char*>{
            "[{name=k;type=int64}; {name=l;type=int64;expression=k}; {name=m;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "k in ((2))",
            "2;2;2",
            "3;3;3"}
));

////////////////////////////////////////////////////////////////////////////////

class TPrepareExpressionTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<TConstExpressionPtr, const char*>>
    , public TCompareExpressionTest
{
protected:
    virtual void SetUp() override
    { }
};

TEST_F(TPrepareExpressionTest, Basic)
{
    auto schema = GetSampleTableSchema();

    auto expr1 = Make<TReferenceExpression>("k");
    auto expr2 = PrepareExpression(Stroka("k"), schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    expr1 = Make<TLiteralExpression>(MakeInt64(90));
    expr2 = PrepareExpression(Stroka("90"), schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    expr1 = Make<TReferenceExpression>("a"),
    expr2 = PrepareExpression(Stroka("k"), schema);

    EXPECT_FALSE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    auto str1 = Stroka("k + 3 - a > 4 * l and (k <= m or k + 1 < 3* l)");
    auto str2 = Stroka("k + 3 - a > 4 * l and (k <= m or k + 2 < 3* l)");

    expr1 = PrepareExpression(str1, schema);
    expr2 = PrepareExpression(str1, schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    expr2 = PrepareExpression(str2, schema);

    EXPECT_FALSE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);
}

TEST_P(TPrepareExpressionTest, Simple)
{
    auto schema = GetSampleTableSchema();
    auto& param = GetParam();

    auto expr1 = std::get<0>(param);
    auto expr2 = PrepareExpression(std::get<1>(param), schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);
}

INSTANTIATE_TEST_CASE_P(
    TPrepareExpressionTest,
    TPrepareExpressionTest,
    ::testing::Values(
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::GreaterOrEqual,
                Make<TReferenceExpression>("k"),
                Make<TLiteralExpression>(MakeInt64(90))),
            "k >= 90"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Greater,
                Make<TReferenceExpression>("k"),
                Make<TLiteralExpression>(MakeInt64(90))),
            "k > 90"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Equal,
                Make<TReferenceExpression>("k"),
                Make<TBinaryOpExpression>(EBinaryOp::Plus,
                    Make<TReferenceExpression>("a"),
                    Make<TReferenceExpression>("b"))),
            "k = a + b"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TFunctionExpression>("is_prefix",
                std::initializer_list<TConstExpressionPtr>({
                    Make<TLiteralExpression>(MakeString("abc")),
                    Make<TReferenceExpression>("s")})),
            "is_prefix(\"abc\", s)"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Greater,
                Make<TUnaryOpExpression>(EUnaryOp::Minus,
                    Make<TReferenceExpression>("a")),
                Make<TLiteralExpression>(MakeInt64(-2))),
            "-a > -2"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Minus,
                Make<TUnaryOpExpression>(EUnaryOp::Minus,
                    Make<TReferenceExpression>("a")),
                Make<TLiteralExpression>(MakeInt64(2))),
            "-a - 2"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::NotEqual,
                Make<TReferenceExpression>("a"),
                Make<TLiteralExpression>(MakeInt64(2))),
            "not a = 2"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Or,
                Make<TBinaryOpExpression>(EBinaryOp::GreaterOrEqual,
                    Make<TReferenceExpression>("a"),
                    Make<TLiteralExpression>(MakeInt64(3))),
                Make<TBinaryOpExpression>(EBinaryOp::Less,
                    Make<TReferenceExpression>("a"),
                    Make<TLiteralExpression>(MakeInt64(2)))),
            "not ((a < 3) and (a >= 2))")
));

////////////////////////////////////////////////////////////////////////////////

using TArithmeticTestParam = std::tuple<EValueType, const char*, const char*, const char*, TUnversionedValue>;

class TArithmeticTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TArithmeticTestParam>
    , public TCompareExpressionTest
{
protected:
    virtual void SetUp() override
    { }
};

TEST_P(TArithmeticTest, ConstantFolding)
{
    auto schema = GetSampleTableSchema();
    auto& param = GetParam();
    auto& lhs = std::get<1>(param);
    auto& op = std::get<2>(param);
    auto& rhs = std::get<3>(param);
    auto expected = Make<TLiteralExpression>(std::get<4>(param));

    auto got = PrepareExpression(Stroka(lhs) + op + rhs, schema);

    EXPECT_TRUE(Equal(got, expected))
        << "got: " <<  ::testing::PrintToString(got) << std::endl
        << "expected: " <<  ::testing::PrintToString(expected) << std::endl;
}

TEST_F(TArithmeticTest, ConstantDivisorsFolding)
{
    auto schema = GetSampleTableSchema();
    auto expr1 = PrepareExpression("k / 100 / 2", schema);
    auto expr2 = PrepareExpression("k / 200", schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " <<  ::testing::PrintToString(expr1) << std::endl
        << "expr2: " <<  ::testing::PrintToString(expr2) << std::endl;

    expr1 = PrepareExpression("k / 3102228988 / 4021316745", schema);
    expr2 = PrepareExpression("k / (3102228988 * 4021316745)", schema);

    EXPECT_FALSE(Equal(expr1, expr2))
        << "expr1: " <<  ::testing::PrintToString(expr1) << std::endl
        << "expr2: " <<  ::testing::PrintToString(expr2) << std::endl;

}

TEST_P(TArithmeticTest, Evaluate)
{
    auto& param = GetParam();
    auto type = std::get<0>(param);
    auto& lhs = std::get<1>(param);
    auto& op = std::get<2>(param);
    auto& rhs = std::get<3>(param);
    auto& expected = std::get<4>(param);

    TUnversionedValue result;
    TCGVariables variables;
    std::vector<std::vector<bool>> allLiteralArgs;
    auto keyColumns = GetSampleKeyColumns();
    auto schema = GetSampleTableSchema();
    schema.Columns()[0].Type = type;
    schema.Columns()[1].Type = type;

    auto expr = PrepareExpression(Stroka("k") + op + "l", schema);
    auto callback = Profile(expr, schema, nullptr, &variables, nullptr, &allLiteralArgs, CreateBuiltinFunctionRegistry())();
    auto row = NTableClient::BuildRow(Stroka("k=") + lhs + ";l=" + rhs, keyColumns, schema, true);

    TQueryStatistics statistics;
    auto permanentBuffer = New<TRowBuffer>();
    auto outputBuffer = New<TRowBuffer>();
    auto intermediateBuffer = New<TRowBuffer>();

    // NB: function contexts need to be destroyed before callback since it hosts destructors.
    TExecutionContext executionContext;
    executionContext.Schema = &schema;
    executionContext.LiteralRows = &variables.LiteralRows;
    executionContext.PermanentBuffer = permanentBuffer;
    executionContext.OutputBuffer = outputBuffer;
    executionContext.IntermediateBuffer = intermediateBuffer;
    executionContext.Statistics = &statistics;
#ifndef NDEBUG
    volatile int dummy;
    executionContext.StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif

    std::vector<TFunctionContext*> functionContexts;
    for (auto& literalArgs : allLiteralArgs) {
        executionContext.FunctionContexts.emplace_back(std::move(literalArgs));
    }
    for (auto& functionContext : executionContext.FunctionContexts) {
        functionContexts.push_back(&functionContext);
    }

    callback(&result, row.Get(), variables.ConstantsRowBuilder.GetRow(), &executionContext, &functionContexts[0]);

    EXPECT_EQ(result, expected)
        << "row: " << ::testing::PrintToString(row);
}

INSTANTIATE_TEST_CASE_P(
    TArithmeticTest,
    TArithmeticTest,
    ::testing::Values(
        TArithmeticTestParam(EValueType::Int64, "1", "+", "2", MakeInt64(3)),
        TArithmeticTestParam(EValueType::Int64, "1", "-", "2", MakeInt64(-1)),
        TArithmeticTestParam(EValueType::Int64, "3", "*", "2", MakeInt64(6)),
        TArithmeticTestParam(EValueType::Int64, "6", "/", "2", MakeInt64(3)),
        TArithmeticTestParam(EValueType::Int64, "6", "%", "4", MakeInt64(2)),
        TArithmeticTestParam(EValueType::Int64, "6", "<<", "2", MakeInt64(24)),
        TArithmeticTestParam(EValueType::Int64, "6", ">>", "1", MakeInt64(3)),
        TArithmeticTestParam(EValueType::Int64, "6", ">", "4", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Int64, "6", "<", "4", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Int64, "6", ">=", "4", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Int64, "6", "<=", "4", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Int64, "6", ">=", "6", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Int64, "6", "<=", "6", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Uint64, "1u", "+", "2u", MakeUint64(3)),
        TArithmeticTestParam(EValueType::Uint64, "1u", "-", "2u", MakeUint64(-1)),
        TArithmeticTestParam(EValueType::Uint64, "3u", "*", "2u", MakeUint64(6)),
        TArithmeticTestParam(EValueType::Uint64, "6u", "/", "2u", MakeUint64(3)),
        TArithmeticTestParam(EValueType::Uint64, "6u", "%", "4u", MakeUint64(2)),
        TArithmeticTestParam(EValueType::Uint64, "6u", "<<", "2u", MakeUint64(24)),
        TArithmeticTestParam(EValueType::Uint64, "6u", ">>", "1u", MakeUint64(3)),
        TArithmeticTestParam(EValueType::Uint64, "6u", ">", "4u", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Uint64, "6u", "<", "4u", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Uint64, "6u", ">=", "4u", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Uint64, "6u", "<=", "4u", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Uint64, "6u", ">=", "6u", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Uint64, "6u", "<=", "6u", MakeBoolean(true))
));

////////////////////////////////////////////////////////////////////////////////

using TCompareWithNullTestParam = std::tuple<const char*, const char*, TUnversionedValue>;

class TCompareWithNullTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TCompareWithNullTestParam>
    , public TCompareExpressionTest
{
protected:
    virtual void SetUp() override
    { }
};

TEST_P(TCompareWithNullTest, Simple)
{
    auto& param = GetParam();
    auto& rowString = std::get<0>(param);
    auto& exprString = std::get<1>(param);
    auto& expected = std::get<2>(param);

    TUnversionedValue result;
    TCGVariables variables;
    std::vector<std::vector<bool>> allLiteralArgs;
    auto schema = GetSampleTableSchema();
    auto keyColumns = GetSampleKeyColumns();

    auto row = NTableClient::BuildRow(rowString, keyColumns, schema, true);
    auto expr = PrepareExpression(exprString, schema);
    auto callback = Profile(expr, schema, nullptr, &variables, nullptr, &allLiteralArgs, CreateBuiltinFunctionRegistry())();

    TQueryStatistics statistics;
    auto permanentBuffer = New<TRowBuffer>();
    auto outputBuffer = New<TRowBuffer>();
    auto intermediateBuffer = New<TRowBuffer>();

    // NB: function contexts need to be destroyed before callback since it hosts destructors.
    TExecutionContext executionContext;
    executionContext.Schema = &schema;
    executionContext.PermanentBuffer = permanentBuffer;
    executionContext.OutputBuffer = outputBuffer;
    executionContext.IntermediateBuffer = intermediateBuffer;
    executionContext.Statistics = &statistics;
#ifndef NDEBUG
    volatile int dummy;
    executionContext.StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif

    executionContext.LiteralRows = &variables.LiteralRows;

    std::vector<TFunctionContext*> functionContexts;
    for (auto& literalArgs : allLiteralArgs) {
        executionContext.FunctionContexts.emplace_back(std::move(literalArgs));
    }
    for (auto& functionContext : executionContext.FunctionContexts) {
        functionContexts.push_back(&functionContext);
    }

    callback(&result, row.Get(), variables.ConstantsRowBuilder.GetRow(), &executionContext, &functionContexts[0]);

    EXPECT_EQ(result, expected)
        << "row: " << ::testing::PrintToString(rowString) << std::endl
        << "expr: " << ::testing::PrintToString(exprString) << std::endl;
}

INSTANTIATE_TEST_CASE_P(
    TCompareWithNullTest,
    TCompareWithNullTest,
    ::testing::Values(
        TCompareWithNullTestParam("k=1", "l != k", MakeBoolean(true)),
        TCompareWithNullTestParam("k=1", "l = k", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "l < k", MakeBoolean(true)),
        TCompareWithNullTestParam("k=1", "l > k", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "k <= l", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "k >= l", MakeBoolean(true)),
        TCompareWithNullTestParam("k=1", "l != m", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "l = m", MakeBoolean(true)),
        TCompareWithNullTestParam("k=1", "l < m", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "l > m", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "m <= l", MakeBoolean(true)),
        TCompareWithNullTestParam("k=1", "m >= l", MakeBoolean(true))
));

////////////////////////////////////////////////////////////////////////////////

using TEvaluateAggregationParam = std::tuple<
    const char*,
    EValueType,
    TUnversionedValue,
    TUnversionedValue,
    TUnversionedValue>;

class TEvaluateAggregationTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TEvaluateAggregationParam>
{ };

TEST_P(TEvaluateAggregationTest, Basic)
{
    const auto& param = GetParam();
    const auto& aggregateName = std::get<0>(param);
    auto type = std::get<1>(param);
    auto value1 = std::get<2>(param);
    auto value2 = std::get<3>(param);
    auto expected = std::get<4>(param);

    auto registry = CreateBuiltinFunctionRegistry();
    auto aggregate = registry->GetAggregateFunction(aggregateName);
    auto callbacks = CodegenAggregate(aggregate->MakeCodegenAggregate(type, type, type, aggregateName));

    auto permanentBuffer = New<TRowBuffer>();
    auto outputBuffer = New<TRowBuffer>();
    auto intermediateBuffer = New<TRowBuffer>();

    auto buffer = New<TRowBuffer>();
    TExecutionContext executionContext;
    executionContext.PermanentBuffer = buffer;
    executionContext.OutputBuffer = buffer;
    executionContext.IntermediateBuffer = buffer;
#ifndef NDEBUG
    volatile int dummy;
    executionContext.StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif

    TUnversionedValue tmp;
    TUnversionedValue state1;
    callbacks.Init(&executionContext, &state1);
    EXPECT_EQ(state1.Type, EValueType::Null);

    callbacks.Update(&executionContext, &tmp, &state1, &value1);
    state1 = tmp;
    EXPECT_EQ(value1, state1);

    TUnversionedValue state2;
    callbacks.Init(&executionContext, &state2);
    EXPECT_EQ(state2.Type, EValueType::Null);

    callbacks.Update(&executionContext, &tmp, &state2, &value2);
    state2 = tmp;
    EXPECT_EQ(value2, state2);

    callbacks.Merge(&executionContext, &tmp, &state1, &state2);
    EXPECT_EQ(expected, tmp);

    TUnversionedValue result;
    callbacks.Finalize(&executionContext, &result, &tmp);
    EXPECT_EQ(expected, result);
}

INSTANTIATE_TEST_CASE_P(
    EvaluateAggregationTest,
    TEvaluateAggregationTest,
    ::testing::Values(
        TEvaluateAggregationParam{
            "sum",
            EValueType::Int64,
            MakeUnversionedSentinelValue(EValueType::Null),
            MakeUnversionedSentinelValue(EValueType::Null),
            MakeUnversionedSentinelValue(EValueType::Null)},
        TEvaluateAggregationParam{
            "sum",
            EValueType::Int64,
            MakeUnversionedSentinelValue(EValueType::Null),
            MakeInt64(1),
            MakeInt64(1)},
        TEvaluateAggregationParam{
            "sum",
            EValueType::Int64,
            MakeInt64(1),
            MakeInt64(2),
            MakeInt64(3)},
        TEvaluateAggregationParam{
            "sum",
            EValueType::Uint64,
            MakeUint64(1),
            MakeUint64(2),
            MakeUint64(3)},
        TEvaluateAggregationParam{
            "max",
            EValueType::Int64,
            MakeInt64(10),
            MakeInt64(20),
            MakeInt64(20)},
        TEvaluateAggregationParam{
            "min",
            EValueType::Int64,
            MakeInt64(10),
            MakeInt64(20),
            MakeInt64(10)}
));

////////////////////////////////////////////////////////////////////////////////

class TEvaluateExpressionTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<const char*, const char*, TUnversionedValue>>
{ };

TEST_P(TEvaluateExpressionTest, Basic)
{
    const auto& param = GetParam();
    const auto& rowString = std::get<0>(param);
    const auto& exprString = std::get<1>(param);
    const auto& expected = std::get<2>(param);

    TTableSchema schema;
    schema.Columns().emplace_back(TColumnSchema("i1", EValueType::Int64));
    schema.Columns().emplace_back(TColumnSchema("i2", EValueType::Int64));
    schema.Columns().emplace_back(TColumnSchema("u1", EValueType::Uint64));
    schema.Columns().emplace_back(TColumnSchema("u2", EValueType::Uint64));
    TKeyColumns keyColumns;

    auto expr = PrepareExpression(exprString, schema);

    TCGVariables variables;
    std::vector<std::vector<bool>> allLiteralArgs;

    auto callback = Profile(expr, schema, nullptr, &variables, nullptr, &allLiteralArgs, CreateBuiltinFunctionRegistry())();

    auto row = NTableClient::BuildRow(rowString, keyColumns, schema, true);
    TUnversionedValue result;

    TQueryStatistics statistics;
    auto permanentBuffer = New<TRowBuffer>();
    auto outputBuffer = New<TRowBuffer>();
    auto intermediateBuffer = New<TRowBuffer>();

    // NB: function contexts need to be destroyed before callback since it hosts destructors.
    TExecutionContext executionContext;
    executionContext.Schema = &schema;
    executionContext.LiteralRows = &variables.LiteralRows;
    executionContext.PermanentBuffer = permanentBuffer;
    executionContext.OutputBuffer = outputBuffer;
    executionContext.IntermediateBuffer = intermediateBuffer;
    executionContext.Statistics = &statistics;
#ifndef NDEBUG
    volatile int dummy;
    executionContext.StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif

    std::vector<TFunctionContext*> functionContexts;
    for (auto& literalArgs : allLiteralArgs) {
        executionContext.FunctionContexts.emplace_back(std::move(literalArgs));
    }
    for (auto& functionContext : executionContext.FunctionContexts) {
        functionContexts.push_back(&functionContext);
    }

    callback(&result, row.Get(), variables.ConstantsRowBuilder.GetRow(), &executionContext, &functionContexts[0]);

    EXPECT_EQ(result, expected);
}

INSTANTIATE_TEST_CASE_P(
    EvaluateExpressionTest,
    TEvaluateExpressionTest,
    ::testing::Values(
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=33;i2=22",
            "i1 + i2",
            MakeInt64(33 + 22)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=33",
            "-i1",
            MakeInt64(-33)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=0",
            "uint64(i1)",
            MakeUint64(0)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "u1=0",
            "int64(u1)",
            MakeInt64(0)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "u1=18446744073709551615u",
            "int64(u1)",
            MakeInt64(-1)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=9223372036854775807",
            "uint64(i1)",
            MakeUint64(9223372036854775807ULL)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=-9223372036854775808",
            "uint64(i1)",
            MakeUint64(9223372036854775808ULL))
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NQueryClient
} // namespace NYT
