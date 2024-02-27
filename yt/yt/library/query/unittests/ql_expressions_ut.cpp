#include "ql_helpers.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/library/query/base/functions.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/coordinator.h>

#include <yt/yt/library/query/engine/builtin_function_profiler.h>
#include <yt/yt/library/query/engine/folding_profiler.h>
#include <yt/yt/library/query/engine/functions_cg.h>

#include <yt/yt/core/test_framework/fixed_growth_string_output.h>

#include <library/cpp/resource/resource.h>

// Tests:
// TCompareExpressionTest
// TEliminateLookupPredicateTest
// TEliminatePredicateTest
// TPrepareExpressionTest
// TArithmeticTest
// TCompareWithNullTest
// TEvaluateExpressionTest
// TEvaluateAggregationTest
// TEvaluateAggregationWithStringStateTest
// TExpressionStrConvTest

namespace NYT::NQueryClient {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NTableClient;

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

template <typename TResultMatcher>
void Evaluate(
    const TConstExpressionPtr& expr,
    const TTableSchemaPtr& schema,
    const TRowBufferPtr& buffer,
    const TUnversionedOwningRow& row,
    const TResultMatcher& resultMatcher)
{
    if (EnableWebAssemblyInUnitTests()) {
        auto result = TUnversionedValue{};
        auto variables = TCGVariables();

        auto image = Profile(
            expr,
            schema,
            nullptr,
            &variables,
            /*useCanonicalNullRelations*/ false,
            /*executionBackend*/ EExecutionBackend::WebAssembly)();

        auto instance = image.Instantiate();

        instance.Run(
            variables.GetLiteralValues(),
            variables.GetOpaqueData(),
            variables.GetOpaqueDataSizes(),
            &result,
            row.Elements(),
            buffer.Get());

        resultMatcher(result);
    }

    {
        auto result = TUnversionedValue{};
        auto variables = TCGVariables();

        auto image = Profile(
            expr,
            schema,
            nullptr,
            &variables,
            /*useCanonicalNullRelations*/ false,
            /*executionBackend*/ EExecutionBackend::Native)();

        auto instance = image.Instantiate();

        instance.Run(
            variables.GetLiteralValues(),
            variables.GetOpaqueData(),
            variables.GetOpaqueDataSizes(),
            &result,
            row.Elements(),
            buffer.Get());

        resultMatcher(result);
    }
}

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
            for (int index = 0; index < std::ssize(functionLhs->Arguments); ++index) {
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
        } else if (auto inLhs = lhs->As<TInExpression>()) {
            auto inRhs = rhs->As<TInExpression>();
            if (inRhs == nullptr
                || inLhs->Values.Size() != inRhs->Values.Size()
                || inLhs->Arguments.size() != inRhs->Arguments.size()) {
                return false;
            }
            for (int index = 0; index < std::ssize(inLhs->Values); ++index) {
                if (inLhs->Values[index] != inRhs->Values[index]) {
                    return false;
                }
            }
            for (int index = 0; index < std::ssize(inLhs->Arguments); ++index) {
                if (!Equal(inLhs->Arguments[index], inRhs->Arguments[index])) {
                    return false;
                }
            }
        } else {
            YT_ABORT();
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TExtractSubexpressionPredicateTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        const char*,
        const char*>>
    , public TCompareExpressionTest
{
protected:
    void SetUp() override
    { }

};

TEST_P(TExtractSubexpressionPredicateTest, Simple)
{
    const auto& args = GetParam();
    const auto& schemaString = std::get<0>(args);
    const auto& subschemaString = std::get<1>(args);
    const auto& predicateString = std::get<2>(args);
    const auto& extractedString = std::get<3>(args);

    auto tableSchema = ConvertTo<TTableSchema>(TYsonString(TString(schemaString)));
    auto tableSubschema = ConvertTo<TTableSchema>(TYsonString(TString(subschemaString)));

    auto predicate = PrepareExpression(predicateString, tableSchema);
    auto expected = PrepareExpression(extractedString, tableSubschema);

    auto extracted = ExtractPredicateForColumnSubset(predicate, tableSubschema);

    TConstExpressionPtr extracted2;
    TConstExpressionPtr remaining;
    std::tie(extracted2, remaining) = SplitPredicateByColumnSubset(predicate, tableSubschema);

    EXPECT_TRUE(Equal(extracted, expected))
        << "schema: " << schemaString << std::endl
        << "subschema: " << subschemaString << std::endl
        << "predicate: " << ::testing::PrintToString(predicate) << std::endl
        << "extracted: " << ::testing::PrintToString(extracted) << std::endl
        << "expected: " << ::testing::PrintToString(expected);

    EXPECT_TRUE(Equal(extracted2, expected))
        << "schema: " << schemaString << std::endl
        << "subschema: " << subschemaString << std::endl
        << "predicate: " << ::testing::PrintToString(predicate) << std::endl
        << "extracted2: " << ::testing::PrintToString(extracted2) << std::endl
        << "expected: " << ::testing::PrintToString(expected);
}

INSTANTIATE_TEST_SUITE_P(
    TExtractSubexpressionPredicateTest,
    TExtractSubexpressionPredicateTest,
    ::testing::Values(
        std::tuple(
            "[{name=a;type=boolean;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=boolean;}]",
            "a and b and c",
            "a"),
        std::tuple(
            "[{name=a;type=boolean;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=boolean;}]",
            "not a and b and c",
            "not a"),
        std::tuple(
            "[{name=a;type=int64;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=int64;}]",
            "not is_null(a) and b and c",
            "not is_null(a)"),
        std::tuple(
            "[{name=a;type=int64;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=int64;}]",
            "a in (1, 2, 3) and b and c",
            "a in (1, 2, 3)"),
        std::tuple(
            "[{name=a;type=int64;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=int64;}]",
            "a = 1 and b and c",
            "a = 1"),
        std::tuple(
            "[{name=a;type=int64;}; {name=b;type=int64}; {name=c;type=boolean}]",
            "[{name=a;type=int64;}; {name=b;type=int64}]",
            "a = b and c",
            "a = b"),
        std::tuple(
            "[{name=a;type=boolean;}; {name=b;type=int64}; {name=c;type=boolean}]",
            "[{name=a;type=boolean;}; {name=b;type=int64}]",
            "if(a, b = 1, false) and c",
            "if(a, b = 1, false)"),
        std::tuple(
            "[{name=a;type=boolean;}; {name=b;type=boolean}]",
            "[{name=a;type=boolean;};]",
            "a or b",
            "true")
));

////////////////////////////////////////////////////////////////////////////////

class TEliminateLookupPredicateTest
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
    void SetUp() override
    { }

    TConstExpressionPtr Eliminate(
        std::vector<TLegacyOwningKey>& lookupKeys,
        TConstExpressionPtr expr,
        const TKeyColumns& keyColumns)
    {
        std::vector<TRow> keys;
        keys.reserve(lookupKeys.size());

        for (const auto& lookupKey : lookupKeys) {
            keys.push_back(lookupKey);
        }

        return EliminatePredicate(keys, expr, keyColumns);
    }
};

TEST_P(TEliminateLookupPredicateTest, Simple)
{
    const auto& args = GetParam();
    const auto& schemaString = std::get<0>(args);
    const auto& keyString = std::get<1>(args);
    const auto& predicateString = std::get<2>(args);
    const auto& refinedString = std::get<3>(args);
    const auto& keyStrings = std::get<4>(args);

    TTableSchema tableSchema;
    TKeyColumns keyColumns;
    Deserialize(tableSchema, ConvertToNode(TYsonString(TString(schemaString))));
    Deserialize(keyColumns, ConvertToNode(TYsonString(TString(keyString))));

    std::vector<TLegacyOwningKey> keys;
    TString keysString;
    for (const auto& keyString : keyStrings) {
        keys.push_back(YsonToKey(keyString));
        keysString += TString(keysString.size() > 0 ? ", " : "") + "[" + keyString + "]";
    }

    auto predicate = PrepareExpression(predicateString, tableSchema);
    auto expected = PrepareExpression(refinedString, tableSchema);
    auto refined = Eliminate(keys, predicate, keyColumns);

    EXPECT_TRUE(Equal(refined, expected))
        << "schema: " << schemaString << std::endl
        << "key_columns: " << keyString << std::endl
        << "keys: " << keysString << std::endl
        << "predicate: " << predicateString << std::endl
        << "refined: " << ::testing::PrintToString(refined) << std::endl
        << "expected: " << ::testing::PrintToString(expected);
}

INSTANTIATE_TEST_SUITE_P(
    TEliminateLookupPredicateTest,
    TEliminateLookupPredicateTest,
    ::testing::Values(
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "false",
            std::vector<const char*>{"1;3"}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"1;2"}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"1;2", "3;4"}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l,k) in ((1,2),(3,4))",
            "false",
            std::vector<const char*>{"3;1"}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l,k) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"2;1"}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l,k) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"2;1", "4;3"}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((1),(3))",
            "true",
            std::vector<const char*>{"1;2", "3;4"}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((1),(3))",
            "true",
            std::vector<const char*>{"1", "3"}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "true",
            std::vector<const char*>{"1;2", "3;4"})
));

////////////////////////////////////////////////////////////////////////////////

class TEliminatePredicateTest
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
    TConstExpressionPtr Eliminate(
        const std::vector<TKeyRange>& keyRanges,
        TConstExpressionPtr expr,
        const TKeyColumns& keyColumns)
    {
        TRowRanges rowRanges;
        for (const auto& keyRange : keyRanges) {
            rowRanges.emplace_back(keyRange.first.Get(), keyRange.second.Get());
        }

        return EliminatePredicate(rowRanges, expr, keyColumns);
    }
};

TEST_P(TEliminatePredicateTest, Simple)
{
    const auto& args = GetParam();
    const auto& schemaString = std::get<0>(args);
    const auto& keyString = std::get<1>(args);
    const auto& predicateString = std::get<2>(args);
    const auto& refinedString = std::get<3>(args);
    const auto& keyStrings = std::get<4>(args);

    const auto& lowerString = keyStrings[0];
    const auto& upperString = keyStrings[1];

    TTableSchema tableSchema;
    TKeyColumns keyColumns;
    Deserialize(tableSchema, ConvertToNode(TYsonString(TString(schemaString))));
    Deserialize(keyColumns, ConvertToNode(TYsonString(TString(keyString))));

    auto predicate = PrepareExpression(predicateString, tableSchema);
    auto expected = PrepareExpression(refinedString, tableSchema);

    std::vector<TKeyRange> owningRanges;
    for (size_t i = 0; i < keyStrings.size() / 2; ++i) {
        owningRanges.emplace_back(YsonToKey(keyStrings[2 * i]), YsonToKey(keyStrings[2 * i + 1]));
    }

    auto refined = Eliminate(owningRanges, predicate, keyColumns);

    EXPECT_TRUE(Equal(refined, expected))
        << "schema: " << schemaString << std::endl
        << "key_columns: " << keyString << std::endl
        << "range: [" << lowerString << ", " << upperString << "]" << std::endl
        << "predicate: " << predicateString << std::endl
        << "refined: " << ::testing::PrintToString(refined) << std::endl
        << "expected: " << ::testing::PrintToString(expected);
}

INSTANTIATE_TEST_SUITE_P(
    TEliminatePredicateTestOld,
    TEliminatePredicateTest,
    ::testing::Values(
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "(k,l) in ((1,2),(3,4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "(k,l) in ((1,2))",
            std::vector<const char*>{"1", "2"}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k) in ((2),(4))",
            "(k) in ((2),(4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l) in ((2),(4))",
            "(l) in ((2),(4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k) in ((2),(4))",
            "(k) in ((2))",
            std::vector<const char*>{"2;1", "3;3"}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending;expression=l}; {name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "l in ((2),(4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "k in ((2))",
            std::vector<const char*>{"2;1", "3;3"}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "k in ((2))",
            std::vector<const char*>{"2;1", "3;3"}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "k in ((2),(4))",
            std::vector<const char*>{"2;1", "4;5"}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "k in ((2))",
            std::vector<const char*>{"2", "3"}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=m;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "k in ((2))",
            std::vector<const char*>{"2;2;2", "3;3;3"})
));

INSTANTIATE_TEST_SUITE_P(
    TEliminatePredicateTest,
    TEliminatePredicateTest,
    ::testing::Values(
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k = 1 and l in (1,2,3)",
            "true",
            std::vector<const char*>{"1;1", "1;1;" _MAX_, "1;2", "1;2;" _MAX_, "1;3", "1;3;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k in (1,2,3) and l = 1",
            "true",
            std::vector<const char*>{"1;1", "1;1;" _MAX_, "2;1", "2;1;" _MAX_, "3;1", "3;1;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"1;2", "1;2;" _MAX_, "3;4", "3;4;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k) in ((2),(4))",
            "true",
            std::vector<const char*>{"2", "2;" _MAX_, "4", "4;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l) in ((2),(4))",
            "(l) in ((2),(4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending;expression=l}; {name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "true",
            std::vector<const char*>{"2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending;expression=\"l+1\"}; {name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "true",
            std::vector<const char*>{"3;2", "3;2;" _MAX_, "5;4", "5;4;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending;expression=l}; {name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((0),(2),(4))",
            "true",
            std::vector<const char*>{"0;0", "0;0;" _MAX_, "2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "true",
            std::vector<const char*>{"0;0", "0;0;" _MAX_, "2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "true",
            std::vector<const char*>{"0;0", "0;0;" _MAX_, "2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_, "6;6", "6;6;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in (1,2,3,4,5) or k > 10",
            "k in (1,2,3,4,5) or k > 10",
            std::vector<const char*>{"1;1", "1;1;" _MAX_, "2;2", "2;2;" _MAX_, "3;3", "3;3;" _MAX_, "4;4", "4;4;" _MAX_, "5;5", "5;5;" _MAX_, "10;" _MAX_, _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in (1,2,3,4,5) or k > 10",
            "true",
            std::vector<const char*>{"1;1", "1;1;" _MAX_, "2;2", "2;2;" _MAX_, "3;3", "3;3;" _MAX_, "4;4", "4;4;" _MAX_, "5;5", "5;5;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in (1,2,3,4,5) or k in (11,12,14,15)",
            "k in (4,5) or k in (11,12)",
            std::vector<const char*>{"4;4", "4;4;" _MAX_, "5;5", "5;5;" _MAX_, "11;11", "11;11;" _MAX_, "12;12", "12;12;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2)) or k in ((4),(6))",
            "k in ((0),(2)) or k in ((4),(6))",
            std::vector<const char*>{"0;0", "0;0;" _MAX_, "2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_, "6;6", "6;6;" _MAX_}),
        std::tuple(
            "[{name=k;type=int64;sort_order=ascending;}; {name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[k;l]",
            "l in (20, 123, 15)",
            "l in (20, 15)",
            std::vector<const char*>{"0;10", "0;20", "0;20", "0;30"})
));

////////////////////////////////////////////////////////////////////////////////

class TPrepareExpressionTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<TConstExpressionPtr, const char*>>
    , public TCompareExpressionTest
{
protected:
    void SetUp() override
    { }
};

TEST_F(TPrepareExpressionTest, Basic)
{
    auto schema = GetSampleTableSchema();

    auto expr1 = Make<TReferenceExpression>("k");
    auto expr2 = PrepareExpression(TString("k"), *schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    expr1 = Make<TLiteralExpression>(MakeInt64(90));
    expr2 = PrepareExpression(TString("90"), *schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    expr1 = Make<TReferenceExpression>("a"),
    expr2 = PrepareExpression(TString("k"), *schema);

    EXPECT_FALSE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    auto str1 = TString("k + 3 - a > 4 * l and (k <= m or k + 1 < 3* l)");
    auto str2 = TString("k + 3 - a > 4 * l and (k <= m or k + 2 < 3* l)");

    expr1 = PrepareExpression(str1, *schema);
    expr2 = PrepareExpression(str1, *schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    expr2 = PrepareExpression(str2, *schema);

    EXPECT_FALSE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);
}

TEST_F(TPrepareExpressionTest, CompareTuple)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("a", EValueType::Int64),
        TColumnSchema("b", EValueType::Int64),
        TColumnSchema("c", EValueType::Int64),
        TColumnSchema("d", EValueType::Int64),
        TColumnSchema("e", EValueType::Int64),
        TColumnSchema("f", EValueType::Int64),
        TColumnSchema("g", EValueType::Int64),
        TColumnSchema("h", EValueType::Int64),
        TColumnSchema("i", EValueType::Int64),
        TColumnSchema("j", EValueType::Int64),
        TColumnSchema("k", EValueType::Int64),
        TColumnSchema("l", EValueType::Int64),
        TColumnSchema("m", EValueType::Int64),
        TColumnSchema("n", EValueType::Int64)
    });

    TKeyColumns keyColumns;

    auto expr = PrepareExpression("(a, b, c, d, e, f, g, h, i, j, k, l, m, n) < (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)", *schema);

    TCGVariables variables;
    ProfileForBothExecutionBackends(expr, schema, nullptr, &variables);
}

TEST_P(TPrepareExpressionTest, Simple)
{
    auto schema = GetSampleTableSchema();
    auto& param = GetParam();

    auto expr1 = std::get<0>(param);
    auto expr2 = PrepareExpression(std::get<1>(param), *schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);
}

INSTANTIATE_TEST_SUITE_P(
    CheckExpressions,
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
                Make<TBinaryOpExpression>(EBinaryOp::Equal,
                    Make<TReferenceExpression>("a"),
                    Make<TLiteralExpression>(MakeInt64(0))),
                Make<TFunctionExpression>("is_null",
                    std::initializer_list<TConstExpressionPtr>({
                        Make<TReferenceExpression>("b")}))),
            "a = 0 or b is null"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TFunctionExpression>("is_null",
                std::initializer_list<TConstExpressionPtr>({
                    Make<TBinaryOpExpression>(EBinaryOp::Equal,
                        Make<TReferenceExpression>("a"),
                        Make<TReferenceExpression>("b"))})),
            "a = b is null"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TUnaryOpExpression>(EUnaryOp::Not,
                Make<TFunctionExpression>("is_null",
                    std::initializer_list<TConstExpressionPtr>({
                            Make<TReferenceExpression>("a")}))),
            "not a is null"),
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

TSharedRange<TRow> MakeRows(const TString& yson)
{
    TUnversionedRowBuilder keyBuilder;
    auto keyParts = ConvertTo<std::vector<INodePtr>>(
        TYsonString(yson, EYsonType::ListFragment));

    auto buffer = New<TRowBuffer>();
    std::vector<TRow> rows;

    for (int id = 0; id < std::ssize(keyParts); ++id) {
        keyBuilder.Reset();

        const auto& keyPart = keyParts[id];
        switch (keyPart->GetType()) {
            #define XX(type, cppType) \
            case ENodeType::type: \
                keyBuilder.AddValue(Make ## type ## Value<TUnversionedValue>( \
                    keyPart->As ## type()->GetValue(), \
                    id)); \
                break;
            ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
            #undef XX
            case ENodeType::Entity:
                keyBuilder.AddValue(MakeSentinelValue<TUnversionedValue>(
                    keyPart->Attributes().Get<EValueType>("type"),
                    id));
                break;
            default:
                keyBuilder.AddValue(MakeAnyValue<TUnversionedValue>(
                    ConvertToYsonString(keyPart).AsStringBuf(),
                    id));
                break;
        }

        rows.push_back(buffer->CaptureRow(keyBuilder.GetRow()));
    }

    return MakeSharedRange(std::move(rows), buffer);
}

TEST_F(TPrepareExpressionTest, Negative1)
{
    auto schema = GetSampleTableSchema();

    EXPECT_THROW_THAT(
        PrepareExpression(TString("ki in (1, 2u, \"abc\")"), *schema),
        HasSubstr("Types mismatch in tuple"));

    EXPECT_THROW_THAT(
        PrepareExpression(TString("ku = \"abc\""), *schema),
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        PrepareExpression(TString("kd = 4611686018427387903"), *schema),
        HasSubstr("to double: inaccurate conversion"));

    EXPECT_THROW_THAT(
        PrepareExpression(TString("kd = 9223372036854775807u"), *schema),
        HasSubstr("to double: inaccurate conversion"));

    EXPECT_THROW_THAT(
        PrepareExpression(TString("ki = 18446744073709551606u"), *schema),
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        PrepareExpression(TString("ku = 1.5"), *schema),
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        PrepareExpression(TString("ki = 1.5"), *schema),
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        PrepareExpression(TString("(1u - 2) / 3.0"), *schema),
        HasSubstr("to double: inaccurate conversion"));

    EXPECT_THROW_THAT(
        PrepareExpression(TString("k = 1 and ku"), *schema),
        HasSubstr("Type mismatch in expression"));
}

INSTANTIATE_TEST_SUITE_P(
    CheckExpressions2,
    TPrepareExpressionTest,
    ::testing::Values(
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Equal,
                Make<TReferenceExpression>("ku"),
                Make<TLiteralExpression>(MakeUint64(1))),
            "ku = 1"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Equal,
                Make<TReferenceExpression>("kd"),
                Make<TLiteralExpression>(MakeDouble(1))),
            "kd = 1"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Equal,
                Make<TReferenceExpression>("kd"),
                Make<TLiteralExpression>(MakeDouble(1))),
            "kd = 1u"),
        std::tuple<TConstExpressionPtr, const char*>(
            New<TInExpression>(
                std::initializer_list<TConstExpressionPtr>({
                    Make<TLiteralExpression>(MakeInt64(4))}),
                MakeRows("1; 2; 3")),
            "4 in (1, 2u, 3.0)"),
        std::tuple<TConstExpressionPtr, const char*>(
            New<TInExpression>(
                std::initializer_list<TConstExpressionPtr>({
                    Make<TReferenceExpression>("ki")}),
                MakeRows("1; 2; 3")),
            "ki in (1, 2u, 3.0)"),
        std::tuple<TConstExpressionPtr, const char*>(
            New<TInExpression>(
                std::initializer_list<TConstExpressionPtr>({
                    Make<TReferenceExpression>("ku")}),
                MakeRows("1u; 2u; 3u")),
            "ku in (1, 2u, 3.0)"),
        std::tuple<TConstExpressionPtr, const char*>(
            New<TInExpression>(
                std::initializer_list<TConstExpressionPtr>({
                    Make<TReferenceExpression>("kd")}),
                MakeRows("1.0; 2.0; 3.0")),
            "kd in (1, 2u, 3.0)"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Equal,
                Make<TReferenceExpression>("kd"),
                Make<TLiteralExpression>(MakeDouble(3))),
            "kd = 1u + 2"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Equal,
                Make<TReferenceExpression>("ku"),
                Make<TLiteralExpression>(MakeUint64(18446744073709551615llu))),
            "ku = 1u - 2"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Equal,
                Make<TReferenceExpression>("ku"),
                Make<TLiteralExpression>(MakeUint64(6148914691236517205llu))),
            "ku = (1u - 2) / 3"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Equal,
                Make<TReferenceExpression>("ku"),
                Make<TLiteralExpression>(MakeUint64(61489146912365173llu))),
            "ku = 184467440737095520u / 3")
));

INSTANTIATE_TEST_SUITE_P(
    CheckPriorities,
    TPrepareExpressionTest,
    ::testing::Values(
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Modulo,
                Make<TBinaryOpExpression>(EBinaryOp::Divide,
                    Make<TBinaryOpExpression>(EBinaryOp::Multiply,
                        Make<TUnaryOpExpression>(EUnaryOp::Minus, Make<TReferenceExpression>("a")),
                        Make<TUnaryOpExpression>(EUnaryOp::Plus, Make<TReferenceExpression>("b"))),
                    Make<TUnaryOpExpression>(EUnaryOp::BitNot, Make<TReferenceExpression>("c"))),
                Make<TLiteralExpression>(MakeInt64(100))),
            "-a * +b / ~c % 100"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Plus,
                Make<TBinaryOpExpression>(EBinaryOp::Multiply,
                    Make<TUnaryOpExpression>(EUnaryOp::Minus, Make<TReferenceExpression>("a")),
                    Make<TUnaryOpExpression>(EUnaryOp::Plus, Make<TReferenceExpression>("b"))),
                Make<TBinaryOpExpression>(EBinaryOp::Divide,
                    Make<TUnaryOpExpression>(EUnaryOp::BitNot, Make<TReferenceExpression>("c")),
                    Make<TLiteralExpression>(MakeInt64(100)))),
            "-a * +b + ~c / 100"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::BitOr,
                Make<TBinaryOpExpression>(EBinaryOp::BitAnd,
                    Make<TReferenceExpression>("k"),
                    Make<TBinaryOpExpression>(EBinaryOp::LeftShift,
                        Make<TBinaryOpExpression>(EBinaryOp::Plus,
                            Make<TReferenceExpression>("a"),
                            Make<TReferenceExpression>("b")),
                        Make<TReferenceExpression>("c"))),
                Make<TBinaryOpExpression>(EBinaryOp::RightShift,
                    Make<TReferenceExpression>("l"),
                    Make<TReferenceExpression>("m"))),
            "k & a + b << c | l >> m"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::NotEqual,
                Make<TBinaryOpExpression>(EBinaryOp::Greater,
                    Make<TReferenceExpression>("c"),
                    Make<TReferenceExpression>("b")),
                Make<TBinaryOpExpression>(EBinaryOp::Less,
                    Make<TReferenceExpression>("a"),
                    Make<TReferenceExpression>("b"))),
            "c > b != a < b"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::NotEqual,
                Make<TBinaryOpExpression>(EBinaryOp::Greater,
                    Make<TReferenceExpression>("c"),
                    Make<TReferenceExpression>("b")),
                Make<TBinaryOpExpression>(EBinaryOp::Less,
                    Make<TReferenceExpression>("a"),
                    Make<TReferenceExpression>("b"))),
            "c > b <> a < b"),
        std::tuple<TConstExpressionPtr, const char*>(
            Make<TBinaryOpExpression>(EBinaryOp::Or,
                Make<TBinaryOpExpression>(EBinaryOp::NotEqual,
                    Make<TBinaryOpExpression>(EBinaryOp::Less,
                        Make<TReferenceExpression>("a"),
                        Make<TReferenceExpression>("b")),
                    Make<TBinaryOpExpression>(EBinaryOp::Greater,
                        Make<TReferenceExpression>("c"),
                        Make<TReferenceExpression>("b"))),
                Make<TBinaryOpExpression>(EBinaryOp::And,
                    Make<TBinaryOpExpression>(EBinaryOp::GreaterOrEqual,
                        Make<TReferenceExpression>("k"),
                        Make<TReferenceExpression>("l")),
                    Make<TBinaryOpExpression>(EBinaryOp::LessOrEqual,
                        Make<TReferenceExpression>("k"),
                        Make<TReferenceExpression>("m")))),
            "NOT a < b = c > b OR k >= l AND k <= m")
));

////////////////////////////////////////////////////////////////////////////////

using TArithmeticTestParam = std::tuple<EValueType, const char*, const char*, const char*, TUnversionedValue>;

class TExpressionTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TArithmeticTestParam>
    , public TCompareExpressionTest
{
protected:
    void SetUp() override
    { }
};

TEST_P(TExpressionTest, ConstantFolding)
{
    auto schema = GetSampleTableSchema();
    auto& param = GetParam();
    auto& lhs = std::get<1>(param);
    auto& op = std::get<2>(param);
    auto& rhs = std::get<3>(param);
    auto expected = Make<TLiteralExpression>(std::get<4>(param));

    auto got = PrepareExpression(TString(lhs) + " " + op + " " + rhs, *schema);

    EXPECT_TRUE(Equal(got, expected))
        << "got: " <<  ::testing::PrintToString(got) << std::endl
        << "expected: " <<  ::testing::PrintToString(expected) << std::endl;
}

TEST_F(TExpressionTest, FunctionNullArgument)
{
    auto schema = GetSampleTableSchema();
    auto buffer = New<TRowBuffer>();

    TUnversionedOwningRow row;

    {
        auto expr = PrepareExpression("int64(null)", *schema);

        EXPECT_EQ(*expr->LogicalType, *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)));

        Evaluate(expr, schema, buffer, row, [&] (const TUnversionedValue& result) {
            EXPECT_EQ(result, MakeNull());
        });
    }

    EXPECT_THROW_THAT(
        PrepareExpression("if(null, null, null)", *schema),
        HasSubstr("Type inference failed"));

    EXPECT_THROW_THAT(
        PrepareExpression("if(true, null, null)", *schema),
        HasSubstr("Type inference failed"));

    EXPECT_THROW_THAT(
        PrepareExpression("is_null(null)", *schema),
        HasSubstr("Type inference failed"));

    {
        auto expr = PrepareExpression("if(null, 1, 2)", *schema);
        EXPECT_EQ(*expr->LogicalType, *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)));

        Evaluate(expr, schema, buffer, row, [&] (const TUnversionedValue& result) {
            EXPECT_EQ(result, MakeNull());
        });
    }

    {
        auto expr = PrepareExpression("if(false, 1, null)", *schema);
        EXPECT_EQ(*expr->LogicalType, *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)));

        Evaluate(expr, schema, buffer, row, [&] (const TUnversionedValue& result) {
            EXPECT_EQ(result, MakeNull());
        });
    }
}

TEST_F(TExpressionTest, Aliasing)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("s", EValueType::String).SetSortOrder(ESortOrder::Ascending),
    });

    auto buffer = New<TRowBuffer>();
    auto expr = PrepareExpression("lower(s)", *schema);

    {
        auto variables = TCGVariables();
        auto value = MakeUnversionedStringValue("ABCD");

        auto image = Profile(
            expr,
            schema,
            /*id*/ nullptr,
            &variables,
            /*useCanonicalNullRelations*/ false,
            /*executionBackend*/ EExecutionBackend::Native)();

        auto instance = image.Instantiate();

        instance.Run(
            variables.GetLiteralValues(),
            variables.GetOpaqueData(),
            variables.GetOpaqueDataSizes(),
            &value,
            TRange<TValue>(&value, 1),
            buffer);

        EXPECT_EQ(value.AsStringBuf(), "abcd");
    }

    if (EnableWebAssemblyInUnitTests()) {
        auto variables = TCGVariables();
        auto value = MakeUnversionedStringValue("ABCD");

        auto image = Profile(
            expr,
            schema,
            /*id*/ nullptr,
            &variables,
            /*useCanonicalNullRelations*/ false,
            /*executionBackend*/ EExecutionBackend::WebAssembly)();

        auto instance = image.Instantiate();

        instance.Run(
            variables.GetLiteralValues(),
            variables.GetOpaqueData(),
            variables.GetOpaqueDataSizes(),
            &value,
            TRange<TValue>(&value, 1),
            buffer);

        EXPECT_EQ(value.AsStringBuf(), "abcd");
    }
}

TUnversionedValue YsonToUnversionedValue(TStringBuf str)
{
    NYson::TTokenizer tokenizer(str);
    const auto& token = SkipAttributes(&tokenizer);
    switch (token.GetType()) {
        case NYson::ETokenType::Int64:
            return MakeUnversionedInt64Value(token.GetInt64Value());
        case NYson::ETokenType::Uint64:
            return MakeUnversionedUint64Value(token.GetUint64Value());
        case NYson::ETokenType::Double:
            return MakeUnversionedDoubleValue(token.GetDoubleValue());
        case NYson::ETokenType::Boolean:
            return MakeUnversionedBooleanValue(token.GetBooleanValue());
        case NYson::ETokenType::String:
            return MakeUnversionedStringValue(token.GetStringValue());
        case NYson::ETokenType::Hash:
            return MakeUnversionedNullValue();
        default:
            YT_ABORT();
    }
}

TEST_P(TExpressionTest, Evaluate)
{
    auto& param = GetParam();
    auto type = std::get<0>(param);
    auto lhs = std::get<1>(param);
    auto& op = std::get<2>(param);
    auto rhs = std::get<3>(param);
    auto& expected = std::get<4>(param);

    auto lhsValue = YsonToUnversionedValue(lhs);
    auto rhsValue = YsonToUnversionedValue(rhs);

    auto lhsType = lhsValue.Type != EValueType::Null ? lhsValue.Type : type;
    auto rhsType = rhsValue.Type != EValueType::Null ? rhsValue.Type : type;

    auto columns = GetSampleTableSchema()->Columns();
    columns[0].SetLogicalType(OptionalLogicalType(SimpleLogicalType(GetLogicalType(lhsType))));
    columns[1].SetLogicalType(OptionalLogicalType(SimpleLogicalType(GetLogicalType(rhsType))));
    auto schema = New<TTableSchema>(std::move(columns));

    auto expr = PrepareExpression(TString("k") + " " + op + " " + "l", *schema);
    auto buffer = New<TRowBuffer>();
    auto row = YsonToSchemafulRow(TString("k=") + lhs + ";l=" + rhs, *schema, true);

    Evaluate(expr, schema, buffer, row, [&] (const TUnversionedValue& result) {
        EXPECT_EQ(result, expected)
            << "row: " << ::testing::PrintToString(row);
    });
}

TEST_P(TExpressionTest, EvaluateLhsValueRhsLiteral)
{
    auto& param = GetParam();
    auto type = std::get<0>(param);
    auto lhs = std::get<1>(param);
    auto& op = std::get<2>(param);
    auto rhs = std::get<3>(param);
    auto& expected = std::get<4>(param);

    auto lhsValue = YsonToUnversionedValue(lhs);
    auto rhsValue = YsonToUnversionedValue(rhs);

    auto lhsType = lhsValue.Type != EValueType::Null ? lhsValue.Type : type;
    auto rhsType = rhsValue.Type != EValueType::Null ? rhsValue.Type : type;

    auto columns = GetSampleTableSchema()->Columns();
    columns[0].SetLogicalType(OptionalLogicalType(SimpleLogicalType(GetLogicalType(lhsType))));
    columns[1].SetLogicalType(OptionalLogicalType(SimpleLogicalType(GetLogicalType(rhsType))));
    auto schema = New<TTableSchema>(std::move(columns));

    auto expr = PrepareExpression(TString("k") + " " + op + " " + rhs, *schema);
    auto row = YsonToSchemafulRow(TString("k=") + lhs, *schema, true);
    auto buffer = New<TRowBuffer>();

    Evaluate(expr, schema, buffer, row, [&] (const TUnversionedValue& result) {
        EXPECT_EQ(result, expected)
            << "row: " << ::testing::PrintToString(row);
    });
}

TEST_P(TExpressionTest, EvaluateLhsLiteralRhsValue)
{
    auto& param = GetParam();
    auto type = std::get<0>(param);
    auto lhs = std::get<1>(param);
    auto& op = std::get<2>(param);
    auto rhs = std::get<3>(param);
    auto& expected = std::get<4>(param);

    auto lhsValue = YsonToUnversionedValue(lhs);
    auto rhsValue = YsonToUnversionedValue(rhs);

    auto lhsType = lhsValue.Type != EValueType::Null ? lhsValue.Type : type;
    auto rhsType = rhsValue.Type != EValueType::Null ? rhsValue.Type : type;

    auto columns = GetSampleTableSchema()->Columns();
    columns[0].SetLogicalType(OptionalLogicalType(SimpleLogicalType(GetLogicalType(lhsType))));
    columns[1].SetLogicalType(OptionalLogicalType(SimpleLogicalType(GetLogicalType(rhsType))));
    auto schema = New<TTableSchema>(std::move(columns));

    auto expr = PrepareExpression(TString(lhs) + " " + op + " " + "l", *schema);
    auto row = YsonToSchemafulRow(TString("l=") + rhs, *schema, true);
    auto buffer = New<TRowBuffer>();

    Evaluate(expr, schema, buffer, row, [&] (const TUnversionedValue& result) {
        EXPECT_EQ(result, expected)
            << "row: " << ::testing::PrintToString(row);
    });
}

INSTANTIATE_TEST_SUITE_P(
    TArithmeticNullTest,
    TExpressionTest,
    ::testing::Values(
        TArithmeticTestParam(EValueType::Boolean, "#", "or", "#", MakeNull()),
        TArithmeticTestParam(EValueType::Boolean, "#", "or", "%true", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Boolean, "%true", "or", "#", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Boolean, "%true", "or", "%true", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Boolean, "#", "or", "%false", MakeNull()),
        TArithmeticTestParam(EValueType::Boolean, "%false", "or", "#", MakeNull()),
        TArithmeticTestParam(EValueType::Boolean, "%false", "or", "%false", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Boolean, "%true", "or", "%false", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Boolean, "%false", "or", "%true", MakeBoolean(true)),

        TArithmeticTestParam(EValueType::Boolean, "#", "and", "#", MakeNull()),
        TArithmeticTestParam(EValueType::Boolean, "#", "and", "%true", MakeNull()),
        TArithmeticTestParam(EValueType::Boolean, "%true", "and", "#", MakeNull()),
        TArithmeticTestParam(EValueType::Boolean, "%true", "and", "%true", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Boolean, "#", "and", "%false",  MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Boolean, "%false", "and", "#",  MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Boolean, "%false", "and", "%false", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Boolean, "%true", "and", "%false",  MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Boolean, "%false", "and", "%true",  MakeBoolean(false)),

        TArithmeticTestParam(EValueType::Int64, "#", "=", "#", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Int64, "#", "!=", "#", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Int64, "#", "<>", "#", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Int64, "1", "=", "#", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Int64, "1", "!=", "#", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Int64, "1", "<>", "#", MakeBoolean(true)),

        TArithmeticTestParam(EValueType::Int64, "1", "+", "#", MakeNull()),

        TArithmeticTestParam(EValueType::Boolean, "%false", "<", "%true", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Boolean, "%true", "<", "%false", MakeBoolean(false))
));

////////////////////////////////////////////////////////////////////////////////

class TArithmeticExpressionTest
    : public ::testing::Test
    , public TCompareExpressionTest
{
protected:
    void SetUp() override
    { }
};

TString UnversionedValueToString(TUnversionedValue value)
{
    TString result;
    TFixedGrowthStringOutput outStream(&result, 300);
    NYson::TUncheckedYsonTokenWriter writer(&outStream);

    switch (value.Type) {
        case EValueType::Int64:
            writer.WriteTextInt64(value.Data.Int64);
            break;
        case EValueType::Uint64:
            writer.WriteTextUint64(value.Data.Uint64);
            break;
        case EValueType::Double:
            writer.WriteTextDouble(value.Data.Double);
            break;
        case EValueType::Boolean:
            writer.WriteTextBoolean(value.Data.Boolean);
            break;
        case EValueType::String:
            writer.WriteTextString(value.AsStringBuf());
            break;
        case EValueType::Null:
            writer.WriteEntity();
            break;
        default:
            YT_ABORT();
    }

    return result;
}

TRange<TUnversionedValue> GetValuesForType(EValueType type)
{
    if (type == EValueType::Int64) {
        static auto result = {
            MakeInt64(0),
            MakeInt64(1),
            MakeInt64(2),
            MakeInt64(13),
            MakeInt64(-1),
            MakeInt64(-2),
            MakeInt64(-10),
            MakeInt64(std::numeric_limits<i64>::min()),
            MakeInt64(std::numeric_limits<i64>::max()),
            MakeInt64(std::numeric_limits<i64>::min() + 10),
            MakeInt64(std::numeric_limits<i64>::max() - 10)
            };
        return result;
    } else if (type == EValueType::Uint64) {
        static auto result = {
            MakeUint64(0),
            MakeUint64(1),
            MakeUint64(2),
            MakeUint64(13),
            MakeUint64(std::numeric_limits<ui64>::max()),
            MakeUint64(std::numeric_limits<ui64>::max() - 1),
            MakeUint64(std::numeric_limits<ui64>::max() - 10)
            };
        return result;
    } else if (type == EValueType::Double) {
        static auto result = {MakeDouble(0), MakeDouble(1), MakeDouble(0.00000234)};
        return result;
    }

    YT_ABORT();
}


template <class T>
T CastNumericValueTo(TUnversionedValue value)
{
    if (value.Type == EValueType::Int64) {
        return value.Data.Int64;
    } else if (value.Type == EValueType::Uint64) {
        return value.Data.Uint64;
    } else if (value.Type == EValueType::Double) {
        return value.Data.Double;
    }

    YT_ABORT();
}

TUnversionedValue CastNumericValue(TUnversionedValue value, EValueType type)
{
    if (type == EValueType::Int64) {
        return MakeInt64(CastNumericValueTo<i64>(value));
    } else if (type == EValueType::Uint64) {
        return MakeUint64(CastNumericValueTo<ui64>(value));
    } else if (type == EValueType::Double) {
        return MakeDouble(CastNumericValueTo<double>(value));
    }

    YT_ABORT();
}


bool IsExceptionCase(EBinaryOp op, EValueType castedType, TUnversionedValue lhs, TUnversionedValue rhs)
{
    if (op == EBinaryOp::Concatenate) {
        return castedType != EValueType::String ||
            lhs.Type != EValueType::String ||
            rhs.Type != EValueType::String;
    }

    // Cast is not revertible.
    if (CastNumericValue(CastNumericValue(lhs, castedType), lhs.Type) != lhs) {
        return true;
    }

    if (CastNumericValue(CastNumericValue(rhs, castedType), rhs.Type) != rhs) {
        return true;
    }

    if (!IsIntegralType(castedType)) {
        return false;
    }

    if (castedType < lhs.Type || castedType < rhs.Type) {
        return true;
    }

    if (op != EBinaryOp::Divide && op != EBinaryOp::Modulo) {
        return false;
    }

    return rhs == MakeInt64(0) || rhs == MakeUint64(0) || (lhs == MakeInt64(std::numeric_limits<i64>::min()) && rhs == MakeInt64(-1));
}


EValueType GetCastedType(EValueType lhsType, EValueType rhsType, bool lhsLiteral, bool rhsLiteral)
{
    if (lhsType == rhsType) {
        return lhsType;
    }

    // Non literals must have equal types. If types are different than one of values must be literal.
    YT_VERIFY(lhsLiteral || rhsLiteral);

    if (!lhsLiteral) {
        return lhsType;
    }

    if (!rhsLiteral) {
        return rhsType;
    }

    return std::max(lhsType, rhsType);
}

template <class T>
T(*GetArithmeticOpFunction(EBinaryOp op))(T, T)
{
    if (op == EBinaryOp::Plus) {
        return [] (T a, T b) {return a + b;};
    } else if (op == EBinaryOp::Minus) {
        return [] (T a, T b) {return a - b;};
    } else if (op == EBinaryOp::Multiply) {
        return [] (T a, T b) {return a * b;};
    } else if (op == EBinaryOp::Divide) {
        return [] (T a, T b) {return a / b;};
    }
    YT_ABORT();
}

template <class T>
T(*GetIntegralOpFunction(EBinaryOp op))(T, T)
{
    if (op == EBinaryOp::Modulo) {
        return [] (T a, T b) {return a % b;};
    } else if (op == EBinaryOp::LeftShift) {
        return [] (T a, T b) {return a << b;};
    } else if (op == EBinaryOp::RightShift) {
        return [] (T a, T b) {return a >> b;};
    } else if (op == EBinaryOp::BitOr) {
        return [] (T a, T b) {return a | b;};
    } else if (op == EBinaryOp::BitAnd) {
        return [] (T a, T b) {return a & b;};
    }

    return GetArithmeticOpFunction<T>(op);
}

template <class T>
bool(*GetRelationOpFunction(EBinaryOp op))(T, T)
{
    if (op == EBinaryOp::Equal) {
        return [] (T a, T b) {return a == b;};
    } else if (op == EBinaryOp::NotEqual) {
        return [] (T a, T b) {return a != b;};
    } else if (op == EBinaryOp::Less) {
        return [] (T a, T b) {return a < b;};
    } else if (op == EBinaryOp::LessOrEqual) {
        return [] (T a, T b) {return a <= b;};
    } else if (op == EBinaryOp::Greater) {
        return [] (T a, T b) {return a > b;};
    } else if (op == EBinaryOp::GreaterOrEqual) {
        return [] (T a, T b) {return a >= b;};
    }

    YT_ABORT();
}

TUnversionedValue GetExpectedArithmeticResult(EBinaryOp op, EValueType castedType, TUnversionedValue lhs, TUnversionedValue rhs)
{
    lhs = CastValueWithCheck(lhs, castedType);
    rhs = CastValueWithCheck(rhs, castedType);

    if (castedType == EValueType::Int64) {
        return MakeInt64(GetIntegralOpFunction<i64>(op)(lhs.Data.Int64, rhs.Data.Int64));
    } else if (castedType == EValueType::Uint64) {
        return MakeUint64(GetIntegralOpFunction<ui64>(op)(lhs.Data.Uint64, rhs.Data.Uint64));
    } else if (castedType == EValueType::Double) {
        return MakeDouble(GetArithmeticOpFunction<double>(op)(lhs.Data.Double, rhs.Data.Double));
    }

    YT_ABORT();
}

TUnversionedValue GetExpectedRelationResult(EBinaryOp op, EValueType castedType, TUnversionedValue lhs, TUnversionedValue rhs)
{
    lhs = CastValueWithCheck(lhs, castedType);
    rhs = CastValueWithCheck(rhs, castedType);

    if (castedType == EValueType::Int64) {
        return MakeBoolean(GetRelationOpFunction<i64>(op)(lhs.Data.Int64, rhs.Data.Int64));
    } else if (castedType == EValueType::Uint64) {
        return MakeBoolean(GetRelationOpFunction<ui64>(op)(lhs.Data.Uint64, rhs.Data.Uint64));
    } else if (castedType == EValueType::Double) {
        return MakeBoolean(GetRelationOpFunction<double>(op)(lhs.Data.Double, rhs.Data.Double));
    }

    YT_ABORT();
}

TEST_F(TArithmeticExpressionTest, Test)
{
    EValueType types[] = {EValueType::Int64, EValueType::Uint64, EValueType::Double};

    auto emptySchema = New<TTableSchema>();

    THashMap<llvm::FoldingSetNodeID, TCGExpressionInstance> compiledExpressionsCache;
    auto getInstance = [&] (TConstExpressionPtr expr, TTableSchemaPtr schema, TCGVariables* variables) -> TCGExpressionInstance* {
        llvm::FoldingSetNodeID id;
        auto image = Profile(
            expr,
            schema,
            &id,
            variables,
            /*useCanonicalNullRelations*/ false,
            EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native);

        auto [it, inserted] = compiledExpressionsCache.emplace(id, TCGExpressionInstance{});
        if (inserted) {
            it->second = image().Instantiate();
        }
        return &it->second;
    };

    auto buffer = New<TRowBuffer>();

    for (auto op : TEnumTraits<EBinaryOp>::GetDomainValues()) {
        if (IsLogicalBinaryOp(op)) {
            continue;
        }

        for (auto lhsType : types) {
            for (auto rhsType : types) {
                for (bool lhsLiteral : {false, true}) {
                    for (bool rhsLiteral : {false, true}) {
                        if (lhsType != rhsType && !lhsLiteral && !rhsLiteral) {
                            continue;
                        }

                        auto castedType = GetCastedType(lhsType, rhsType, lhsLiteral, rhsLiteral);
                        if (IsIntegralBinaryOp(op) && !IsIntegralType(castedType)) {
                            continue;
                        }

                        for (auto lhsValue : GetValuesForType(lhsType)) {
                            for (auto rhsValue : GetValuesForType(rhsType)) {

                                if (IsExceptionCase(op, castedType, lhsValue, rhsValue)) {
                                    continue;
                                }

                                auto expectedValue = IsRelationalBinaryOp(op)
                                    ? GetExpectedRelationResult(op, castedType, lhsValue, rhsValue)
                                    : GetExpectedArithmeticResult(op, castedType, lhsValue, rhsValue);

                                if (lhsLiteral && rhsLiteral) {
                                    auto queryString = Format("%v %v %v", UnversionedValueToString(lhsValue), GetBinaryOpcodeLexeme(op), UnversionedValueToString(rhsValue));

                                    auto got = PrepareExpression(queryString, *emptySchema);
                                    auto expected = Make<TLiteralExpression>(expectedValue);

                                    EXPECT_TRUE(Equal(got, expected))
                                        << "got: " <<  ::testing::PrintToString(got) << std::endl
                                        << "expected: " <<  ::testing::PrintToString(expected) << std::endl;

                                    continue;
                                }

                                TUnversionedValue result{};
                                TCGVariables variables;
                                TTableSchemaPtr schema;
                                TConstExpressionPtr expr;
                                TUnversionedOwningRow row;

                                if (lhsLiteral) {
                                    schema = New<TTableSchema>(std::vector{
                                        TColumnSchema("b", rhsValue.Type)});
                                    expr = PrepareExpression(Format("%v %v b", UnversionedValueToString(lhsValue), GetBinaryOpcodeLexeme(op)), *schema);
                                    row = YsonToSchemafulRow(Format("b=%v", UnversionedValueToString(rhsValue)), *schema, true);
                                } else if (rhsLiteral) {
                                    schema = New<TTableSchema>(std::vector{
                                        TColumnSchema("a", lhsValue.Type)});
                                    expr = PrepareExpression(Format("a %v %v", GetBinaryOpcodeLexeme(op), UnversionedValueToString(rhsValue)), *schema);
                                    row = YsonToSchemafulRow(Format("a=%v", UnversionedValueToString(lhsValue)), *schema, true);
                                } else {
                                    schema = New<TTableSchema>(std::vector{
                                        TColumnSchema("a", lhsValue.Type),
                                        TColumnSchema("b", rhsValue.Type)});
                                    expr = PrepareExpression(Format("a %v b", GetBinaryOpcodeLexeme(op)), *schema);
                                    row = YsonToSchemafulRow(Format("a=%v;b=%v", UnversionedValueToString(lhsValue), UnversionedValueToString(rhsValue)), *schema, true);
                                }

                                auto instance = getInstance(expr, schema, &variables);

                                // TODO(dtorilov): Test both execution backends.
                                instance->Run(
                                    variables.GetLiteralValues(),
                                    variables.GetOpaqueData(),
                                    variables.GetOpaqueDataSizes(),
                                    &result,
                                    row.Elements(),
                                    buffer);

                                EXPECT_EQ(result, expectedValue)
                                    << "row: " << ::testing::PrintToString(row);
                            }
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTernaryLogicTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<EBinaryOp, TValue, TValue, TValue>>
    , public TCompareExpressionTest
{
protected:
    void SetUp() override
    { }
};

TEST_P(TTernaryLogicTest, Evaluate)
{
    auto& param = GetParam();

    auto op = std::get<0>(param);
    auto lhs = std::get<1>(param);
    auto rhs = std::get<2>(param);
    auto expected = std::get<3>(param);

    auto buffer = New<TRowBuffer>();
    auto row = YsonToSchemafulRow("", TTableSchema(), true);

    {
        auto firstExpression = New<TBinaryOpExpression>(EValueType::Boolean, op,
            New<TLiteralExpression>(EValueType::Boolean, lhs),
            New<TLiteralExpression>(EValueType::Boolean, rhs));

        Evaluate(firstExpression, New<TTableSchema>(), buffer, row, [&] (const TUnversionedValue& result) {
            EXPECT_TRUE(CompareRowValues(result, expected) == 0);
        });
    }

    {
        auto secondExpression = New<TBinaryOpExpression>(EValueType::Boolean, op,
            New<TLiteralExpression>(EValueType::Boolean, rhs),
            New<TLiteralExpression>(EValueType::Boolean, lhs));

        Evaluate(secondExpression, New<TTableSchema>(), buffer, row, [&] (const TUnversionedValue& result) {
            EXPECT_TRUE(CompareRowValues(result, expected) == 0);
        });
    }
}

INSTANTIATE_TEST_SUITE_P(
    AndOr,
    TTernaryLogicTest,
    ::testing::Values(
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::And,
            MakeBoolean(true),
            MakeBoolean(true),
            MakeBoolean(true)),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::And,
            MakeBoolean(true),
            MakeBoolean(false),
            MakeBoolean(false)),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::And,
            MakeBoolean(false),
            MakeBoolean(false),
            MakeBoolean(false)),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::And,
            MakeBoolean(false),
            MakeNull(),
            MakeBoolean(false)),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::And,
            MakeBoolean(true),
            MakeNull(),
            MakeNull()),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::And,
            MakeNull(),
            MakeNull(),
            MakeNull()),

        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::Or,
            MakeBoolean(true),
            MakeBoolean(true),
            MakeBoolean(true)),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::Or,
            MakeBoolean(true),
            MakeBoolean(false),
            MakeBoolean(true)),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::Or,
            MakeBoolean(false),
            MakeBoolean(false),
            MakeBoolean(false)),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::Or,
            MakeBoolean(false),
            MakeNull(),
            MakeNull()),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::Or,
            MakeBoolean(true),
            MakeNull(),
            MakeBoolean(true)),
        std::tuple<EBinaryOp, TValue, TValue, TValue>(
            EBinaryOp::Or,
            MakeNull(),
            MakeNull(),
            MakeNull())
));

////////////////////////////////////////////////////////////////////////////////

using TCompareWithNullTestParam = std::tuple<const char*, const char*, TUnversionedValue>;

class TCompareWithNullTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TCompareWithNullTestParam>
    , public TCompareExpressionTest
{
protected:
    void SetUp() override
    { }
};

TEST_P(TCompareWithNullTest, Simple)
{
    auto& param = GetParam();
    auto& rowString = std::get<0>(param);
    auto& exprString = std::get<1>(param);
    auto& nonCanonExpected = std::get<2>(param);
    auto canonExpected = MakeNull();

    TUnversionedValue nonCanonResult{};
    TUnversionedValue canonResult{};
    TCGVariables variables;
    auto schema = GetSampleTableSchema();

    auto row = YsonToSchemafulRow(rowString, *schema, /*treatMissingAsNull*/ true);
    auto expr = PrepareExpression(exprString, *schema);

    auto buffer = New<TRowBuffer>();

    llvm::FoldingSetNodeID nonCanonId, canonId;

    {
        auto image = Profile(
            expr,
            schema,
            &nonCanonId,
            &variables,
            /*useCanonicalNullRelations*/ false,
            EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native)();

        auto instance = image.Instantiate();

        instance.Run(
            variables.GetLiteralValues(),
            variables.GetOpaqueData(),
            variables.GetOpaqueDataSizes(),
            &nonCanonResult,
            row.Elements(),
            buffer);
    }

    {
        auto image = Profile(
            expr,
            schema,
            &canonId,
            &variables,
            /*useCanonicalNullRelations*/ true,
            EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native)();

        auto instance = image.Instantiate();

        instance.Run(
            variables.GetLiteralValues(),
            variables.GetOpaqueData(),
            variables.GetOpaqueDataSizes(),
            &canonResult,
            row.Elements(),
            buffer);
    }

    EXPECT_NE(nonCanonId, canonId);

    EXPECT_EQ(nonCanonResult, nonCanonExpected)
        << "row: " << ::testing::PrintToString(rowString) << std::endl
        << "expr: " << ::testing::PrintToString(exprString) << std::endl
        << "non-canonical null relations" << std::endl;

    EXPECT_EQ(canonResult, canonExpected)
        << "row: " << ::testing::PrintToString(rowString) << std::endl
        << "expr: " << ::testing::PrintToString(exprString) << std::endl
        << "canonical null relations" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TCompareWithNullTest,
    TCompareWithNullTest,
    ::testing::Values(
        TCompareWithNullTestParam("k=1", "l != k", MakeBoolean(true)),
        TCompareWithNullTestParam("k=1", "l <> k", MakeBoolean(true)),
        TCompareWithNullTestParam("k=1", "l = k", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "l < k", MakeBoolean(true)),
        TCompareWithNullTestParam("k=1", "l > k", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "k <= l", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "k >= l", MakeBoolean(true)),
        TCompareWithNullTestParam("k=1", "l != m", MakeBoolean(false)),
        TCompareWithNullTestParam("k=1", "l <> m", MakeBoolean(false)),
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

TEST_F(TEvaluateAggregationTest, AggregateFlag)
{
    auto aggregateProfilers = New<TAggregateProfilerMap>();

    auto builder = CreateProfilerFunctionRegistryBuilder(
        nullptr,
        aggregateProfilers.Get());

    builder->RegisterAggregate(
        "xor_aggregate",
        std::unordered_map<TTypeParameter, TUnionType>(),
        EValueType::Int64,
        EValueType::Int64,
        EValueType::Int64,
        "xor_aggregate",
        ECallingConvention::UnversionedValue);

    // TODO(dtorilov): Test both execution backends.
    auto image = CodegenAggregate(
        aggregateProfilers->GetAggregate("xor_aggregate")->Profile(
            {EValueType::Int64},
            EValueType::Int64,
            EValueType::Int64,
            "xor_aggregate",
            EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native),
        {EValueType::Int64},
        EValueType::Int64,
        EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native);
    auto instance = image.Instantiate();

    auto buffer = New<TRowBuffer>();

    auto state = MakeUint64(0);
    auto value = MakeUint64(0);

    // Init sets aggregate flag to true.
    instance.RunInit(buffer, &state);
    EXPECT_EQ(EValueFlags::Aggregate, state.Flags);

    value.Flags |= EValueFlags::Aggregate;
    instance.RunUpdate(buffer, &state, TRange<TValue>(&value, 1));
    EXPECT_EQ(EValueFlags::None, state.Flags);

    instance.RunUpdate(buffer, &state, TRange<TValue>(&value, 1));
    EXPECT_EQ(EValueFlags::Aggregate, state.Flags);

    value.Flags &= ~EValueFlags::Aggregate;
    instance.RunUpdate(buffer, &state, TRange<TValue>(&value, 1));
    EXPECT_EQ(EValueFlags::Aggregate, state.Flags);

    value.Flags &= ~EValueFlags::Aggregate;
    instance.RunMerge(buffer, &state, &value);
    EXPECT_EQ(EValueFlags::Aggregate, state.Flags);

    value.Flags |= EValueFlags::Aggregate;
    instance.RunMerge(buffer, &state, &value);
    EXPECT_EQ(EValueFlags::None, state.Flags);

    TUnversionedValue result{};
    // Finalize preserves aggregate flag.
    state.Flags &= ~EValueFlags::Aggregate;
    instance.RunFinalize(buffer, &result, &state);
    EXPECT_EQ(EValueFlags::None, state.Flags);

    state.Flags |= EValueFlags::Aggregate;
    instance.RunFinalize(buffer, &result, &state);
    EXPECT_EQ(EValueFlags::Aggregate, state.Flags);
}

TEST_F(TEvaluateAggregationTest, Aliasing)
{
    auto aggregateProfilers = New<TAggregateProfilerMap>();

    auto builder = CreateProfilerFunctionRegistryBuilder(
        nullptr,
        aggregateProfilers.Get());

    builder->RegisterAggregate(
        "concat_all",
        std::unordered_map<TTypeParameter, TUnionType>(),
        EValueType::String,
        EValueType::String,
        EValueType::String,
        "concat_all",
        ECallingConvention::UnversionedValue);

    auto image = CodegenAggregate(
        /* codegenAggregate */ aggregateProfilers->GetAggregate("concat_all")->Profile(
            /* argumentTypes */ {EValueType::String},
            /* stateType */ EValueType::String,
            /* resultType */ EValueType::String,
            /* name */ "concat_all",
            EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native),
        /* argumentTypes */ {EValueType::String},
        /* stateType */ EValueType::String,
        EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native);
    auto instance = image.Instantiate();

    {
        auto buffer = New<TRowBuffer>();

        auto result = MakeUnversionedNullValue();
        instance.RunInit(buffer, &result);

        auto input = MakeUnversionedStringValue("abc");
        instance.RunUpdate(buffer, &result, TRange<TValue>(&input, 1));
        EXPECT_EQ(result.AsStringBuf(), "abc");

        input = MakeUnversionedStringValue("def");
        instance.RunMerge(buffer, &result, &input);
        EXPECT_EQ(result.AsStringBuf(), "abcdef");

        auto result2 = MakeUnversionedNullValue();

        instance.RunFinalize(buffer, &result2, &result);
        EXPECT_EQ(result2.AsStringBuf(), "abcdef");
    }

    {
        auto buffer = New<TRowBuffer>();

        auto result = MakeUnversionedNullValue();
        instance.RunInit(buffer, &result);

        auto input = MakeUnversionedStringValue("abc");
        instance.RunUpdate(buffer, &result, TRange<TValue>(&input, 1));
        EXPECT_EQ(result.AsStringBuf(), "abc");

        input = MakeUnversionedStringValue("def");
        instance.RunUpdate(buffer, &result, TRange<TValue>(&input, 1));
        EXPECT_EQ(result.AsStringBuf(), "abcdef");

        instance.RunFinalize(buffer, &result, &result);
        EXPECT_EQ(result.AsStringBuf(), "abcdef");
    }

    {
        auto buffer = New<TRowBuffer>();

        auto result = MakeUnversionedNullValue();
        instance.RunInit(buffer, &result);

        auto input = MakeUnversionedStringValue("abc");
        instance.RunUpdate(buffer, &result, TRange<TValue>(&input, 1));
        EXPECT_EQ(result.AsStringBuf(), "abc");

        instance.RunUpdate(buffer, &result, TRange<TValue>(&result, 1));
        EXPECT_EQ(result.AsStringBuf(), "abcabc");

        instance.RunMerge(buffer, &result, &result);
        EXPECT_EQ(result.AsStringBuf(), "abcabcabcabc");

        instance.RunFinalize(buffer, &result, &result);
        EXPECT_EQ(result.AsStringBuf(), "abcabcabcabc");
    }
}

TEST_P(TEvaluateAggregationTest, Basic)
{
    const auto& param = GetParam();
    const auto& aggregateName = std::get<0>(param);
    auto type = std::get<1>(param);
    auto value1 = std::get<2>(param);
    auto value2 = std::get<3>(param);
    auto expected = std::get<4>(param);

    auto registry = GetBuiltinAggregateProfilers();
    auto aggregate = registry->GetAggregate(aggregateName);
    auto image = CodegenAggregate(
        aggregate->Profile(
            {type},
            type,
            type,
            aggregateName,
            EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native),
        {type},
        type,
        EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native);
    auto instance = image.Instantiate();

    auto buffer = New<TRowBuffer>();

    TUnversionedValue state1{};
    instance.RunInit(buffer, &state1);
    EXPECT_EQ(EValueType::Null, state1.Type);

    instance.RunUpdate(buffer, &state1, TRange<TValue>(&value1, 1));
    EXPECT_EQ(value1, state1);

    TUnversionedValue state2{};
    instance.RunInit(buffer, &state2);
    EXPECT_EQ(EValueType::Null, state2.Type);

    instance.RunUpdate(buffer, &state2, TRange<TValue>(&value2, 1));
    EXPECT_EQ(value2, state2);

    instance.RunMerge(buffer, &state1, &state2);
    EXPECT_EQ(expected, state1);

    TUnversionedValue result{};
    instance.RunFinalize(buffer, &result, &state1);
    EXPECT_EQ(expected, result);
}

INSTANTIATE_TEST_SUITE_P(
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
            MakeInt64(10)},
        TEvaluateAggregationParam{
            "max",
            EValueType::Boolean,
            MakeBoolean(true),
            MakeBoolean(false),
            MakeBoolean(true)},
        TEvaluateAggregationParam{
            "min",
            EValueType::Boolean,
            MakeBoolean(true),
            MakeBoolean(false),
            MakeBoolean(false)}
));

////////////////////////////////////////////////////////////////////////////////

using TEvaluateAggregationWithStringStateParam = std::tuple<
    const char*,
    std::vector<EValueType>,
    EValueType,
    EValueType,
    std::vector<std::vector<TUnversionedValue>>,
    std::vector<std::vector<TUnversionedValue>>,
    std::vector<TStringBuf>,
    std::vector<TStringBuf>,
    TStringBuf,
    TUnversionedValue>;

class TEvaluateAggregationWithStringStateTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TEvaluateAggregationWithStringStateParam>
{ };

TEST_P(TEvaluateAggregationWithStringStateTest, Basic)
{
    const auto& param = GetParam();
    const auto& aggregateName = std::get<0>(param);
    auto argumentTypes = std::get<1>(param);
    auto stateType = std::get<2>(param);
    auto resultType = std::get<3>(param);
    auto argumentPackList1 = std::get<4>(param);
    auto argumentPackList2 = std::get<5>(param);
    auto expectedValuesOnUpdate1 = std::get<6>(param);
    auto expectedValuesOnUpdate2 = std::get<7>(param);
    auto afterMergeValue = std::get<8>(param);
    auto expectedFinalValue = std::get<9>(param);

    ASSERT_EQ(argumentPackList1.size(), expectedValuesOnUpdate1.size());
    ASSERT_EQ(argumentPackList2.size(), expectedValuesOnUpdate2.size());

    auto registry = GetBuiltinAggregateProfilers();
    auto aggregate = registry->GetAggregate(aggregateName);
    auto image = CodegenAggregate(
        aggregate->Profile(
            argumentTypes,
            stateType,
            resultType,
            aggregateName,
            EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native),
        argumentTypes,
        stateType,
        EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native);
    auto instance = image.Instantiate();

    auto buffer = New<TRowBuffer>();

    TUnversionedValue state1{};
    instance.RunInit(buffer, &state1);
    EXPECT_EQ(EValueType::Null, state1.Type);

    for (int index = 0; index < std::ssize(argumentPackList1); ++index) {
        instance.RunUpdate(buffer, &state1, TRange<TValue>(argumentPackList1[index]));
        EXPECT_EQ(state1, MakeString(expectedValuesOnUpdate1[index]));
    }

    TUnversionedValue state2{};
    instance.RunInit(buffer, &state2);
    EXPECT_EQ(EValueType::Null, state2.Type);

    for (int index = 0; index < std::ssize(argumentPackList2); ++index) {
        instance.RunUpdate(buffer, &state2, TRange<TValue>(argumentPackList2[index]));
        EXPECT_EQ(state2, MakeString(expectedValuesOnUpdate2[index]));
    }

    instance.RunMerge(buffer, &state1, &state2);
    EXPECT_EQ(state1, MakeString(afterMergeValue));

    TUnversionedValue result{};
    instance.RunFinalize(buffer, &result, &state1);
    EXPECT_EQ(result, expectedFinalValue);
}

INSTANTIATE_TEST_SUITE_P(
    EvaluateAggregationWithStringStateTest,
    TEvaluateAggregationWithStringStateTest,
    ::testing::Values(
        TEvaluateAggregationWithStringStateParam{
            "argmax",
            {EValueType::String, EValueType::Int64},
            EValueType::String,
            EValueType::String,
            {
                {MakeString("ingrid"), MakeInt64(022)},
                {MakeString("kilian"), MakeInt64(016)},
                {MakeString("luca"), MakeInt64(031)}
            },
            {
                {MakeString("ophelia"), MakeInt64(027)},
                {MakeString("tyodor"), MakeInt64(036)},
                {MakeString("fiona"), MakeInt64(020)}
            },
            {
                TStringBuf("\6\0\0\0ingrid\22\0\0\0\0\0\0\0", 18),
                TStringBuf("\6\0\0\0ingrid\22\0\0\0\0\0\0\0", 18),
                TStringBuf("\4\0\0\0luca\31\0\0\0\0\0\0\0", 16)
            },
            {
                TStringBuf("\7\0\0\0ophelia\27\0\0\0\0\0\0\0", 19),
                TStringBuf("\6\0\0\0tyodor\36\0\0\0\0\0\0\0", 18),
                TStringBuf("\6\0\0\0tyodor\36\0\0\0\0\0\0\0", 18)
            },
            TStringBuf("\6\0\0\0tyodor\36\0\0\0\0\0\0\0", 18),
            MakeString("tyodor")},
        TEvaluateAggregationWithStringStateParam{
            "argmin",
            {EValueType::Boolean, EValueType::Int64},
            EValueType::String,
            EValueType::Boolean,
            {
                {MakeBoolean(true), MakeInt64(022)},
                {MakeBoolean(false), MakeInt64(016)},
                {MakeBoolean(true), MakeInt64(031)}
            },
            {
                {MakeBoolean(true), MakeInt64(027)},
                {MakeBoolean(false), MakeInt64(036)},
                {MakeBoolean(false), MakeInt64(020)}
            },
            {
                TStringBuf("\1\0\0\0\0\0\0\0\22\0\0\0\0\0\0\0", 16),
                TStringBuf("\0\0\0\0\0\0\0\0\16\0\0\0\0\0\0\0", 16),
                TStringBuf("\0\0\0\0\0\0\0\0\16\0\0\0\0\0\0\0", 16)
            },
            {
                TStringBuf("\1\0\0\0\0\0\0\0\27\0\0\0\0\0\0\0", 16),
                TStringBuf("\1\0\0\0\0\0\0\0\27\0\0\0\0\0\0\0", 16),
                TStringBuf("\0\0\0\0\0\0\0\0\20\0\0\0\0\0\0\0", 16)
            },
            TStringBuf("\0\0\0\0\0\0\0\0\16\0\0\0\0\0\0\0", 16),
            MakeBoolean(false)},
        TEvaluateAggregationWithStringStateParam{
            "argmin",
            {EValueType::String, EValueType::String},
            EValueType::String,
            EValueType::String,
            {
                {MakeString("aba"), MakeString("caba")},
                {MakeString("aca"), MakeString("baca")}
            },
            {
                {MakeString("cab"), MakeString("abac")},
                {MakeString("bac"), MakeString("acab")}
            },
            {
                TStringBuf("\3\0\0\0aba\4\0\0\0caba", 15),
                TStringBuf("\3\0\0\0aca\4\0\0\0baca", 15)
            },
            {
                TStringBuf("\3\0\0\0cab\4\0\0\0abac", 15),
                TStringBuf("\3\0\0\0cab\4\0\0\0abac", 15)
            },
            TStringBuf("\3\0\0\0cab\4\0\0\0abac", 15),
            MakeString("cab")},
        TEvaluateAggregationWithStringStateParam{
            "argmax",
            {EValueType::Double, EValueType::Double},
            EValueType::String,
            EValueType::Double,
            {
                {MakeDouble(4.44), MakeDouble(44.4)},
                {MakeDouble(5.55), MakeDouble(33.3)}
            },
            {
                {MakeDouble(1.11), MakeDouble(77.7)},
                {MakeDouble(2.22), MakeDouble(88.8)}
            },
            {
                TStringBuf("\303\365\50\134\217\302\21\100\63\63\63\63\63\63\106\100", 16),
                TStringBuf("\303\365\50\134\217\302\21\100\63\63\63\63\63\63\106\100", 16)
            },
            {
                TStringBuf("\303\365\50\134\217\302\361\77\315\314\314\314\314\154\123\100", 16),
                TStringBuf("\303\365\50\134\217\302\1\100\63\63\63\63\63\63\126\100", 16)
            },
            TStringBuf("\303\365\50\134\217\302\1\100\63\63\63\63\63\63\126\100", 16),
            MakeDouble(2.22)}
));

////////////////////////////////////////////////////////////////////////////////

void EvaluateExpression(
    TConstExpressionPtr expr,
    const TString& rowString,
    const TTableSchemaPtr& schema,
    TUnversionedValue* result,
    TRowBufferPtr buffer)
{
    TCGVariables variables;

    auto image = Profile(
        expr,
        schema,
        /*id*/ nullptr,
        &variables,
        /*useCanonicalNullRelations*/ false,
        EnableWebAssemblyInUnitTests() ? EExecutionBackend::WebAssembly : EExecutionBackend::Native)();
    auto instance = image.Instantiate();

    auto row = YsonToSchemafulRow(rowString, *schema, true);

    instance.Run(
        variables.GetLiteralValues(),
        variables.GetOpaqueData(),
        variables.GetOpaqueDataSizes(),
        result,
        row.Elements(),
        buffer);
}

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

    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("i1", EValueType::Int64),
        TColumnSchema("i2", EValueType::Int64),
        TColumnSchema("u1", EValueType::Uint64),
        TColumnSchema("u2", EValueType::Uint64),
        TColumnSchema("s1", EValueType::String),
        TColumnSchema("s2", EValueType::String),
        TColumnSchema("any", EValueType::Any)
    });

    auto expr = PrepareExpression(exprString, *schema);

    auto buffer = New<TRowBuffer>();
    TUnversionedValue result{};
    EvaluateExpression(expr, rowString, schema, &result, buffer);

    EXPECT_EQ(result, expected);
}

INSTANTIATE_TEST_SUITE_P(
    EvaluateExpressionTest,
    TEvaluateExpressionTest,
    ::testing::Values(
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "lower('')",
            MakeString("")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "lower('ПрИвЕт, КаК ДеЛа?')",
            MakeString("привет, как дела?")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "length('abc')",
            MakeInt64(3)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "length('abcdefg')",
            MakeInt64(7)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "concat('', '')",
            MakeString("")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "concat('abc', '')",
            MakeString("abc")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "concat('', 'def')",
            MakeString("def")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "concat('abc', 'def')",
            MakeString("abcdef")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "'' || ''",
            MakeString("")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "'abc' || 'def'",
            MakeString("abcdef")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "s1=abc",
            "s1 || 'def'",
            MakeString("abcdef")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "s1=abc;s2=def",
            "s1 || s2",
            MakeString("abcdef")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "s1=abc;s2=def",
            "s1 || ' ' || s1 || s2",
            MakeString("abc abcdef")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "# || #",
            MakeNull()),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "s1=abc",
            "s1 || #",
            MakeNull()),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=33;i2=22",
            "i1 + i2",
            MakeInt64(33 + 22)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=33",
            "-i1",
            MakeInt64(-33)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "-----1",
            MakeInt64(-1)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "~~~~42",
            MakeInt64(42)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "42 ++ --2 ---2",
            MakeInt64(42)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=0",
            "uint64(i1)",
            MakeUint64(0)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "u1=0u",
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
            MakeUint64(9223372036854775808ULL)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "u1=17271244077285990991u",
            "u1=17271244077285990991",
            MakeBoolean(true)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "3.14",
            MakeDouble(3.14)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            ".5",
            MakeDouble(0.5)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            ".50000000",
            MakeDouble(0.5)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "2.",
            MakeDouble(2)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "123e+4",
            MakeDouble(123e+4)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "123e4",
            MakeDouble(123e4)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "",
            "125e-3",
            MakeDouble(125e-3)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=%false",
            "boolean(any)",
            MakeBoolean(false)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=123u",
            "int64(any)",
            MakeInt64(123)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=123",
            "uint64(any)",
            MakeUint64(123)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=123",
            "double(any)",
            MakeDouble(123)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=\"hello\"",
            "string(any)",
            MakeString("hello")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=hello",
            "string(string(any))",
            MakeString("hello")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=0",
            "boolean(boolean(i1))",
            MakeBoolean(false)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "u1=42u",
            "boolean(boolean(u1))",
            MakeBoolean(true)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=-42",
            "boolean(boolean(i1))",
            MakeBoolean(true)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=#",
            "int64(any)",
            MakeNull()),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=#",
            "list_contains(any, \"1\")",
            MakeNull()),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=[7; 9; 3]",
            "list_contains(any, 5)",
            MakeBoolean(false)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=[7;9;3;%false]",
            "list_contains(any, %true)",
            MakeBoolean(false)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=[7; 9; 3]",
            "list_contains(any, 9)",
            MakeBoolean(true)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=[\"x\"; 2.7; 5]",
            "list_contains(any, 2.7)",
            MakeBoolean(true)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=[%false; \"x\"; 4u]",
            "list_contains(any, \"y\")",
            MakeBoolean(false)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=[%false; \"y\"; 4u]",
            "list_contains(any, \"y\")",
            MakeBoolean(true)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "any=[%false; \"y\"; 4u]",
            "list_contains(any, 4u)",
            MakeBoolean(true)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "u1=123u;u2=345u",
            "coalesce(u2, u1)",
            MakeUint64(345)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=234;i2=123",
            "coalesce(#, #, #, i1, i2)",
            MakeInt64(234)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i2=100",
            "coalesce(i1, i2)",
            MakeInt64(100))));

class TEvaluateLikeExpressionTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<const char*, bool>>
{ };

TEST_P(TEvaluateLikeExpressionTest, Basic)
{
    const auto& param = GetParam();
    const auto& exprString = std::get<0>(param);
    const auto& expected = std::get<1>(param);
    auto schema = New<TTableSchema>();
    auto expr = PrepareExpression(exprString, *schema);
    auto buffer = New<TRowBuffer>();
    TUnversionedValue result{};
    EvaluateExpression(expr, "", schema, &result, buffer);
    EXPECT_EQ(result, MakeBoolean(expected));
}

INSTANTIATE_TEST_SUITE_P(
    EvaluateLikeExpressionTest,
    TEvaluateLikeExpressionTest,
    ::testing::Values(
        std::tuple("'abc' like ''", false),
        std::tuple("'abc' like '' escape ''", false),
        std::tuple("'' like '' escape ''", true),
        std::tuple("'' like '%'", true),
        std::tuple("'ab.cd.efgh' like '__.__.____'", true),
        std::tuple("'ab.cd-efgh' like '__.__.____'", false),
        std::tuple("'a' || 'b' || 'c' like '_'", false),
        std::tuple("'abc' like '_' || '_' || 'c'", true),
        std::tuple("'$[](){}' like '$[](){}'", true),
        std::tuple("'ABC' like '(?i)abc'", false),
        std::tuple("'(?i)ABC' like '(?i)abc'", false),
        std::tuple("'(?i)ABC' like '(?i)ABC'", true),
        std::tuple("'(?i)ABC' ilike '(?i)abc'", true),
        std::tuple("'abc' like '%'", true),
        std::tuple("'abc' like '_%_'", true),
        std::tuple("'abcdef' like 'abcdef'", true),
        std::tuple("'abcdef' like 'a_c_%f'", true),
        std::tuple("'abcdef' like '%abc%def%'", true),
        std::tuple("'abcdef' not like '%abc%def%'", false),
        std::tuple("'abcdef' like '%abc%def%'", true),
        std::tuple("'abc\n\tdef' like '%abc%def%'", true),
        std::tuple("'abc\n\tdef' like '%Abc%def%'", false),
        std::tuple("'abc\n\tdef' ilike '%Abc%def%'", true),
        std::tuple("'abc\n\tdef' not ilike '%Abc%def%'", false),
        std::tuple("'Abc\n\tdef' ilike '%BC%DEF%'", true),
        std::tuple("'Abc\n\tdef' not ilike '%BC%DEF%'", false),
        std::tuple("'q' not like 'x_' escape 'x'", true),
        std::tuple("'_' like '._' escape '.'", true),
        std::tuple("'a' like '._' escape '.'", false),
        std::tuple("'_' like '[_' escape '['", true),
        std::tuple("'a' like '[_' escape '['", false),
        std::tuple(R"('_' like '\\_' escape '\\')", true),
        std::tuple(R"('a' like '\\_' escape '\\')", false),
        std::tuple(R"('\\q' like '\\_' escape '')", true),
        std::tuple(R"('\\qwe' like '\\%' escape '')", true),
        std::tuple(R"('\\abc\\' like '\\abc\\' escape 'x')", true),
        std::tuple(R"('_' like '\\_')", true),
        std::tuple(R"('_' like 'x_' escape 'x')", true),
        std::tuple(R"('\\_\\%_%' like '\\_\\%_%' escape '')", true),
        std::tuple(R"('\\' like '\\\\')", true),
        std::tuple(R"('\\\\' like '\\\\')", false),
        std::tuple(R"('\\' like 'x\\' escape 'x')", true),
        std::tuple(R"('\\abc\\' like '\\\\%\\\\')", true),
        std::tuple(R"('Abc def' rlike '\\w+')", false),
        std::tuple(R"('Abc def' rlike '\\w+ \\w+')", true),
        std::tuple(R"('Abc def' rlike '\\w+\\s+\\w+')", true),
        std::tuple(R"('Abc def' regexp '\\w+\\s+\\w+')", true),
        std::tuple(R"('Abc def' not regexp '\\w+\\s+\\w+')", false)));

INSTANTIATE_TEST_SUITE_P(
    EvaluateTimestampExpressionTest,
    TEvaluateExpressionTest,
    ::testing::Values(
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=1446325284",
            "format_timestamp(i1, '')",
            MakeString("")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=1446325284",
            "format_timestamp(i1, '%Y-%m-%dT%H:%M:%S')",
            MakeString("2015-10-31T21:01:24")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=1446325284",
            "timestamp_floor_hour(i1)",
            MakeInt64(1446325200)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=1446325284",
            "timestamp_floor_day(i1)",
            MakeInt64(1446249600)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=1446325284",
            "timestamp_floor_week(i1)",
            MakeInt64(1445817600)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=1446325284",
            "timestamp_floor_month(i1)",
            MakeInt64(1443657600)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "i1=1446325284",
            "timestamp_floor_year(i1)",
            MakeInt64(1420070400))
));

class TFormatTimestampExpressionTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    { }
};

TEST_F(TFormatTimestampExpressionTest, TooSmallTimestamp)
{
    auto schema = New<TTableSchema>();

    auto expr = PrepareExpression("format_timestamp(-62135596801, '')", *schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        EvaluateExpression(expr, "", schema, &result, buffer),
        HasSubstr("Timestamp is smaller than minimal value"));
}

TEST_F(TFormatTimestampExpressionTest, TooLargeTimestamp)
{
    auto schema = New<TTableSchema>();

    auto expr = PrepareExpression("format_timestamp(253402300800, '%Y%m%d')", *schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        EvaluateExpression(expr, "", schema, &result, buffer),
        HasSubstr("Timestamp is greater than maximal value"));
}

TEST_F(TFormatTimestampExpressionTest, InvalidFormat)
{
    auto schema = New<TTableSchema>();

    auto expr = PrepareExpression("format_timestamp(0, '11111111112222222222333333333344')", *schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        EvaluateExpression(expr, "", schema, &result, buffer),
        HasSubstr("Format string is too long"));
}

class TExpressionErrorTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    { }
};

TEST_F(TExpressionErrorTest, Int64_DivisionByZero)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("i1", EValueType::Int64),
        TColumnSchema("i2", EValueType::Int64)
    });

    auto expr = PrepareExpression("i1 / i2", *schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        EvaluateExpression(expr, "i1=1; i2=0", schema, &result, buffer),
        HasSubstr("Division by zero"));
}

TEST_F(TExpressionErrorTest, Int64_ModuloByZero)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("i1", EValueType::Int64),
        TColumnSchema("i2", EValueType::Int64)
    });

    auto expr = PrepareExpression("i1 % i2", *schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        EvaluateExpression(expr, "i1=1; i2=0", schema, &result, buffer),
        HasSubstr("Division by zero"));
}

TEST_F(TExpressionErrorTest, UInt64_DivisionByZero)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("u1", EValueType::Uint64),
        TColumnSchema("u2", EValueType::Uint64)
    });

    auto expr = PrepareExpression("u1 / u2", *schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        EvaluateExpression(expr, "u1=1u; u2=0u", schema, &result, buffer),
        HasSubstr("Division by zero"));
}

TEST_F(TExpressionErrorTest, UInt64_ModuloByZero)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("u1", EValueType::Uint64),
        TColumnSchema("u2", EValueType::Uint64)
    });

    auto expr = PrepareExpression("u1 % u2", *schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        EvaluateExpression(expr, "u1=1u; u2=0u", schema, &result, buffer),
        HasSubstr("Division by zero"));
}

TEST_F(TExpressionErrorTest, Int64_DivisionIntMinByMinusOne)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("i1", EValueType::Int64),
        TColumnSchema("i2", EValueType::Int64)
    });

    auto expr = PrepareExpression("i1 / i2", *schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        EvaluateExpression(expr, "i1=-9223372036854775808; i2=-1", schema, &result, buffer),
        HasSubstr("Division of INT_MIN by -1"));
}

TEST_F(TExpressionErrorTest, ConvertFromAny)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("any", EValueType::Any)
    });

    auto buffer = New<TRowBuffer>();
    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("string(any)", *schema);
            EvaluateExpression(expr, "any=1", schema, &result, buffer);
        }(),
        HasSubstr("Cannot convert value"));

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("int64(any)", *schema);
            EvaluateExpression(expr, "any=\"hello\"", schema, &result, buffer);
        }(),
        HasSubstr("Cannot convert value"));

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("int64(any)", *schema);
            EvaluateExpression(expr, "any=%true", schema, &result, buffer);
        }(),
        HasSubstr("Cannot convert value"));
}

TEST_F(TExpressionErrorTest, ListContainsAny)
{
    auto schema = New<TTableSchema>();
    auto buffer = New<TRowBuffer>();
    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("list_contains(to_any(\"a\"), to_any(42))", *schema);
            EvaluateExpression(expr, "", schema, &result, buffer);
        }(),
        HasSubstr("Wrong type for argument"));
}

TEST_F(TExpressionErrorTest, ConcatenateOperator)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("i1", EValueType::Int64),
        TColumnSchema("i2", EValueType::Int64),
        TColumnSchema("s1", EValueType::String),
    });

    auto buffer = New<TRowBuffer>();
    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("1 || 2", *schema);
            EvaluateExpression(expr, "", schema, &result, buffer);
        }(),
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("i1 || i2", *schema);
            EvaluateExpression(expr, "i1=1;i2=2", schema, &result, buffer);
        }(),
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("s1 || 2", *schema);
            EvaluateExpression(expr, "s1=abc", schema, &result, buffer);
        }(),
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("s1 || i2", *schema);
            EvaluateExpression(expr, "s1=abc;i2=2", schema, &result, buffer);
        }(),
        HasSubstr("Type mismatch in expression"));
}

class TExpressionStrConvTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<const char*, const char*, TUnversionedValue>>
{ };

TEST_P(TExpressionStrConvTest, Basic)
{
    const auto& param = GetParam();
    const auto& rowString = std::get<0>(param);
    const auto& exprString = std::get<1>(param);
    const auto& expected = std::get<2>(param);

    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("int64", EValueType::Int64),
        TColumnSchema("uint64", EValueType::Uint64),
        TColumnSchema("double", EValueType::Double),
        TColumnSchema("string", EValueType::String)
    });

    auto expr = PrepareExpression(exprString, *schema);

    auto buffer = New<TRowBuffer>();
    TUnversionedValue result{};
    EvaluateExpression(expr, rowString, schema, &result, buffer);

    EXPECT_EQ(result, expected);
}

INSTANTIATE_TEST_SUITE_P(
    ExpressionStrConvTest,
    TExpressionStrConvTest,
    ::testing::Values(
        std::tuple<const char*, const char*, TUnversionedValue>(
            "int64=1",
            "numeric_to_string(int64)",
            MakeString("1")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "uint64=1u",
            "numeric_to_string(uint64)",
            MakeString("1u")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "double=0.0",
            "numeric_to_string(double)",
            MakeString("0.")),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "int64=#",
            "numeric_to_string(int64)",
            MakeNull()),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=\"1\"",
            "parse_int64(string)",
            MakeInt64(1)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=\"1u\"",
            "parse_int64(string)",
            MakeInt64(1)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=\"1.0\"",
            "parse_int64(string)",
            MakeInt64(1)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=#",
            "parse_int64(string)",
            MakeNull()),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=\"1u\"",
            "parse_uint64(string)",
            MakeUint64(1)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=\"1\"",
            "parse_uint64(string)",
            MakeUint64(1)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=\"1.0\"",
            "parse_uint64(string)",
            MakeUint64(1)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=\"3.1415\"",
            "parse_double(string)",
            MakeUnversionedDoubleValue(3.1415)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=\"1\"",
            "parse_double(string)",
            MakeDouble(1)),
        std::tuple<const char*, const char*, TUnversionedValue>(
            "string=\"1u\"",
            "parse_double(string)",
            MakeDouble(1))
));

TEST_F(TExpressionStrConvTest, ErrorConvertStringToNumericTest) {
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("string", EValueType::String)
    });

    auto buffer = New<TRowBuffer>();
    TUnversionedValue result{};

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("parse_int64(string)", *schema);
            EvaluateExpression(expr, "string=\"hello\"", schema, &result, buffer);
        }(),
        HasSubstr("Cannot convert value"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
