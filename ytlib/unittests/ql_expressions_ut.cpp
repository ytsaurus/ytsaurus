#include <yt/core/test_framework/framework.h>

#include "ql_helpers.h"

#include <yt/client/query_client/query_statistics.h>

#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/config.h>
#include <yt/ytlib/query_client/folding_profiler.h>
#include <yt/ytlib/query_client/query_helpers.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/functions.h>
#include <yt/ytlib/query_client/functions_cg.h>
#include <yt/ytlib/query_client/coordinator.h>

#include <yt/ytlib/table_client/helpers.h>

// Tests:
// TCompareExpressionTest
// TEliminateLookupPredicateTest
// TEliminatePredicateTest
// TPrepareExpressionTest
// TArithmeticTest
// TCompareWithNullTest
// TEvaluateExpressionTest
// TEvaluateAggregationTest

namespace NYT::NQueryClient {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NTableClient;

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
        } else if (auto inLhs = lhs->As<TInExpression>()) {
            auto inRhs = rhs->As<TInExpression>();
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
            Y_UNREACHABLE();
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
    virtual void SetUp() override
    { }

};

TEST_P(TExtractSubexpressionPredicateTest, Simple)
{
    const auto& args = GetParam();
    const auto& schemaString = std::get<0>(args);
    const auto& subschemaString = std::get<1>(args);
    const auto& predicateString = std::get<2>(args);
    const auto& extractedString = std::get<3>(args);

    TTableSchema tableSchema;
    TTableSchema tableSubschema;
    Deserialize(tableSchema, ConvertToNode(TYsonString(schemaString)));
    Deserialize(tableSubschema, ConvertToNode(TYsonString(subschemaString)));

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

INSTANTIATE_TEST_CASE_P(
    TExtractSubexpressionPredicateTest,
    TExtractSubexpressionPredicateTest,
    ::testing::Values(
        std::make_tuple(
            "[{name=a;type=boolean;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=boolean;}]",
            "a and b and c",
            "a"),
        std::make_tuple(
            "[{name=a;type=boolean;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=boolean;}]",
            "not a and b and c",
            "not a"),
        std::make_tuple(
            "[{name=a;type=int64;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=int64;}]",
            "not is_null(a) and b and c",
            "not is_null(a)"),
        std::make_tuple(
            "[{name=a;type=int64;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=int64;}]",
            "a in (1, 2, 3) and b and c",
            "a in (1, 2, 3)"),
        std::make_tuple(
            "[{name=a;type=int64;}; {name=b;type=boolean}; {name=c;type=boolean}]",
            "[{name=a;type=int64;}]",
            "a = 1 and b and c",
            "a = 1"),
        std::make_tuple(
            "[{name=a;type=int64;}; {name=b;type=int64}; {name=c;type=boolean}]",
            "[{name=a;type=int64;}; {name=b;type=int64}]",
            "a = b and c",
            "a = b"),
        std::make_tuple(
            "[{name=a;type=boolean;}; {name=b;type=int64}; {name=c;type=boolean}]",
            "[{name=a;type=boolean;}; {name=b;type=int64}]",
            "if(a, b = 1, false) and c",
            "if(a, b = 1, false)"),
        std::make_tuple(
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
    virtual void SetUp() override
    { }

    TConstExpressionPtr Eliminate(
        std::vector<TOwningKey>& lookupKeys,
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
    Deserialize(tableSchema, ConvertToNode(TYsonString(schemaString)));
    Deserialize(keyColumns, ConvertToNode(TYsonString(keyString)));

    std::vector<TOwningKey> keys;
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

INSTANTIATE_TEST_CASE_P(
    TEliminateLookupPredicateTest,
    TEliminateLookupPredicateTest,
    ::testing::Values(
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "false",
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
            "false",
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
        const TTableSchema& tableSchema,
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
    Deserialize(tableSchema, ConvertToNode(TYsonString(schemaString)));
    Deserialize(keyColumns, ConvertToNode(TYsonString(keyString)));

    auto predicate = PrepareExpression(predicateString, tableSchema);
    auto expected = PrepareExpression(refinedString, tableSchema);

    std::vector<TKeyRange> owningRanges;
    for (size_t i = 0; i < keyStrings.size() / 2; ++i) {
        owningRanges.emplace_back(YsonToKey(keyStrings[2 * i]), YsonToKey(keyStrings[2 * i + 1]));
    }

    auto refined = Eliminate(owningRanges, predicate, tableSchema, keyColumns);

    EXPECT_TRUE(Equal(refined, expected))
        << "schema: " << schemaString << std::endl
        << "key_columns: " << keyString << std::endl
        << "range: [" << lowerString << ", " << upperString << "]" << std::endl
        << "predicate: " << predicateString << std::endl
        << "refined: " << ::testing::PrintToString(refined) << std::endl
        << "expected: " << ::testing::PrintToString(expected);
}

INSTANTIATE_TEST_CASE_P(
    TEliminatePredicateTestOld,
    TEliminatePredicateTest,
    ::testing::Values(
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "(k,l) in ((1,2),(3,4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "(k,l) in ((1,2))",
            std::vector<const char*>{"1", "2"}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k) in ((2),(4))",
            "(k) in ((2),(4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l) in ((2),(4))",
            "(l) in ((2),(4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k) in ((2),(4))",
            "(k) in ((2))",
            std::vector<const char*>{"2;1", "3;3"}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending;expression=l}; {name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "l in ((2),(4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "k in ((2))",
            std::vector<const char*>{"2;1", "3;3"}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "k in ((2))",
            std::vector<const char*>{"2;1", "3;3"}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "k in ((2),(4))",
            std::vector<const char*>{"2;1", "4;5"}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "k in ((2))",
            std::vector<const char*>{"2", "3"}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=m;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "k in ((2))",
            std::vector<const char*>{"2;2;2", "3;3;3"})
));

INSTANTIATE_TEST_CASE_P(
    TEliminatePredicateTest,
    TEliminatePredicateTest,
    ::testing::Values(
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k = 1 and l in (1,2,3)",
            "true",
            std::vector<const char*>{"1;1", "1;1;" _MAX_, "1;2", "1;2;" _MAX_, "1;3", "1;3;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "k in (1,2,3) and l = 1",
            "true",
            std::vector<const char*>{"1;1", "1;1;" _MAX_, "2;1", "2;1;" _MAX_, "3;1", "3;1;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k,l) in ((1,2),(3,4))",
            "true",
            std::vector<const char*>{"1;2", "1;2;" _MAX_, "3;4", "3;4;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(k) in ((2),(4))",
            "true",
            std::vector<const char*>{"2", "2;" _MAX_, "4", "4;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "(l) in ((2),(4))",
            "(l) in ((2),(4))",
            std::vector<const char*>{_MIN_, _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending;expression=l}; {name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "true",
            std::vector<const char*>{"2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending;expression=\"l+1\"}; {name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((2),(4))",
            "true",
            std::vector<const char*>{"3;2", "3;2;" _MAX_, "5;4", "5;4;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending;expression=l}; {name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[k;l]",
            "l in ((0),(2),(4))",
            "true",
            std::vector<const char*>{"0;0", "0;0;" _MAX_, "2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4))",
            "true",
            std::vector<const char*>{"0;0", "0;0;" _MAX_, "2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2),(4),(6))",
            "true",
            std::vector<const char*>{"0;0", "0;0;" _MAX_, "2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_, "6;6", "6;6;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in (1,2,3,4,5) or k > 10",
            "k in (1,2,3,4,5) or k > 10",
            std::vector<const char*>{"1;1", "1;1;" _MAX_, "2;2", "2;2;" _MAX_, "3;3", "3;3;" _MAX_, "4;4", "4;4;" _MAX_, "5;5", "5;5;" _MAX_, "10;" _MAX_, _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in (1,2,3,4,5) or k > 10",
            "true",
            std::vector<const char*>{"1;1", "1;1;" _MAX_, "2;2", "2;2;" _MAX_, "3;3", "3;3;" _MAX_, "4;4", "4;4;" _MAX_, "5;5", "5;5;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in (1,2,3,4,5) or k in (11,12,14,15)",
            "k in (4,5) or k in (11,12)",
            std::vector<const char*>{"4;4", "4;4;" _MAX_, "5;5", "5;5;" _MAX_, "11;11", "11;11;" _MAX_, "12;12", "12;12;" _MAX_}),
        std::make_tuple(
            "[{name=k;type=int64;sort_order=ascending}; {name=l;type=int64;sort_order=ascending;expression=k}; {name=a;type=int64}]",
            "[k;l]",
            "k in ((0),(2)) or k in ((4),(6))",
            "k in ((0),(2)) or k in ((4),(6))",
            std::vector<const char*>{"0;0", "0;0;" _MAX_, "2;2", "2;2;" _MAX_, "4;4", "4;4;" _MAX_, "6;6", "6;6;" _MAX_})
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
    auto expr2 = PrepareExpression(TString("k"), schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    expr1 = Make<TLiteralExpression>(MakeInt64(90));
    expr2 = PrepareExpression(TString("90"), schema);

    EXPECT_TRUE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    expr1 = Make<TReferenceExpression>("a"),
    expr2 = PrepareExpression(TString("k"), schema);

    EXPECT_FALSE(Equal(expr1, expr2))
        << "expr1: " << ::testing::PrintToString(expr1) << std::endl
        << "expr2: " << ::testing::PrintToString(expr2);

    auto str1 = TString("k + 3 - a > 4 * l and (k <= m or k + 1 < 3* l)");
    auto str2 = TString("k + 3 - a > 4 * l and (k <= m or k + 2 < 3* l)");

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

TEST_F(TPrepareExpressionTest, CompareTuple)
{
    TTableSchema schema({
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

    auto expr = PrepareExpression("(a, b, c, d, e, f, g, h, i, j, k, l, m, n) < (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)", schema);

    TCGVariables variables;
    Profile(expr, schema, nullptr, &variables)();
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

    for (int id = 0; id < keyParts.size(); ++id) {
        keyBuilder.Reset();

        const auto& keyPart = keyParts[id];
        switch (keyPart->GetType()) {
            case ENodeType::Int64:
                keyBuilder.AddValue(MakeInt64Value<TUnversionedValue>(
                    keyPart->GetValue<i64>(),
                    id));
                break;
            case ENodeType::Uint64:
                keyBuilder.AddValue(MakeUint64Value<TUnversionedValue>(
                    keyPart->GetValue<ui64>(),
                    id));
                break;
            case ENodeType::Double:
                keyBuilder.AddValue(MakeDoubleValue<TUnversionedValue>(
                    keyPart->GetValue<double>(),
                    id));
                break;
            case ENodeType::String:
                keyBuilder.AddValue(MakeStringValue<TUnversionedValue>(
                    keyPart->GetValue<TString>(),
                    id));
                break;
            case ENodeType::Entity:
                keyBuilder.AddValue(MakeSentinelValue<TUnversionedValue>(
                    keyPart->Attributes().Get<EValueType>("type"),
                    id));
                break;
            default:
                keyBuilder.AddValue(MakeAnyValue<TUnversionedValue>(
                    ConvertToYsonString(keyPart).GetData(),
                    id));
                break;
        }

        rows.push_back(buffer->Capture(keyBuilder.GetRow()));
    }

    return MakeSharedRange(std::move(rows), buffer);
}

TEST_F(TPrepareExpressionTest, Negative1)
{
    auto schema = GetSampleTableSchema();

    EXPECT_THROW_THAT(
        [&] { PrepareExpression(TString("ki in (1, 2u, \"abc\")"), schema); },
        HasSubstr("Types mismatch in tuple"));

    EXPECT_THROW_THAT(
        [&] { PrepareExpression(TString("ku = \"abc\""), schema); },
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        [&] { PrepareExpression(TString("kd = 4611686018427387903"), schema); },
        HasSubstr("to double: inaccurate conversion"));

    EXPECT_THROW_THAT(
        [&] { PrepareExpression(TString("kd = 9223372036854775807u"), schema); },
        HasSubstr("to double: inaccurate conversion"));

    EXPECT_THROW_THAT(
        [&] { PrepareExpression(TString("ki = 18446744073709551606u"), schema); },
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        [&] { PrepareExpression(TString("ku = 1.5"), schema); },
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        [&] { PrepareExpression(TString("ki = 1.5"), schema); },
        HasSubstr("Type mismatch in expression"));

    EXPECT_THROW_THAT(
        [&] { PrepareExpression(TString("(1u - 2) / 3.0"), schema); },
        HasSubstr("to double: inaccurate conversion"));

    EXPECT_THROW_THAT(
        [&] { PrepareExpression(TString("k = 1 and ku"), schema); },
        HasSubstr("Type mismatch in expression"));
}

INSTANTIATE_TEST_CASE_P(
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

INSTANTIATE_TEST_CASE_P(
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
    virtual void SetUp() override
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

    auto got = PrepareExpression(TString(lhs) + " " + op + " " + rhs, schema);

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
        auto expr = PrepareExpression("int64(null)", schema);

        EXPECT_EQ(expr->Type, EValueType::Int64);

        TUnversionedValue result;
        TCGVariables variables;

        auto callback = Profile(expr, schema, nullptr, &variables)();

        callback(variables.GetLiteralValues(), variables.GetOpaqueData(), &result, row.Begin(), buffer.Get());

        EXPECT_EQ(result, MakeNull());
    }

    EXPECT_THROW_THAT(
        [&] { PrepareExpression("if(null, null, null)", schema); },
        HasSubstr("Type inference failed"));

    {
        auto expr = PrepareExpression("if(null, 1, 2)", schema);
        EXPECT_EQ(expr->Type, EValueType::Int64);

        TUnversionedValue result;
        TCGVariables variables;

        auto callback = Profile(expr, schema, nullptr, &variables)();
        callback(variables.GetLiteralValues(), variables.GetOpaqueData(), &result, row.Begin(), buffer.Get());

        EXPECT_EQ(result, MakeNull());
    }

    {
        auto expr = PrepareExpression("if(false, 1, null)", schema);
        EXPECT_EQ(expr->Type, EValueType::Int64);

        TUnversionedValue result;
        TCGVariables variables;

        auto callback = Profile(expr, schema, nullptr, &variables)();
        callback(variables.GetLiteralValues(), variables.GetOpaqueData(), &result, row.Begin(), buffer.Get());

        EXPECT_EQ(result, MakeNull());
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

    TUnversionedValue result;
    TCGVariables variables;

    auto columns = GetSampleTableSchema().Columns();
    columns[0].SetLogicalType(GetLogicalType(type));
    columns[1].SetLogicalType(GetLogicalType(type));
    auto schema = TTableSchema(columns);

    auto expr = PrepareExpression(TString("k") + " " + op + " " + "l", schema);

    auto callback = Profile(expr, schema, nullptr, &variables)();

    auto row = YsonToSchemafulRow(TString("k=") + lhs + ";l=" + rhs, schema, true);

    auto buffer = New<TRowBuffer>();

    callback(variables.GetLiteralValues(), variables.GetOpaqueData(), &result, row.Begin(), buffer.Get());

    EXPECT_EQ(result, expected)
        << "row: " << ::testing::PrintToString(row);
}

TEST_P(TExpressionTest, EvaluateLhsValueRhsLiteral)
{
    auto& param = GetParam();
    auto type = std::get<0>(param);
    auto lhs = std::get<1>(param);
    auto& op = std::get<2>(param);
    auto rhs = std::get<3>(param);
    auto& expected = std::get<4>(param);

    TUnversionedValue result;
    TCGVariables variables;

    auto columns = GetSampleTableSchema().Columns();
    columns[0].SetLogicalType(GetLogicalType(type));
    columns[1].SetLogicalType(GetLogicalType(type));
    auto schema = TTableSchema(columns);

    auto expr = PrepareExpression(TString("k") + " " + op + " " + rhs, schema);

    auto callback = Profile(expr, schema, nullptr, &variables)();

    auto row = YsonToSchemafulRow(TString("k=") + lhs, schema, true);

    auto buffer = New<TRowBuffer>();

    callback(variables.GetLiteralValues(), variables.GetOpaqueData(), &result, row.Begin(), buffer.Get());

    EXPECT_EQ(result, expected)
        << "row: " << ::testing::PrintToString(row);
}

TEST_P(TExpressionTest, EvaluateLhsLiteralRhsValue)
{
    auto& param = GetParam();
    auto type = std::get<0>(param);
    auto lhs = std::get<1>(param);
    auto& op = std::get<2>(param);
    auto rhs = std::get<3>(param);
    auto& expected = std::get<4>(param);

    TUnversionedValue result;
    TCGVariables variables;

    auto columns = GetSampleTableSchema().Columns();
    columns[0].SetLogicalType(GetLogicalType(type));
    columns[1].SetLogicalType(GetLogicalType(type));
    auto schema = TTableSchema(columns);

    auto expr = PrepareExpression(TString(lhs) + " " + op + " " + "l", schema);

    auto callback = Profile(expr, schema, nullptr, &variables)();

    auto row = YsonToSchemafulRow(TString("l=") + rhs, schema, true);

    auto buffer = New<TRowBuffer>();

    callback(variables.GetLiteralValues(), variables.GetOpaqueData(), &result, row.Begin(), buffer.Get());

    EXPECT_EQ(result, expected)
        << "row: " << ::testing::PrintToString(row);
}

INSTANTIATE_TEST_CASE_P(
    TArithmeticTest,
    TExpressionTest,
    ::testing::Values(
        TArithmeticTestParam(EValueType::Int64, "1", "+", "2", MakeInt64(3)),
        TArithmeticTestParam(EValueType::Int64, "1", "-", "2", MakeInt64(-1)),
        TArithmeticTestParam(EValueType::Int64, "3", "*", "2", MakeInt64(6)),
        TArithmeticTestParam(EValueType::Int64, "6", "/", "2", MakeInt64(3)),
        TArithmeticTestParam(EValueType::Int64, "6", "%", "4", MakeInt64(2)),
        TArithmeticTestParam(EValueType::Int64, "6", "<<", "2", MakeInt64(24)),
        TArithmeticTestParam(EValueType::Int64, "6", ">>", "1", MakeInt64(3)),
        TArithmeticTestParam(EValueType::Int64, "1234567", "|", "1111111", MakeInt64(1242823)),
        TArithmeticTestParam(EValueType::Int64, "1234567", "&", "1111111", MakeInt64(1102855)),
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
        TArithmeticTestParam(EValueType::Uint64, "1234567u", "|", "1111111u", MakeUint64(1242823)),
        TArithmeticTestParam(EValueType::Uint64, "1234567u", "&", "1111111u", MakeUint64(1102855)),
        TArithmeticTestParam(EValueType::Uint64, "6u", ">", "4u", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Uint64, "6u", "<", "4u", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Uint64, "6u", ">=", "4u", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Uint64, "6u", "<=", "4u", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Uint64, "6u", ">=", "6u", MakeBoolean(true)),
        TArithmeticTestParam(EValueType::Uint64, "6u", "<=", "6u", MakeBoolean(true))
));

INSTANTIATE_TEST_CASE_P(
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
        TArithmeticTestParam(EValueType::Int64, "1", "=", "#", MakeBoolean(false)),
        TArithmeticTestParam(EValueType::Int64, "1", "!=", "#", MakeBoolean(true)),

        TArithmeticTestParam(EValueType::Int64, "1", "+", "#", MakeNull())
));
////////////////////////////////////////////////////////////////////////////////

class TTernaryLogicTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<EBinaryOp, TValue, TValue, TValue>>
    , public TCompareExpressionTest
{
protected:
    virtual void SetUp() override
    { }
};

TEST_P(TTernaryLogicTest, Evaluate)
{
    auto& param = GetParam();

    auto op = std::get<0>(param);
    auto lhs = std::get<1>(param);
    auto rhs = std::get<2>(param);
    auto expected = std::get<3>(param);

    TUnversionedValue result;
    TCGVariables variables;
    auto buffer = New<TRowBuffer>();
    auto row = YsonToSchemafulRow("", TTableSchema(), true);

    auto expr1 = New<TBinaryOpExpression>(EValueType::Boolean, op,
        New<TLiteralExpression>(EValueType::Boolean, lhs),
        New<TLiteralExpression>(EValueType::Boolean, rhs));

    auto expr2 = New<TBinaryOpExpression>(EValueType::Boolean, op,
        New<TLiteralExpression>(EValueType::Boolean, rhs),
        New<TLiteralExpression>(EValueType::Boolean, lhs));

    TCGVariables variables1;
    auto compiledExpr1 = Profile(expr1, TTableSchema(), nullptr, &variables1)();
    compiledExpr1(variables1.GetLiteralValues(), variables1.GetOpaqueData(), &result, row.Begin(), buffer.Get());
    EXPECT_TRUE(CompareRowValues(result, expected) == 0);

    TCGVariables variables2;
    auto compiledExpr2 = Profile(expr2, TTableSchema(), nullptr, &variables2)();
    compiledExpr2(variables2.GetLiteralValues(), variables2.GetOpaqueData(), &result, row.Begin(), buffer.Get());
    EXPECT_TRUE(CompareRowValues(result, expected) == 0);
}

INSTANTIATE_TEST_CASE_P(
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
    auto schema = GetSampleTableSchema();

    auto row = YsonToSchemafulRow(rowString, schema, true);
    auto expr = PrepareExpression(exprString, schema);
    auto callback = Profile(expr, schema, nullptr, &variables)();

    auto buffer = New<TRowBuffer>();

    callback(variables.GetLiteralValues(), variables.GetOpaqueData(), &result, row.Begin(), buffer.Get());

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

    auto registry = BuiltinAggregateProfilers;
    auto aggregate = registry->GetAggregate(aggregateName);
    auto callbacks = CodegenAggregate(
        aggregate->Profile(type, type, type, aggregateName),
        type, type);

    auto buffer = New<TRowBuffer>();

    TUnversionedValue state1;
    callbacks.Init(buffer.Get(), &state1);
    EXPECT_EQ(EValueType::Null, state1.Type);

    callbacks.Update(buffer.Get(), &state1, &value1);
    EXPECT_EQ(value1, state1);

    TUnversionedValue state2;
    callbacks.Init(buffer.Get(), &state2);
    EXPECT_EQ(EValueType::Null, state2.Type);

    callbacks.Update(buffer.Get(), &state2, &value2);
    EXPECT_EQ(value2, state2);

    callbacks.Merge(buffer.Get(), &state1, &state2);
    EXPECT_EQ(expected, state1);

    TUnversionedValue result;
    callbacks.Finalize(buffer.Get(), &result, &state1);
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

void EvaluateExpression(
    TConstExpressionPtr expr,
    const TString& rowString,
    const TTableSchema& schema,
    TUnversionedValue* result,
    TRowBufferPtr buffer)
{
    TCGVariables variables;

    auto callback = Profile(expr, schema, nullptr, &variables)();

    auto row = YsonToSchemafulRow(rowString, schema, true);

    callback(variables.GetLiteralValues(), variables.GetOpaqueData(), result, row.Begin(), buffer.Get());
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

    TTableSchema schema({
        TColumnSchema("i1", EValueType::Int64),
        TColumnSchema("i2", EValueType::Int64),
        TColumnSchema("u1", EValueType::Uint64),
        TColumnSchema("u2", EValueType::Uint64),
        TColumnSchema("any", EValueType::Any)
    });

    auto expr = PrepareExpression(exprString, schema);

    auto buffer = New<TRowBuffer>();
    TUnversionedValue result;
    EvaluateExpression(expr, rowString, schema, &result, buffer);

    EXPECT_EQ(result, expected);
}

INSTANTIATE_TEST_CASE_P(
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
        ///
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
            MakeString("hello"))
));

INSTANTIATE_TEST_CASE_P(
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
    virtual void SetUp() override
    { }
};

TEST_F(TFormatTimestampExpressionTest, TooSmallTimestamp)
{
    TTableSchema schema;
    TKeyColumns keyColumns;

    auto expr = PrepareExpression("format_timestamp(-62135596801, '')", schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result;

    EXPECT_THROW_THAT(
        [&] { EvaluateExpression(expr, "", schema, &result, buffer); },
        HasSubstr("Timestamp is smaller than minimal value"));
}

TEST_F(TFormatTimestampExpressionTest, TooLargeTimestamp)
{
    TTableSchema schema;

    auto expr = PrepareExpression("format_timestamp(253402300800, '%Y%m%d')", schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result;

    EXPECT_THROW_THAT(
        [&] { EvaluateExpression(expr, "", schema, &result, buffer); },
        HasSubstr("Timestamp is greater than maximal value"));
}

TEST_F(TFormatTimestampExpressionTest, InvalidFormat)
{
    TTableSchema schema;

    auto expr = PrepareExpression("format_timestamp(0, '11111111112222222222333333333344')", schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result;

    EXPECT_THROW_THAT(
        [&] { EvaluateExpression(expr, "", schema, &result, buffer); },
        HasSubstr("Format string is too long"));
}

class TExpressionErrorTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    { }
};

TEST_F(TExpressionErrorTest, Int64_DivisionByZero)
{
    TTableSchema schema({
        TColumnSchema("i1", EValueType::Int64),
        TColumnSchema("i2", EValueType::Int64)
    });

    auto expr = PrepareExpression("i1 / i2", schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result;

    EXPECT_THROW_THAT(
        [&] { EvaluateExpression(expr, "i1=1; i2=0", schema, &result, buffer); },
        HasSubstr("Division by zero"));
}

TEST_F(TExpressionErrorTest, Int64_ModuloByZero)
{
    TTableSchema schema({
        TColumnSchema("i1", EValueType::Int64),
        TColumnSchema("i2", EValueType::Int64)
    });

    auto expr = PrepareExpression("i1 % i2", schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result;

    EXPECT_THROW_THAT(
        [&] { EvaluateExpression(expr, "i1=1; i2=0", schema, &result, buffer); },
        HasSubstr("Division by zero"));
}

TEST_F(TExpressionErrorTest, UInt64_DivisionByZero)
{
    TTableSchema schema({
        TColumnSchema("u1", EValueType::Uint64),
        TColumnSchema("u2", EValueType::Uint64)
    });

    auto expr = PrepareExpression("u1 / u2", schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result;

    EXPECT_THROW_THAT(
        [&] { EvaluateExpression(expr, "u1=1u; u2=0u", schema, &result, buffer); },
        HasSubstr("Division by zero"));
}

TEST_F(TExpressionErrorTest, UInt64_ModuloByZero)
{
    TTableSchema schema({
        TColumnSchema("u1", EValueType::Uint64),
        TColumnSchema("u2", EValueType::Uint64)
    });

    auto expr = PrepareExpression("u1 % u2", schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result;

    EXPECT_THROW_THAT(
        [&] { EvaluateExpression(expr, "u1=1u; u2=0u", schema, &result, buffer); },
        HasSubstr("Division by zero"));
}

TEST_F(TExpressionErrorTest, Int64_DivisionIntMinByMinusOne)
{
    TTableSchema schema({
        TColumnSchema("i1", EValueType::Int64),
        TColumnSchema("i2", EValueType::Int64)
    });

    auto expr = PrepareExpression("i1 / i2", schema);
    auto buffer = New<TRowBuffer>();

    TUnversionedValue result;

    EXPECT_THROW_THAT(
        [&] { EvaluateExpression(expr, "i1=-9223372036854775808; i2=-1", schema, &result, buffer); },
        HasSubstr("Division INT_MIN by -1"));
}

TEST_F(TExpressionErrorTest, ConvertFromAny)
{
    TTableSchema schema({
        TColumnSchema("any", EValueType::Any)
    });

    auto buffer = New<TRowBuffer>();
    TUnversionedValue result;

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("string(any)", schema);
            EvaluateExpression(expr, "any=1", schema, &result, buffer);
        },
        HasSubstr("Can not convert value"));

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("int64(any)", schema);
            EvaluateExpression(expr, "any=\"hello\"", schema, &result, buffer);
        },
        HasSubstr("Can not convert value"));

    EXPECT_THROW_THAT(
        [&] {
            auto expr = PrepareExpression("int64(any)", schema);
            EvaluateExpression(expr, "any=%true", schema, &result, buffer);
        },
        HasSubstr("Can not convert value"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
