#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/orm/client/misc/error.h>

#include <yt/yt/orm/library/query/query_rewriter.h>
#include <yt/yt/orm/library/query/filter_introspection.h>
#include <yt/yt/orm/library/query/query_optimizer.h>

#include <yt/yt/orm/library/query/heavy/expression_evaluator.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/library/query/base/constraints.h>

#include <util/string/subst.h>

#include <map>

using namespace NYT::NQueryClient::NAst;

namespace NYT::NOrm::NQuery::NTests {

using NTableClient::EValueType;

////////////////////////////////////////////////////////////////////////////////

std::string RewriteBitNotTimeQuery(std::string query)
{
    auto parsed = ParseSource(query, NQueryClient::EParseMode::Expression);
    auto* expression = std::get<TExpressionPtr>(parsed->AstHead.Ast);
    TBitNotQueryRewriter rewriter(&parsed->AstHead, "time", TReference("inverted_time"));
    auto result = rewriter.Visit(expression);
    return FormatExpression(*result);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBitNotQueryRewriterTest, Inequality)
{
    EXPECT_EQ(RewriteBitNotTimeQuery("time > 5"), "(inverted_time)<(18446744073709551610u)");
    EXPECT_EQ(RewriteBitNotTimeQuery("5 < time"), "(inverted_time)<(18446744073709551610u)");
    EXPECT_EQ(RewriteBitNotTimeQuery("time >= 5"), "(inverted_time)<=(18446744073709551610u)");
    EXPECT_EQ(RewriteBitNotTimeQuery("5 <= time"), "(inverted_time)<=(18446744073709551610u)");
}

TEST(TBitNotQueryRewriterTest, Equality)
{
    EXPECT_EQ(RewriteBitNotTimeQuery("time = 10"), "(inverted_time)=(18446744073709551605u)");
    EXPECT_EQ(RewriteBitNotTimeQuery("10 = time"), "(inverted_time)=(18446744073709551605u)");
    EXPECT_EQ(RewriteBitNotTimeQuery("time != 10"), "(inverted_time)!=(18446744073709551605u)");
    EXPECT_EQ(RewriteBitNotTimeQuery("10 != time"), "(inverted_time)!=(18446744073709551605u)");
}

TEST(TBitNotQueryRewriterTest, Ranges)
{
    EXPECT_EQ(
        RewriteBitNotTimeQuery("(5 < time) AND (time < 10)"),
        "((inverted_time)<(18446744073709551610u))AND((inverted_time)>(18446744073709551605u))");
    EXPECT_EQ(
        RewriteBitNotTimeQuery("(5 <= time) AND (time < 10)"),
        "((inverted_time)<=(18446744073709551610u))AND((inverted_time)>(18446744073709551605u))");
    EXPECT_EQ(
        RewriteBitNotTimeQuery("(5 < time) AND (time <= 10)"),
        "((inverted_time)<(18446744073709551610u))AND((inverted_time)>=(18446744073709551605u))");
    EXPECT_EQ(
        RewriteBitNotTimeQuery("(5 <= time) AND (time <= 10)"),
        "((inverted_time)<=(18446744073709551610u))AND((inverted_time)>=(18446744073709551605u))");
}

TEST(TBitNotQueryRewriterTest, BestEffort)
{
    EXPECT_EQ(
        RewriteBitNotTimeQuery("greatest(time, 5) = 10"),
        "(greatest(~(inverted_time), 5))=(10)");
}

////////////////////////////////////////////////////////////////////////////////

std::string RewriteDefaultFilter(
    const std::string& filter,
    const std::function<TExpressionPtr(TObjectsHolder*)>& buildDefault)
{
    auto parsed = NQueryClient::ParseSource(filter, NQueryClient::EParseMode::Expression);
    auto* expression = std::get<TExpressionPtr>(parsed->AstHead.Ast);
    TQueryRewriter rewriter(
        &parsed->AstHead,
        DummyReferenceMapping,
        DummyFunctionRewriter,
        [&] (TExpressionPtr expr) {
            return RewriteNullAsDefaultPredicate(&parsed->AstHead, expr, [&] (const TReference& reference) {
                return reference.ColumnName == "x" ? buildDefault(&parsed->AstHead) : nullptr;
            });
        });
    return FormatExpression(*rewriter.Run(expression));
}

std::string RewriteDefaultFilter(const std::string& filter, EValueType type)
{
    return RewriteDefaultFilter(filter, [type] (TObjectsHolder* holder) {
        static const std::map<EValueType, TLiteralValue> DefaultValues{
            {EValueType::Int64, i64{0}},
            {EValueType::Uint64, ui64{0}},
            {EValueType::Double, 0.0},
            {EValueType::Boolean, false},
            {EValueType::String, std::string()},
        };
        auto valueIt = DefaultValues.find(type);
        return valueIt == DefaultValues.end()
            ? nullptr
            : holder->New<TLiteralExpression>(NQueryClient::TSourceLocation(), valueIt->second);
    });
}

struct TNullAsDefaultRewriteCase
{
    std::string Filter;
    EValueType Type;
    std::string ExpectedFilter;
};

class TNullAsDefaultRewriteTest
    : public ::testing::TestWithParam<TNullAsDefaultRewriteCase>
{ };

TEST_P(TNullAsDefaultRewriteTest, RewrittenFilter)
{
    const auto& testCase = GetParam();
    SCOPED_TRACE(testCase.Filter);
    auto expected = NQueryClient::ParseSource(testCase.ExpectedFilter, NQueryClient::EParseMode::Expression);
    EXPECT_EQ(
        RewriteDefaultFilter(testCase.Filter, testCase.Type),
        FormatExpression(*std::get<TExpressionPtr>(expected->AstHead.Ast)));
}

INSTANTIATE_TEST_SUITE_P(
    Expressions,
    TNullAsDefaultRewriteTest,
    ::testing::Values(
        TNullAsDefaultRewriteCase{
            "y = 0", EValueType::Int64, "y = 0"},
        TNullAsDefaultRewriteCase{
            "x = 0", EValueType::Int64, "x IN (null, 0)"},
        TNullAsDefaultRewriteCase{
            "0 = x", EValueType::Int64, "x IN (null, 0)"},
        TNullAsDefaultRewriteCase{
            "x = 0u", EValueType::Uint64, "x IN (null, 0u)"},
        TNullAsDefaultRewriteCase{
            "x = 0.0", EValueType::Double, "x IN (null, 0.0)"},
        TNullAsDefaultRewriteCase{
            "x = %false", EValueType::Boolean, "x IN (null, %false)"},
        TNullAsDefaultRewriteCase{
            "x = \"\"", EValueType::String, "x IN (null, \"\")"},
        TNullAsDefaultRewriteCase{
            "x = 7", EValueType::Int64, "x = 7"},
        TNullAsDefaultRewriteCase{
            "x < -1", EValueType::Int64, "NOT (x = null) AND x < -1"},
        TNullAsDefaultRewriteCase{
            "x < 7", EValueType::Int64, "x < 7"},
        TNullAsDefaultRewriteCase{
            "x <= 7", EValueType::Int64, "x <= 7"},
        TNullAsDefaultRewriteCase{
            "x > 7", EValueType::Int64, "x > 7"},
        TNullAsDefaultRewriteCase{
            "x >= 7", EValueType::Int64, "x >= 7"},
        TNullAsDefaultRewriteCase{
            "x != 7", EValueType::Int64, "x != 7"},
        TNullAsDefaultRewriteCase{
            "x < 0", EValueType::Int64, "NOT (x = null) AND x < 0"},
        TNullAsDefaultRewriteCase{
            "x <= 0", EValueType::Int64, "x <= 0"},
        TNullAsDefaultRewriteCase{
            "x > 0", EValueType::Int64, "x > 0"},
        TNullAsDefaultRewriteCase{
            "x >= 0", EValueType::Int64, "x = null OR x >= 0"},
        TNullAsDefaultRewriteCase{
            "x != 0", EValueType::Int64, "NOT (x = null) AND x != 0"},
        TNullAsDefaultRewriteCase{
            "x > -1", EValueType::Int64, "x = null OR x > -1"},
        TNullAsDefaultRewriteCase{
            "x <= -1", EValueType::Int64, "NOT (x = null) AND x <= -1"},
        TNullAsDefaultRewriteCase{
            "7 > x", EValueType::Int64, "7 > x"},
        TNullAsDefaultRewriteCase{
            "0 > x", EValueType::Int64, "NOT (x = null) AND 0 > x"},
        TNullAsDefaultRewriteCase{
            "0 <= x", EValueType::Int64, "x = null OR 0 <= x"},
        TNullAsDefaultRewriteCase{
            "0 != x", EValueType::Int64, "NOT (x = null) AND 0 != x"},
        TNullAsDefaultRewriteCase{
            "x IN (0, 7)", EValueType::Int64, "x IN (null, 0, 7)"},
        TNullAsDefaultRewriteCase{
            "x IN (null, 0, 7)", EValueType::Int64, "x IN (null, 0, 7)"},
        TNullAsDefaultRewriteCase{
            "x IN (1, 2)", EValueType::Int64, "x IN (1, 2)"},
        TNullAsDefaultRewriteCase{
            "x IN (null, 1, 2)", EValueType::Int64, "x IN (1, 2)"},
        TNullAsDefaultRewriteCase{
            "x IN (null, 1, null, 2)", EValueType::Int64, "x IN (1, 2)"},
        TNullAsDefaultRewriteCase{
            "x IN (null)", EValueType::Int64, "%false"},
        TNullAsDefaultRewriteCase{
            "x IN (null, null)", EValueType::Int64, "%false"},
        TNullAsDefaultRewriteCase{
            "is_null(x)", EValueType::Int64, "%false"},
        TNullAsDefaultRewriteCase{
            "is_null(x)", EValueType::Uint64, "%false"},
        TNullAsDefaultRewriteCase{
            "is_null(x)", EValueType::Double, "%false"},
        TNullAsDefaultRewriteCase{
            "is_null(x)", EValueType::Boolean, "%false"},
        TNullAsDefaultRewriteCase{
            "is_null(x)", EValueType::String, "%false"},
        TNullAsDefaultRewriteCase{
            "is_null(x)", EValueType::Any, "is_null(x)"},
        TNullAsDefaultRewriteCase{
            "is_null(x)", EValueType::Composite, "is_null(x)"},
        TNullAsDefaultRewriteCase{
            "is_null(x)", EValueType::Null, "is_null(x)"},
        TNullAsDefaultRewriteCase{
            "is_null(y)", EValueType::Int64, "is_null(y)"},
        TNullAsDefaultRewriteCase{
            "is_null(if(%true, null, x))", EValueType::Int64, "is_null(if(%true, null, if_null(x, 0)))"},
        TNullAsDefaultRewriteCase{
            "is_null(x + 1)", EValueType::Int64, "is_null(if_null(x, 0) + 1)"},
        TNullAsDefaultRewriteCase{
            "x = null", EValueType::Int64, "if_null(x, 0) = null"},
        TNullAsDefaultRewriteCase{
            "null = x", EValueType::Int64, "null = if_null(x, 0)"},
        TNullAsDefaultRewriteCase{
            "x + 1 = 1", EValueType::Int64, "if_null(x, 0) + 1 = 1"},
        TNullAsDefaultRewriteCase{
            "x + 1u = 1u", EValueType::Uint64, "if_null(x, 0u) + 1u = 1u"},
        TNullAsDefaultRewriteCase{
            "x + 1.0 = 1.0", EValueType::Double, "if_null(x, 0.0) + 1.0 = 1.0"},
        TNullAsDefaultRewriteCase{
            "length(x) = 0", EValueType::String, "length(if_null(x, \"\")) = 0"},
        TNullAsDefaultRewriteCase{
            "NOT x", EValueType::Boolean, "NOT if_null(x, %false)"},
        TNullAsDefaultRewriteCase{
            "x = y", EValueType::Int64, "if_null(x, 0) = y"},
        TNullAsDefaultRewriteCase{
            "x + x = 0", EValueType::Int64, "if_null(x, 0) + if_null(x, 0) = 0"},
        TNullAsDefaultRewriteCase{
            "(x, x) IN ((0, 0))", EValueType::Int64, "(if_null(x, 0), if_null(x, 0)) IN ((0, 0))"},
        TNullAsDefaultRewriteCase{
            "x BETWEEN 0 AND 7", EValueType::Int64,
            "(x = null OR x >= 0) AND x <= 7"},
        TNullAsDefaultRewriteCase{
            "if(%true, x, 7) = 0", EValueType::Int64, "if(%true, if_null(x, 0), 7) = 0"},
        TNullAsDefaultRewriteCase{
            "x = 7 AND x + 1 > 0", EValueType::Int64, "x = 7 AND if_null(x, 0) + 1 > 0"}));

struct TCollectionTypeCase
{
    std::string Constructor;
    std::string EmptyValue;
    std::string NonEmptyValue;
};

struct TNullAsEmptyCollectionCase
{
    std::string Filter;
    std::string ExpectedFilter;
    bool MatchesEmpty;
    bool MatchesNonEmpty;
};

class TNullAsEmptyCollectionFilterTest
    : public ::testing::TestWithParam<std::tuple<TCollectionTypeCase, TNullAsEmptyCollectionCase, int>>
{ };

TEST_P(TNullAsEmptyCollectionFilterTest, RewriteAndEvaluate)
{
    const auto& [type, testCase, valueIndex] = GetParam();
    auto value = valueIndex == 0 ? "#" : valueIndex == 1 ? type.EmptyValue : type.NonEmptyValue;
    SCOPED_TRACE(testCase.Filter);
    SCOPED_TRACE(value);
    auto rewritten = RewriteDefaultFilter(testCase.Filter, [&] (TObjectsHolder* holder) {
        return holder->New<TFunctionExpression>(
            NQueryClient::TSourceLocation(), type.Constructor, TExpressionList{});
    });
    auto expectedFilter = testCase.ExpectedFilter;
    SubstGlobal(expectedFilter, "$empty", type.Constructor + "()");
    auto expected = NQueryClient::ParseSource(expectedFilter, NQueryClient::EParseMode::Expression);
    EXPECT_EQ(rewritten, FormatExpression(*std::get<TExpressionPtr>(expected->AstHead.Ast)));

    auto evaluator = CreateExpressionEvaluator(rewritten, {{"x", EValueType::Any}});
    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto payload = NYson::TYsonString(value);
    TNonOwningAttributePayload attributePayload = payload;
    auto result = evaluator->Evaluate(attributePayload, rowBuffer).ValueOrThrow();
    ASSERT_EQ(result.Type, EValueType::Boolean);
    EXPECT_EQ(result.Data.Boolean, valueIndex == 2 ? testCase.MatchesNonEmpty : testCase.MatchesEmpty);
}

INSTANTIATE_TEST_SUITE_P(
    Predicates,
    TNullAsEmptyCollectionFilterTest,
    ::testing::Combine(
        ::testing::Values(
            TCollectionTypeCase{"make_list", "[]", "[7]"},
            TCollectionTypeCase{"make_map", "{}", "{key=7}"}),
        ::testing::Values(
            TNullAsEmptyCollectionCase{"is_null(x)", "%false", false, false},
            TNullAsEmptyCollectionCase{"x = null", "if_null(x, $empty) = null", false, false},
            TNullAsEmptyCollectionCase{
                "yson_length(x) = 0", "yson_length(if_null(x, $empty)) = 0", true, false},
            TNullAsEmptyCollectionCase{
                "yson_length(x) = 1", "yson_length(if_null(x, $empty)) = 1", false, true},
            TNullAsEmptyCollectionCase{
                "is_null(if(%true, null, x))", "is_null(if(%true, null, if_null(x, $empty)))", true, true}),
        ::testing::Values(0, 1, 2)));

struct TScalarTypeCase
{
    NTableClient::EValueType Type;
    std::string DefaultLiteral;
    std::string OtherLiteral;
};

class TNullAsDefaultScalarFilterTest
    : public ::testing::TestWithParam<std::tuple<TScalarTypeCase, std::string, std::string>>
{ };

TEST_P(TNullAsDefaultScalarFilterTest, ScalarPredicates)
{
    const auto& [testCase, value, filterTemplate] = GetParam();
    auto filter = filterTemplate;
    SubstGlobal(filter, "$default", testCase.DefaultLiteral);
    SubstGlobal(filter, "$other", testCase.OtherLiteral);
    SCOPED_TRACE(filter);
    SCOPED_TRACE(value);
    auto rewritten = RewriteDefaultFilter(filter, testCase.Type);
    SCOPED_TRACE(rewritten);
    auto actual = CreateExpressionEvaluator(rewritten, {{"x", testCase.Type}});
    auto parsed = NQueryClient::ParseSource(filter, NQueryClient::EParseMode::Expression);
    auto parsedDefault = NQueryClient::ParseSource(testCase.DefaultLiteral, NQueryClient::EParseMode::Expression);
    auto* defaultExpression = std::get<TExpressionPtr>(parsedDefault->AstHead.Ast);
    TQueryRewriter expectedRewriter(&parsed->AstHead, [&] (const TReference& reference) {
        return parsed->AstHead.New<TFunctionExpression>(
            NQueryClient::TSourceLocation(),
            "if_null",
            TExpressionList{
                parsed->AstHead.New<TReferenceExpression>(NQueryClient::TSourceLocation(), reference),
                defaultExpression,
            });
    });
    auto expected = CreateExpressionEvaluator(
        FormatExpression(*expectedRewriter.Run(std::get<TExpressionPtr>(parsed->AstHead.Ast))),
        {{"x", testCase.Type}});
    auto payload = NYson::TYsonString(value);
    TNonOwningAttributePayload attributePayload = payload;
    std::string stringValue;
    if (testCase.Type == EValueType::String && value != "#") {
        stringValue = value.substr(1, value.size() - 2);
        attributePayload = TStringBuf(stringValue);
    }
    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto actualValue = actual->Evaluate(attributePayload, rowBuffer).ValueOrThrow();
    auto expectedValue = expected->Evaluate(attributePayload, rowBuffer).ValueOrThrow();
    ASSERT_EQ(actualValue.Type, EValueType::Boolean);
    EXPECT_EQ(actualValue.Data.Boolean, expectedValue.Data.Boolean);
}

const auto ScalarPredicates = ::testing::Values(
    "x = $default", "x != $default", "x < $default", "x <= $default", "x > $default", "x >= $default",
    "x = $other", "x != $other", "x < $other", "x <= $other", "x > $other", "x >= $other",
    "$other = x", "$other != x", "$other < x", "$other <= x", "$other > x", "$other >= x",
    "x IN ($default, $other)", "x IN ($other)", "NOT (x IN ($other))",
    "x IN (null, $other)", "NOT (x IN (null, $other))",
    "x IN (null)", "NOT (x IN (null))", "x IN (null, null)",
    "is_null(x)", "NOT is_null(x)",
    "is_null(if(%true, null, x))", "is_null(if(%false, null, x))",
    "x = null", "null = x", "x != null",
    "if(%true, x, $other) = $default", "x = if(%true, $default, $other)",
    "(x, x) IN (($default, $default))", "if(x = $default, x, $other) = $default");

INSTANTIATE_TEST_SUITE_P(
    Int64,
    TNullAsDefaultScalarFilterTest,
    ::testing::Combine(
        ::testing::Values(TScalarTypeCase{EValueType::Int64, "0", "7"}),
        ::testing::Values("#", "0", "7", "-1", "9223372036854775807"),
        ScalarPredicates));

INSTANTIATE_TEST_SUITE_P(
    Uint64,
    TNullAsDefaultScalarFilterTest,
    ::testing::Combine(
        ::testing::Values(TScalarTypeCase{EValueType::Uint64, "0u", "7u"}),
        ::testing::Values("#", "0u", "7u", "18446744073709551615u"),
        ScalarPredicates));

INSTANTIATE_TEST_SUITE_P(
    Double,
    TNullAsDefaultScalarFilterTest,
    ::testing::Combine(
        ::testing::Values(TScalarTypeCase{EValueType::Double, "0.0", "7.0"}),
        ::testing::Values("#", "0.0", "7.0", "-1.0"),
        ScalarPredicates));

INSTANTIATE_TEST_SUITE_P(
    Boolean,
    TNullAsDefaultScalarFilterTest,
    ::testing::Combine(
        ::testing::Values(TScalarTypeCase{EValueType::Boolean, "%false", "%true"}),
        ::testing::Values("#", "%false", "%true"),
        ScalarPredicates));

INSTANTIATE_TEST_SUITE_P(
    String,
    TNullAsDefaultScalarFilterTest,
    ::testing::Combine(
        ::testing::Values(TScalarTypeCase{EValueType::String, "\"\"", "\"abc\""}),
        ::testing::Values("#", "\"\"", "\"abc\"", "\"xyz\""),
        ScalarPredicates));

INSTANTIATE_TEST_SUITE_P(
    NegativeConstant,
    TNullAsDefaultScalarFilterTest,
    ::testing::Combine(
        ::testing::Values(TScalarTypeCase{EValueType::Int64, "0", "7"}),
        ::testing::Values("#", "0", "7", "-1", "9223372036854775807"),
        ::testing::Values("x < -1")));

INSTANTIATE_TEST_SUITE_P(
    Arithmetic,
    TNullAsDefaultScalarFilterTest,
    ::testing::Combine(
        ::testing::Values(TScalarTypeCase{EValueType::Int64, "0", "7"}),
        ::testing::Values("#", "0", "7", "-1"),
        ::testing::Values("x + 1 = 1", "x * 2 = 0", "x + x = 0", "x BETWEEN 0 AND 7")));

INSTANTIATE_TEST_SUITE_P(
    StringFunctions,
    TNullAsDefaultScalarFilterTest,
    ::testing::Combine(
        ::testing::Values(TScalarTypeCase{EValueType::String, "\"\"", "\"abc\""}),
        ::testing::Values("#", "\"\"", "\"abc\""),
        ::testing::Values("length(x) = 0", "concat(x, \"!\") = \"!\"")));

INSTANTIATE_TEST_SUITE_P(
    BooleanExpressions,
    TNullAsDefaultScalarFilterTest,
    ::testing::Combine(
        ::testing::Values(TScalarTypeCase{EValueType::Boolean, "%false", "%true"}),
        ::testing::Values("#", "%false", "%true"),
        ::testing::Values("NOT x", "x OR %false", "if(x, %false, %true)")));

class TNullAsDefaultKeyConstraintsTest
    : public ::testing::TestWithParam<std::tuple<std::string, std::string, bool>>
{ };

TEST_P(TNullAsDefaultKeyConstraintsTest, KeyConstraints)
{
    const auto& [filter, expected, uniquePrefix] = GetParam();
    const NTableClient::TTableSchema schema({
        NTableClient::TColumnSchema("x", NTableClient::EValueType::Int64),
    });
    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto constraintsFor = [&] (const std::string& filter) {
        NQueryClient::TConstraintsHolder constraints(
            1, GetRefCountedTypeCookie<NQueryClient::TConstraintsHolder>(), GetDefaultMemoryChunkProvider());
        auto root = constraints.ExtractFromExpression(
            NQueryClient::PrepareExpression(filter, schema), {"x"}, rowBuffer);
        return NQueryClient::ToString(constraints, root);
    };
    SCOPED_TRACE(filter);
    auto rewritten = RewriteDefaultFilter(filter, EValueType::Int64);
    EXPECT_EQ(constraintsFor(rewritten), constraintsFor(expected));
    auto parsed = NQueryClient::ParseSource(rewritten, NQueryClient::EParseMode::Expression);
    auto* expression = std::get<TExpressionPtr>(parsed->AstHead.Ast);
    EXPECT_TRUE(IntrospectFilterForDefinedReference(expression, TReference("x"), /*allowValueRange*/ true));
    TQueryRewriter qualify(&parsed->AstHead, [&] (const TReference& reference) {
        return parsed->AstHead.New<TReferenceExpression>(
            NQueryClient::TSourceLocation(), TReference(reference.ColumnName, "i"));
    });
    EXPECT_EQ(TryOptimizeGroupByWithUniquePrefix(qualify.Run(expression), {"x"}, "i"), uniquePrefix);
}

INSTANTIATE_TEST_SUITE_P(
    Predicates,
    TNullAsDefaultKeyConstraintsTest,
    ::testing::Values(
        std::tuple{"x = 0", "x IN (null, 0)", false},
        std::tuple{"x = 7", "x = 7", true},
        std::tuple{"x IN (0, 7)", "x IN (null, 0, 7)", false},
        std::tuple{"x > 7", "x > 7", false},
        std::tuple{"x < 0", "NOT (x = null) AND x < 0", false},
        std::tuple{"x >= 0", "x = null OR x >= 0", false},
        std::tuple{"x = 7 AND x + 1 > 0", "x = 7", true},
        std::tuple{"x IN (0, 7) AND x + 1 > 0", "x IN (null, 0, 7)", false}));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery::NTests
