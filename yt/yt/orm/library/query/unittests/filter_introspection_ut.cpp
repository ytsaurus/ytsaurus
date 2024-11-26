#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/library/query/filter_introspection.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NOrm::NQuery::NTests {
namespace {

using namespace NYT::NQueryClient;
using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

TEST(TFilterIntrospectionTest, DefinedAttributeValue)
{
    // Invalid attribute path.
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("1=1", "//"), TErrorException);
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("1=1", "/abr/"), TErrorException);
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("1=1", ""), TErrorException);

    // Invalid filter.
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("=", "/meta/id"), TErrorException);
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("1=", "/meta/id"), TErrorException);
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("(a,b,c)", "/meta/id"), TErrorException);

    // Other types.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=1", "/meta/id").TryMoveAs<i64>(), 1);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=1u", "/meta/id").TryMoveAs<ui64>(), 1);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=1u", "/meta/id").TryMoveAs<i64>(), std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=null", "/meta/id").TryMoveAs<TNullLiteralValue>(), TNullLiteralValue{});
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=3.5", "/meta/id").TryMoveAs<double>(), 3.5);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=%false", "/meta/id").TryMoveAs<bool>(), std::make_optional(false));

    // Incorrect type.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=(1,2)", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=(1+2+3,2)", "/meta/id").Value, std::nullopt);

    // Equality.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\"", "/meta/id").TryMoveAs<TString>(), "aba");

    // And.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" and [/meta/creation_time] > 100", "/meta/id").TryMoveAs<TString>(), "aba");
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" and not ([/meta/id]=\"aba\")", "/meta/id").TryMoveAs<TString>(), "aba");
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" and [/meta/id]=\"cde\"", "/meta/id").TryMoveAs<TString>(), "aba");
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" and 123", "/meta/id").TryMoveAs<TString>(), "aba");

    // Or.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" or [/meta/id]=\"cde\"", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" or [/meta/id]=\"aba\"", "/meta/id").TryMoveAs<TString>(), "aba");
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" or 123", "/meta/id").Value, std::nullopt);

    // Too complex for now.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"a\"+\"b\"", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("([/meta/id],[/meta/creation_time])=(\"aba\",1020)", "/meta/id").Value, std::nullopt);

    // Other.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("not ([/meta/id]=\"aba\")", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("10+20+30", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("abracadabra", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("", "/meta/id").Value, std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

bool RunIntrospectFilterForDefinedReference(
    const TString& expressionString,
    const std::string& referenceName,
    const std::optional<TString>& tableName = std::nullopt,
    bool allowValueRange = true)
{
    auto parsedQuery = ParseSource(expressionString, NQueryClient::EParseMode::Expression);
    auto expression = std::get<NAst::TExpressionPtr>(parsedQuery->AstHead.Ast);

    return IntrospectFilterForDefinedReference(expression, NQueryClient::NAst::TReference(referenceName, tableName), allowValueRange);
}

TEST(TFilterIntrospectionTest, DefinedReference)
{
    // Defined simple.
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1",
        "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "i.[/spec/year]=1",
        "/spec/year",
        "i"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]<2",
        "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/name]=\"text\"",
         "/spec/name"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year] in (1, 2, 3)",
         "/spec/year"));

    // Defined AND.
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/name]=\"text\" AND [/spec/year]=1",
         "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/name]=\"text\" AND ([/spec/author]=\"Tom\" AND [/spec/year]>0)",
         "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1 AND true",
        "/spec/year"));

    // Defined OR.
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR [/spec/year]<=2000",
         "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR ([/spec/publisher]=\"O'Relly\" AND [/spec/year]<=2000)",
         "/spec/year"));

    // Defined IN.
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year] IN (2000)",
         "/spec/year",
         /*tableName*/ {},
         /*allowValueRange*/ false));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year] IN (2000, 2001)",
         "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "([/spec/year], [/spec/author]) IN ((2000, \"Tom\"), (2001, \"Jim\"))",
         "/spec/year"));

    // Defined Between.
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year] BETWEEN 2000 and 2001",
         "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "([/spec/year], [/spec/author]) BETWEEN ((2000, \"Tom\") and (2001, \"Jim\"))",
         "/spec/year"));

    // Not defined simple.
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1",
        "/spec/name"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "True",
        "/spec/name"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1",
        ""));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1",
        "spec.year"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]",
        "/spec/year"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]<2",
        "/spec/year",
        /*tableName*/ std::nullopt,
        /*allowValueRange*/ false));

    // Not defined yet.
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "list_contains([/spec/genres], \"fantasy\")",
        "/spec/genres"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR false",
         "/spec/year"));

    // Not defined AND.
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/name]=\"text\" AND [/spec/year]=1",
         "/spec/author"));

    // Not defined OR.
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR [/spec/name]=\"text\"",
         "/spec/year"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR [/spec/name]=\"text\"",
         "/spec/genres"));

    // Not defined BETWEEN.
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year] BETWEEN 2000 and 2001",
         "/spec/name"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFilterIntrospectionTest, ExtractAllReferences)
{
    // Check constant node filters.
    for (const auto& nodeFilter : {
            "",
            "%true",
            "%false",
            "1 > 2",
            "(5 + 4) * 2",
            "is_substr(\"Intel\", \"Intel(R) Xeon(R) CPU E5-2660 0 @ 2.20GHz\")",
        })
    {
        THashSet<std::string> result;
        ExtractFilterAttributeReferences(
            nodeFilter,
            [&result] (const std::string& attribute) {
                result.insert(attribute);
            });
        EXPECT_EQ(result, THashSet<std::string>());
    }

    // Check simple expressions.
    for (const auto& opString : {"=", "!=", ">", "<", "<=", ">="})
    {
        THashSet<std::string> result;
        ExtractFilterAttributeReferences(
            Format("[/spec/weight] %v 152", opString),
            [&result] (const std::string& attribute) {
                result.insert(attribute);
            });
        EXPECT_EQ(result, THashSet<std::string>{"/spec/weight"});
    }

    // Check complex expression with repetition.
    {
        THashSet<std::string> result;
        ExtractFilterAttributeReferences(
            "[/labels/position] = 153 OR is_substr(\"disabled\", [/status/state/raw])"
            "OR list_contains([/spec/supported_modes], \"CMP\") AND NOT ([/status/disabled] = %true"
            "OR is_substr(\"disabled\", [/status/state/raw]))",
            [&result] (const std::string& attribute) {
                result.insert(attribute);
            });
        EXPECT_EQ(
            result,
            THashSet<std::string>({"/labels/position", "/spec/supported_modes", "/status/disabled", "/status/state/raw"}));
    }

    // Check expression with repetition, extraction to vector
    {
        std::vector<std::string> result;
        ExtractFilterAttributeReferences(
            "[/labels/position] > 153 OR is_substr(\"disabled\", [/status/state/raw]) or [/labels/position] < 152",
            [&result] (const std::string& attribute) {
                result.push_back(attribute);
            });
        EXPECT_EQ(
            result,
            std::vector<std::string>({"/labels/position", "/status/state/raw", "/labels/position"}));
    }
}

TEST(TFilterIntrospectionTest, FullScanIntrospection)
{
    NQueryClient::NAst::TQuery query;

    TObjectsHolder holder;
    query.WherePredicate = MakeExpression<NAst::TBinaryOpExpression>(
        &holder,
        NQueryClient::TSourceLocation(),
        EBinaryOp::Equal,
        MakeExpression<NAst::TReferenceExpression>(&holder, NQueryClient::TSourceLocation(), "foo"),
        MakeExpression<NAst::TLiteralExpression>(&holder, NQueryClient::TSourceLocation(), 5));

    EXPECT_FALSE(IntrospectQueryForFullScan(
        &query,
        /*firstKeyFieldName*/ "hash",
        /*firstNonEvaluatedKeyFieldName*/ "foo"));
    EXPECT_TRUE(IntrospectQueryForFullScan(&query, "hash", "bar"));

    // TODO(dgolear): Enable when order by is taken into account.
    // query.WherePredicate.value()[0]->As<NAst::TBinaryOpExpression>()->Opcode = EBinaryOp::Less;
    // query.OrderExpressions.push_back(TOrderExpression{
    //     .Expressions = MakeExpression<NAst::TReferenceExpression>(&holder, NQueryClient::TSourceLocation(), "foo"),
    // });
    // EXPECT_TRUE(IntrospectQueryForFullScan(&query, "hash", "foo"));
    // query.OrderExpressions[0].Expressions[0]->As<NAst::TReferenceExpression>()->Reference.ColumnName = "hash";
    // EXPECT_FALSE(IntrospectQueryForFullScan(&query, "hash", "foo"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests
