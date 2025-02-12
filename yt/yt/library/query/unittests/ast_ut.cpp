#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NQueryClient {
namespace {

using namespace NQueryClient::NAst;
using NQueryClient::NAst::TExpressionPtr;
using NQueryClient::NAst::TQuery;

////////////////////////////////////////////////////////////////////////////////

class TAstFormatTest
    : public ::testing::Test
{
protected:
    void TestExpression(const TString& source)
    {
        auto parsedSource1 = ParseSource(source, EParseMode::Expression);
        auto formattedSource = FormatExpression(*std::get<TExpressionPtr>(parsedSource1->AstHead.Ast));
        auto parsedSource2 = ParseSource(formattedSource, EParseMode::Expression);
        EXPECT_TRUE(*std::get<TExpressionPtr>(parsedSource1->AstHead.Ast) == *std::get<TExpressionPtr>(parsedSource2->AstHead.Ast))
            << source << " -> " << formattedSource;
    }

    void TestQuery(const TString& source)
    {
        auto parsedSource1 = ParseSource(source, EParseMode::Query);
        auto formattedSource = FormatQuery(std::get<NAst::TQuery>(parsedSource1->AstHead.Ast));
        auto parsedSource2 = ParseSource(formattedSource, EParseMode::Query);
        EXPECT_TRUE(std::get<NAst::TQuery>(parsedSource1->AstHead.Ast) == std::get<NAst::TQuery>(parsedSource2->AstHead.Ast))
            << source << " -> " << formattedSource;
    }
};

TEST_F(TAstFormatTest, Id)
{
    EXPECT_EQ("id", FormatId("id"));
    EXPECT_EQ("`0`", FormatId("0"));
    EXPECT_EQ("x0123456789_", FormatId("x0123456789_"));
    EXPECT_EQ("``", FormatId(""));
    EXPECT_EQ("___", FormatId("___"));
    EXPECT_EQ("`offset`", FormatId("offset"));
    EXPECT_EQ("`JOIN`", FormatId("JOIN"));
    EXPECT_EQ("`as`", FormatId("as"));
    EXPECT_EQ("`\\``", FormatId("`"));
}

TEST_F(TAstFormatTest, Reference)
{
    EXPECT_EQ("column", FormatReference(TReference(TString("column"))));
    EXPECT_EQ("table.column", FormatReference(TReference(TString("column"), TString("table"))));
    EXPECT_EQ("`my.column`", FormatReference(TReference(TString("my.column"))));
    EXPECT_EQ("table.`my.column`", FormatReference(TReference(TString("my.column"), TString("table"))));
    EXPECT_EQ("my.column", InferColumnName(TReference(TString("my.column"))));
    EXPECT_EQ("table.my.column", InferColumnName(TReference(TString("my.column"), TString("table"))));
}

TEST_F(TAstFormatTest, LiteralValue)
{
    EXPECT_EQ("null", FormatLiteralValue(TLiteralValue(std::in_place_type_t<TNullLiteralValue>())));
    EXPECT_EQ("0", FormatLiteralValue(TLiteralValue(std::in_place_type_t<i64>(), 0)));
    EXPECT_EQ("123", FormatLiteralValue(TLiteralValue(std::in_place_type_t<i64>(), 123)));
    EXPECT_EQ("9223372036854775807", FormatLiteralValue(TLiteralValue(std::in_place_type_t<i64>(), std::numeric_limits<i64>::max())));
    EXPECT_EQ("-9223372036854775808", FormatLiteralValue(TLiteralValue(std::in_place_type_t<i64>(), std::numeric_limits<i64>::min())));
    EXPECT_EQ("-123", FormatLiteralValue(TLiteralValue(std::in_place_type_t<i64>(), -123)));
    EXPECT_EQ("0u", FormatLiteralValue(TLiteralValue(std::in_place_type_t<ui64>(), 0)));
    EXPECT_EQ("123u", FormatLiteralValue(TLiteralValue(std::in_place_type_t<ui64>(), 123)));
    EXPECT_EQ("18446744073709551615u", FormatLiteralValue(TLiteralValue(std::in_place_type_t<ui64>(), std::numeric_limits<ui64>::max())));
    EXPECT_EQ("3.140000", FormatLiteralValue(TLiteralValue(std::in_place_type_t<double>(), 3.14)));
    EXPECT_EQ("\"\"", FormatLiteralValue(TLiteralValue(std::in_place_type_t<TString>(), "")));
    EXPECT_EQ("\"\\\\\"", FormatLiteralValue(TLiteralValue(std::in_place_type_t<TString>(), "\\")));
    EXPECT_EQ("\"hello\"", FormatLiteralValue(TLiteralValue(std::in_place_type_t<TString>(), "hello")));
}

TEST_F(TAstFormatTest, Expression)
{
    TestExpression("\"dochelper.Физ\\\\. Лицо\"");
    TestExpression("a");
    TestExpression("a + b");
    TestExpression("a - b");
    TestExpression("a * b");
    TestExpression("a / b");
    TestExpression("a % b");
    TestExpression("-a");
    TestExpression("a or b");
    TestExpression("a and b");
    TestExpression("not a");
    TestExpression("a < b");
    TestExpression("a > b");
    TestExpression("a <= b");
    TestExpression("a >= b");
    TestExpression("a = b");
    TestExpression("(a, b) > (1, 2)");
    TestExpression("a != b");
    TestExpression("-a in (1)");
    TestExpression("not (a in (1))");
    TestExpression("(a + b) in (1)");
    TestExpression("a in (1)");
    TestExpression("a in (1, 2)");
    TestExpression("a in (1, 2, 3)");
    TestExpression("a in ((1))");
    TestExpression("a in ((1), (2))");
    TestExpression("a in ((1, 2), (2, 3))");
    TestExpression("(a,b) in ((1, 2), (2, 3))");
    TestExpression("transform(a, (1, 2), (2, 3))");
    TestExpression("transform((a, b), ((1, 2), (2, 3)), (\"x\", \"y\"))");
    TestExpression("transform((a, b), ((1, 2), (2, 3)), (\"x\", \"y\"), a + 1)");
    TestExpression("case when c1 then r1 end");
    TestExpression("case when c1 then r1 when c2 then r2 end");
    TestExpression("case when c1 then r1 when c2 then r2 else r3 end");
    TestExpression("case x when c1 then r1 end");
    TestExpression("case x when c1 then r1 when c2 then r2 end");
    TestExpression("case x when c1 then r1 when c2 then r2 else r3 end");
    TestExpression("a * (b + c)");
    TestExpression("a * b + c");
    TestExpression("0");
    TestExpression("null");
    TestExpression("#");
    TestExpression("a & b");
    TestExpression("a | b");
    TestExpression("~a");
    TestExpression("f()");
    TestExpression("f(a)");
    TestExpression("f(a, b)");
    TestExpression("f(a, b, c)");
    TestExpression("(a + 1 as x) * x");
    TestExpression("([x-y] as [x-y])");
    TestExpression("(`x-y` as `x-y`)");
    TestExpression("`\\``");
    TestExpression("[`]");
    TestExpression("`[`");
    TestExpression("`]`");
    TestExpression("`[]`");
    TestExpression("`][`");
    TestExpression("`]]]`");
    TestExpression("`[[[`");
    TestExpression("(`x \\- y` as `x \\- y`)");
    TestExpression("[a]");
    TestExpression("`a`");
    TestExpression("`\\n \\x42 \\u2019 \\` `");
    TestExpression("[t.a]");
    TestExpression("`t.a`");
    TestExpression("t.[a]");
    TestExpression("t.`a`");
    TestExpression("[t.a] + b");
    TestExpression("`t.a` + b");
    TestExpression("t.[a] + b");
    TestExpression("t.`a` + b");
}

TEST_F(TAstFormatTest, Query)
{
    TestQuery("* from t");
    TestQuery("a from t");
    TestQuery("a, b from t");
    TestQuery("t.a, t.b from t");
    TestQuery("* from t where key > 0");
    TestQuery("* from t with index table_index");
    TestQuery("* from t order by key");
    TestQuery("* from t order by key asc");
    TestQuery("* from t order by key desc");
    TestQuery("* from t order by key1 asc, key2 desc");
    TestQuery("* from t group by key");
    TestQuery("* from t group by key1, key2");
    TestQuery("* from t group by 0 as x");
    TestQuery("* from t group by a + b as y, b * c as z");
    TestQuery("* from t group by a with totals");
    TestQuery("* from t group by a with totals having b > 0");
    TestQuery("* from t group by a having b > 0 with totals");
    TestQuery("* from t order by key offset 100");
    TestQuery("* from t order by key offset 100 limit 100");
    TestQuery("* from t limit 100");
    TestQuery("* from t t_alias");
    TestQuery("* from t as t_alias");
    TestQuery("* from t1 join t2 using a, b");
    TestQuery("* from t1 join t2 using a, b and a > b");
    TestQuery("* from t1 left join t2 using a, b and a > b");
    TestQuery("* from t1 left join t2 on a = b");
    TestQuery("* from t1 left join t2 on t1.a = t2.b");
    TestQuery("* from t1 left join t2 on a = b and c > d");
    TestQuery("* from t1 left join t2 on a = b join t3 using x");
    TestQuery("* from t1 left join t2 on (a1, a2) = (b1, b2) join t3 using x");
    TestQuery("* from t1 array join a1 as u1, a2 as u2 and u1 != 2");
    TestQuery("* from t1 left array join a1 + a2 as u1, b1 as u2");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
