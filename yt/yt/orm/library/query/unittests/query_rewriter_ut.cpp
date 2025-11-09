#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/orm/client/misc/error.h>

#include <yt/yt/orm/library/query/query_rewriter.h>

using namespace NYT::NQueryClient::NAst;

namespace NYT::NOrm::NQuery::NTests {

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

} // namespace NYT::NOrm::NQuery::NTests
