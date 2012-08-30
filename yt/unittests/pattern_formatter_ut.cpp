#include "stdafx.h"

#include <ytlib/misc/pattern_formatter.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPatternFormatterTest
    : public ::testing::Test
{
protected:
    TPatternFormatter Formatter;
};

#define EXPECT_FORMAT(pattern, expected) \
    EXPECT_EQ((expected), Formatter.Format(pattern))

TEST_F(TPatternFormatterTest, EmptyPattern)
{
    Formatter.AddProperty("key", "value");
    EXPECT_FORMAT("", "");
}

TEST_F(TPatternFormatterTest, PatternWithoutProperties)
{
    Formatter.AddProperty("key", "value");
    EXPECT_FORMAT("some text", "some text");
}

TEST_F(TPatternFormatterTest, PropertyWithEmptyName1)
{
    Formatter.AddProperty("", "");
    EXPECT_FORMAT("<<-$()->>", "<<-->>");
}

TEST_F(TPatternFormatterTest, PropertyWithEmptyName2)
{
    Formatter.AddProperty("", "foobar");
    EXPECT_FORMAT("<<-$()->>", "<<-foobar->>");
}

TEST_F(TPatternFormatterTest, CommonCase)
{
    Formatter.AddProperty("a", "1");
    Formatter.AddProperty("b", "10");
    Formatter.AddProperty("c", "100");
    EXPECT_FORMAT("$(a)", "1");
    EXPECT_FORMAT(
        "Hello! You own me $(a)$(b) dollars; not $(c)!",
        "Hello! You own me 110 dollars; not 100!");

}

TEST_F(TPatternFormatterTest, MultiplePropertyDeclaration)
{
    Formatter.AddProperty("x", "1");
    EXPECT_FORMAT("<$(x)>", "<1>");
    Formatter.AddProperty("x", "2");
    EXPECT_FORMAT("<$(x)>", "<2>");
    Formatter.AddProperty("x", "3");
    EXPECT_FORMAT("<$(x)>", "<3>");
}

TEST_F(TPatternFormatterTest, MultiplePropertyUsage)
{
    Formatter.AddProperty("x", "2");
    EXPECT_FORMAT("$(x) = 2", "2 = 2");
    EXPECT_FORMAT("$(x) + $(x) = 4", "2 + 2 = 4");
    EXPECT_FORMAT("$(x) + $(x) + $(x) = 6", "2 + 2 + 2 = 6");
}

TEST_F(TPatternFormatterTest, UndefinedValue)
{
    Formatter.AddProperty("a", "b");
    Formatter.AddProperty("key", "value");
    EXPECT_THROW(Formatter.Format("$(a) $(kkeeyy)"), std::exception);
}

TEST_F(TPatternFormatterTest, OptionalValue)
{
    Formatter.AddProperty("abc", "1");
    EXPECT_FORMAT("$(abc)-$(def?)-", "1--");
}

TEST_F(TPatternFormatterTest, BadSyntax)
{
    Formatter.AddProperty("a", "b");
    EXPECT_THROW(Formatter.Format("$(a"), std::exception);
    EXPECT_THROW(Formatter.Format("$a)"), std::exception);
    EXPECT_THROW(Formatter.Format("$"),   std::exception);
}

#undef EXPECT_FORMAT

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

