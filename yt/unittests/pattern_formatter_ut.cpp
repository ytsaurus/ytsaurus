#include "../ytlib/misc/pattern_formatter.h"

#include "framework/framework.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPatternFormatterTest : public ::testing::Test {
protected:
    TPatternFormatter Formatter;
};

#define ASSERT_FORMAT(pattern, expected) \
    ASSERT_EQ(Formatter.Format(pattern), (expected))

TEST_F(TPatternFormatterTest, EmptyPattern)
{
    Formatter.AddProperty("key", "value");
    ASSERT_FORMAT("", "");
}

TEST_F(TPatternFormatterTest, PatternWithoutProperties)
{
    Formatter.AddProperty("key", "value");
    ASSERT_FORMAT("some text", "some text");
}

TEST_F(TPatternFormatterTest, PropertyWithEmptyName1)
{
    Formatter.AddProperty("", "");
    ASSERT_FORMAT("<<-$()->>", "<<-->>");
}

TEST_F(TPatternFormatterTest, PropertyWithEmptyName2)
{
    Formatter.AddProperty("", "foobar");
    ASSERT_FORMAT("<<-$()->>", "<<-foobar->>");
}

TEST_F(TPatternFormatterTest, CommonCase)
{
    Formatter.AddProperty("a", "1");
    Formatter.AddProperty("b", "10");
    Formatter.AddProperty("c", "100");
    ASSERT_FORMAT("$(a)", "1");
    ASSERT_FORMAT(
        "Hello! You own me $(a)$(b) dollars; not $(c)!",
        "Hello! You own me 110 dollars; not 100!");

}

TEST_F(TPatternFormatterTest, MultiplePropertyDeclaration)
{
    Formatter.AddProperty("x", "1");
    ASSERT_FORMAT("<$(x)>", "<1>");
    Formatter.AddProperty("x", "2");
    ASSERT_FORMAT("<$(x)>", "<2>");
    Formatter.AddProperty("x", "3");
    ASSERT_FORMAT("<$(x)>", "<3>");
}

TEST_F(TPatternFormatterTest, MultiplePropertyUsage)
{
    Formatter.AddProperty("x", "2");
    ASSERT_FORMAT("$(x) = 2", "2 = 2");
    ASSERT_FORMAT("$(x) + $(x) = 4", "2 + 2 = 4");
    ASSERT_FORMAT("$(x) + $(x) + $(x) = 6", "2 + 2 + 2 = 6");
}

TEST_F(TPatternFormatterTest, UndefinedValue)
{
    Formatter.AddProperty("a", "b");
    Formatter.AddProperty("key", "value");
    ASSERT_THROW(Formatter.Format("$(a) $(kkeeyy)"), yexception);
}

TEST_F(TPatternFormatterTest, OptionalValue)
{
    Formatter.AddProperty("abc", "1");
    ASSERT_FORMAT("$(abc)-$(def?)-", "1--");
}

TEST_F(TPatternFormatterTest, BadSyntax)
{
    Formatter.AddProperty("a", "b");
    ASSERT_THROW(Formatter.Format("$(a"), yexception);
    ASSERT_THROW(Formatter.Format("$a)"), yexception);
    ASSERT_THROW(Formatter.Format("$"),   yexception);
}

#undef ASSERT_FORMAT

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

