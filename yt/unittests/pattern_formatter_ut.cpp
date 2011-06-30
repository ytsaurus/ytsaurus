// Add includes here.

#include "../ytlib/misc/pattern_formatter.h"
#include <library/unittest/registar.h>

namespace NYT {

class TPatternFormatterTest
    : public TTestBase
{
    UNIT_TEST_SUITE(TPatternFormatterTest);
        UNIT_TEST(TestEmptyName);

        UNIT_TEST(TestEmptyPattern);
        UNIT_TEST(TestPatternWithoutProperties);
        UNIT_TEST(TestEmptyName);
        UNIT_TEST(TestMultipleDefinition);
        UNIT_TEST(TestUsualCase);
        UNIT_TEST(TestUndefinedValue);
        UNIT_TEST(TestOptionalValue);
        UNIT_TEST(TestBadSyntax);
    UNIT_TEST_SUITE_END();

public:

    static void CheckFormatting(
        TPatternFormatter& formatter,
        Stroka pattern, Stroka answer)
    {
        Stroka result = formatter.Format(pattern);
        UNIT_ASSERT_EQUAL(result, answer);
    }

    void TestEmptyPattern()
    {
        TPatternFormatter formatter;
        formatter.AddProperty("key", "value");
        CheckFormatting(formatter, "", "");
    }

    void TestPatternWithoutProperties()
    {
        TPatternFormatter formatter;
        formatter.AddProperty("key1", "value1");
        formatter.AddProperty("key2", "value2");
        CheckFormatting(formatter, "some text", "some text");
    }

    void TestEmptyName()
    {
        {
            TPatternFormatter formatter;
            formatter.AddProperty("", "");
            CheckFormatting(formatter, "<<-$()->>", "<<-->>");
        }
        {
            TPatternFormatter formatter;
            formatter.AddProperty("", "aaa");
            CheckFormatting(formatter, "<<-$()->>", "<<-aaa->>");
        }
    }

    void TestUsualCase()
    {
        TPatternFormatter formatter;
        formatter.AddProperty("a", "1");
        formatter.AddProperty("b", "10");
        formatter.AddProperty("c", "100");
        CheckFormatting(formatter, "$(a)", "1");
        CheckFormatting(formatter,
            "abcd - $(a)$(b) wxyz $(c)", "abcd - 110 wxyz 100");

    }

    void TestMultipleDefinition()
    {
        TPatternFormatter formatter;
        formatter.AddProperty("aa", "1");
        formatter.AddProperty("aa", "2");
        CheckFormatting(formatter, "$(aa) + $(aa) = 4", "2 + 2 = 4");
        formatter.AddProperty("aa", "9");
        CheckFormatting(formatter, "3 * 3 = $(aa)", "3 * 3 = 9");
    }

    void TestUndefinedValue()
    {
        TPatternFormatter formatter;
        formatter.AddProperty("a", "b");
        formatter.AddProperty("key", "value");
        UNIT_ASSERT_EXCEPTION(formatter.Format("$(a) $(kkeeyy)"), yexception);
    }

    void TestOptionalValue()
    {
        TPatternFormatter formatter;
        formatter.AddProperty("abc", "1");
        CheckFormatting(formatter, "$(abc)-$(def?)-", "1--");
    }


    void TestBadSyntax()
    {
        TPatternFormatter formatter;
        formatter.AddProperty("a", "b");
        UNIT_ASSERT_EXCEPTION(formatter.Format("$(a"), yexception);
        UNIT_ASSERT_EXCEPTION(formatter.Format("$a)"), yexception);
        UNIT_ASSERT_EXCEPTION(formatter.Format("$"), yexception);
    }

};

UNIT_TEST_SUITE_REGISTRATION(TPatternFormatterTest);

} // namespace NYT
