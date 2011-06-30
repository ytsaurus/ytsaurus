#include "../ytlib/misc/preprocessor.h"

#include <library/unittest/registar.h>

namespace NYT {

class TPreprocessorTest
    : public TTestBase
{
    UNIT_TEST_SUITE(TPreprocessorTest);
        UNIT_TEST(TestConcatenation);
        UNIT_TEST(TestStringize);
        UNIT_TEST(TestCount);
        UNIT_TEST(TestKill);
        UNIT_TEST(TestHead);
        UNIT_TEST(TestTail);
        UNIT_TEST(TestForEach);
        UNIT_TEST(TestElement);
        UNIT_TEST(TestConditional);
        UNIT_TEST(TestIsSequence);
    UNIT_TEST_SUITE_END();

public:
    void TestConcatenation()
    {
        UNIT_ASSERT_EQUAL(PP_CONCAT(1, 2), 12);
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_CONCAT(Foo, Bar)), "FooBar");
    }

    void TestStringize()
    {
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(123456), "123456");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(FooBar), "FooBar");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(T::XYZ), "T::XYZ");
    }

    void TestCount()
    {
        UNIT_ASSERT_EQUAL(PP_COUNT(), 0);
        UNIT_ASSERT_EQUAL(PP_COUNT((0)), 1);
        UNIT_ASSERT_EQUAL(PP_COUNT((0)(0)), 2);
        UNIT_ASSERT_EQUAL(PP_COUNT((0)(0)(0)), 3);
        UNIT_ASSERT_EQUAL(PP_COUNT((0)(0)(0)(0)), 4);
        UNIT_ASSERT_EQUAL(PP_COUNT((0)(0)(0)(0)(0)), 5);
    }

    void TestKill()
    {
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_NIL PP_KILL((0), 0)), "PP_NIL (0)");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_NIL PP_KILL((0), 1)), "PP_NIL");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_NIL PP_KILL((0)(1)(2), 0)), "PP_NIL (0)(1)(2)");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_NIL PP_KILL((0)(1)(2), 1)), "PP_NIL (1)(2)");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_NIL PP_KILL((0)(1)(2), 2)), "PP_NIL (2)");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_NIL PP_KILL((0)(1)(2), 3)), "PP_NIL");
    }

    void TestHead()
    {
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_HEAD((0))), "0");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_HEAD((0)(1))), "0");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_HEAD((0)(1)(2))), "0");
    }

    void TestTail()
    {
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_NIL PP_TAIL((0))), "PP_NIL");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_NIL PP_TAIL((0)(1))), "PP_NIL (1)");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_NIL PP_TAIL((0)(1)(2))), "PP_NIL (1)(2)");
    }

    void TestForEach()
    {
        UNIT_ASSERT_STRINGS_EQUAL(
            PP_STRINGIZE(PP_FOR_EACH(PP_STRINGIZE, (Foo)(Bar)(Spam)(Ham))),
            "\"Foo\" \"Bar\" \"Spam\" \"Ham\""
        );
#define my_functor(x) +x+
        UNIT_ASSERT_STRINGS_EQUAL(
            PP_STRINGIZE(PP_FOR_EACH(my_functor, (1)(2)(3))),
            "+1+ +2+ +3+"
        );
#undef  my_functor
    }

    void TestElement()
    {
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1), 0), 1);
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1)(2), 0), 1);
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1)(2), 1), 2);
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1)(2)(3), 0), 1);
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1)(2)(3), 1), 2);
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1)(2)(3), 2), 3);
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1)(2)(3)(4), 0), 1);
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1)(2)(3)(4), 1), 2);
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1)(2)(3)(4), 2), 3);
        UNIT_ASSERT_EQUAL(PP_ELEMENT((1)(2)(3)(4), 3), 4);
    }

    void TestConditional()
    {
        UNIT_ASSERT_EQUAL(PP_IF(PP_TRUE,  1, 2), 1);
        UNIT_ASSERT_EQUAL(PP_IF(PP_FALSE, 1, 2), 2);
    }

    void TestIsSequence()
    {
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_IS_SEQUENCE( 0    )), "PP_FALSE");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_IS_SEQUENCE((0)   )), "PP_TRUE");
        UNIT_ASSERT_STRINGS_EQUAL(PP_STRINGIZE(PP_IS_SEQUENCE((0)(0))), "PP_TRUE");
    }
};

UNIT_TEST_SUITE_REGISTRATION(TPreprocessorTest);

} // namespace NYT
