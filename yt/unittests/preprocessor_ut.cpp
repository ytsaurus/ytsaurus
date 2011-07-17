#include "../ytlib/misc/preprocessor.h"

#include "framework/framework.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TEST(TPreprocessorTest, Concatenation)
{
    ASSERT_EQ(12, PP_CONCAT(1, 2));
    ASSERT_STREQ("FooBar", PP_STRINGIZE(PP_CONCAT(Foo, Bar)));
}

TEST(TPreprocessorTest, Stringize)
{
    ASSERT_STREQ(PP_STRINGIZE(123456), "123456");
    ASSERT_STREQ(PP_STRINGIZE(FooBar), "FooBar");
    ASSERT_STREQ(PP_STRINGIZE(T::XYZ), "T::XYZ");
}

TEST(TPreprocessorTest, Count)
{
    ASSERT_EQ(0, PP_COUNT());
    ASSERT_EQ(1, PP_COUNT((0)));
    ASSERT_EQ(2, PP_COUNT((0)(0)));
    ASSERT_EQ(3, PP_COUNT((0)(0)(0)));
    ASSERT_EQ(4, PP_COUNT((0)(0)(0)(0)));
    ASSERT_EQ(5, PP_COUNT((0)(0)(0)(0)(0)));
}

TEST(TPreprocessorTest, Kill)
{
    ASSERT_STREQ("PP_NIL (0)",       PP_STRINGIZE(PP_NIL PP_KILL((0), 0)));
    ASSERT_STREQ("PP_NIL",           PP_STRINGIZE(PP_NIL PP_KILL((0), 1)));
    ASSERT_STREQ("PP_NIL (0)(1)(2)", PP_STRINGIZE(PP_NIL PP_KILL((0)(1)(2), 0)));
    ASSERT_STREQ("PP_NIL (1)(2)",    PP_STRINGIZE(PP_NIL PP_KILL((0)(1)(2), 1)));
    ASSERT_STREQ("PP_NIL (2)",       PP_STRINGIZE(PP_NIL PP_KILL((0)(1)(2), 2)));
    ASSERT_STREQ("PP_NIL",           PP_STRINGIZE(PP_NIL PP_KILL((0)(1)(2), 3)));
}

TEST(TPreprocessorTest, Head)
{
    ASSERT_STREQ("0", PP_STRINGIZE(PP_HEAD((0))));
    ASSERT_STREQ("0", PP_STRINGIZE(PP_HEAD((0)(1))));
    ASSERT_STREQ("0", PP_STRINGIZE(PP_HEAD((0)(1)(2))));
}

TEST(TPreprocessorTest, Tail)
{
    ASSERT_STREQ("PP_NIL",        PP_STRINGIZE(PP_NIL PP_TAIL((0))));
    ASSERT_STREQ("PP_NIL (1)",    PP_STRINGIZE(PP_NIL PP_TAIL((0)(1))));
    ASSERT_STREQ("PP_NIL (1)(2)", PP_STRINGIZE(PP_NIL PP_TAIL((0)(1)(2))));
}

TEST(TPreprocessorTest, ForEach)
{
    ASSERT_STREQ(
        "\"Foo\" \"Bar\" \"Spam\" \"Ham\"",
        PP_STRINGIZE(PP_FOR_EACH(PP_STRINGIZE, (Foo)(Bar)(Spam)(Ham)))
    );
#define my_functor(x) +x+
    ASSERT_STREQ(
        "+1+ +2+ +3+",
        PP_STRINGIZE(PP_FOR_EACH(my_functor, (1)(2)(3)))
    );
#undef  my_functor
}

TEST(TPreprocessorTest, Element)
{
    ASSERT_EQ(1, PP_ELEMENT((1), 0));
    ASSERT_EQ(1, PP_ELEMENT((1)(2), 0));
    ASSERT_EQ(2, PP_ELEMENT((1)(2), 1));
    ASSERT_EQ(1, PP_ELEMENT((1)(2)(3), 0));
    ASSERT_EQ(2, PP_ELEMENT((1)(2)(3), 1));
    ASSERT_EQ(3, PP_ELEMENT((1)(2)(3), 2));
    ASSERT_EQ(1, PP_ELEMENT((1)(2)(3)(4), 0));
    ASSERT_EQ(2, PP_ELEMENT((1)(2)(3)(4), 1));
    ASSERT_EQ(3, PP_ELEMENT((1)(2)(3)(4), 2));
    ASSERT_EQ(4, PP_ELEMENT((1)(2)(3)(4), 3));
}

TEST(TPreprocessorTest, Conditional)
{
    ASSERT_EQ(1, PP_IF(PP_TRUE,  1, 2));
    ASSERT_EQ(2, PP_IF(PP_FALSE, 1, 2));
}

TEST(TPreprocessorTest, IsSequence)
{
    ASSERT_STREQ("PP_FALSE", PP_STRINGIZE(PP_IS_SEQUENCE( 0    )));
    ASSERT_STREQ("PP_TRUE",  PP_STRINGIZE(PP_IS_SEQUENCE((0)   )));
    ASSERT_STREQ("PP_TRUE",  PP_STRINGIZE(PP_IS_SEQUENCE((0)(0))));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

