#include "stdafx.h"
#include "framework.h"

#include <core/misc/format.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFormatTest, Nothing)
{
    EXPECT_EQ("abc", Format("a%nb%nc", 1, 2));
}

TEST(TFormatTest, Verbatim)
{
    EXPECT_EQ("", Format(""));
    EXPECT_EQ("test", Format("test"));
    EXPECT_EQ("%", Format("%%"));
    EXPECT_EQ("%hello%world%", Format("%%hello%%world%%"));
}

TEST(TFormatTest, MultipleArgs)
{
    EXPECT_EQ("2+2=4", Format("%v+%v=%v", 2, 2, 4));
}

TEST(TFormatTest, Strings)
{
    EXPECT_EQ("test", Format("%s", "test"));
    EXPECT_EQ("test", Format("%s", STRINGBUF("test")));
    EXPECT_EQ("test", Format("%s", Stroka("test")));

    EXPECT_EQ("   abc", Format("%6s", Stroka("abc")));
    EXPECT_EQ("abc   ", Format("%-6s", Stroka("abc")));
    EXPECT_EQ("       abc", Format("%10v", Stroka("abc")));
    EXPECT_EQ("abc       ", Format("%-10v", Stroka("abc")));
    EXPECT_EQ("abc", Format("%2s", Stroka("abc")));
    EXPECT_EQ("abc", Format("%-2s", Stroka("abc")));
    EXPECT_EQ("abc", Format("%0s", Stroka("abc")));
    EXPECT_EQ("abc", Format("%-0s", Stroka("abc")));
}

TEST(TFormatTest, Integers)
{
    EXPECT_EQ("123", Format("%d", 123));
    EXPECT_EQ("123", Format("%v", 123));

    EXPECT_EQ("2147483647", Format("%d", std::numeric_limits<i32>::max()));
    EXPECT_EQ("-2147483648", Format("%d", std::numeric_limits<i32>::min()));

    EXPECT_EQ("0", Format("%u", 0U));
    EXPECT_EQ("0", Format("%v", 0U));
    EXPECT_EQ("4294967295", Format("%u", std::numeric_limits<ui32>::max()));
    EXPECT_EQ("4294967295", Format("%v", std::numeric_limits<ui32>::max()));

    EXPECT_EQ("9223372036854775807", Format("%" PRId64, std::numeric_limits<i64>::max()));
    EXPECT_EQ("9223372036854775807", Format("%v", std::numeric_limits<i64>::max()));
    EXPECT_EQ("-9223372036854775808", Format("%" PRId64, std::numeric_limits<i64>::min()));
    EXPECT_EQ("-9223372036854775808", Format("%v", std::numeric_limits<i64>::min()));

    EXPECT_EQ("0", Format("%" PRIu64, 0ULL));
    EXPECT_EQ("0", Format("%v", 0ULL));
    EXPECT_EQ("18446744073709551615", Format("%" PRIu64, std::numeric_limits<ui64>::max()));
    EXPECT_EQ("18446744073709551615", Format("%v", std::numeric_limits<ui64>::max()));
}

TEST(TFormatTest, Floats)
{
    EXPECT_EQ("3.14", Format("%.2f", 3.1415F));
    EXPECT_EQ("3.14", Format("%.2v", 3.1415F));
    EXPECT_EQ("3.14", Format("%.2lf", 3.1415));
    EXPECT_EQ("3.14", Format("%.2v", 3.1415));
}

DECLARE_ENUM(EColor,
    (Red)
    (BlackAndWhite)
);

TEST(TFormatTest, Enum)
{
    EXPECT_EQ("Red", Format("%v", EColor(EColor::Red)));
    EXPECT_EQ("red", Format("%lv", EColor(EColor::Red)));

    EXPECT_EQ("BlackAndWhite", Format("%v", EColor(EColor::BlackAndWhite)));
    EXPECT_EQ("black_and_white", Format("%v", EColor(EColor::BlackAndWhite)));

    EXPECT_EQ("EColor(100)", Format("%v", EColor(100)));
}

TEST(TFormatTest, Bool)
{
    EXPECT_EQ("True", Format("%v", true));
    EXPECT_EQ("False", Format("%v", false));
    EXPECT_EQ("true", Format("%lv", true));
    EXPECT_EQ("false", Format("%lv", false));
}

TEST(TFormatTest, Quotes)
{
    EXPECT_EQ("\"True\"", Format("%Qv", true));
    EXPECT_EQ("'False'", Format("%qv", false));
}

TEST(TFormatTest, Nullable)
{
    EXPECT_EQ("1", Format("%v", MakeNullable<int>(1)));
    EXPECT_EQ("<null>", Format("%v", Null));
    EXPECT_EQ("<null>", Format("%v", TNullable<int>()));
    EXPECT_EQ("3.14", Format("%.2f", TNullable<double>(3.1415)));
}

TEST(TFormatTest, Pointers)
{
    // No idea if pointer format is standardized, check against Sprintf.
    auto p = reinterpret_cast<void*>(123);
    EXPECT_EQ(Sprintf("%p", reinterpret_cast<void*>(123)), Format("%p", p));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

