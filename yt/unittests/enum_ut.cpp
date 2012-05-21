#include "stdafx.h"

#include <ytlib/misc/common.h>

#include <util/stream/buffer.h>
#include <util/ysaveload.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {
    DECLARE_ENUM(ESimple, (X)(Y)(Z));
    DECLARE_ENUM(EColor,
        ((Red)  (10))
        ((Green)(20))
        ((Blue) (30))
         (Black)
         (White)
    );
} // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TEnumTest, Basic)
{
    EXPECT_EQ(0, ESimple(ESimple::X).ToValue());
    EXPECT_EQ(1, ESimple(ESimple::Y).ToValue());
    EXPECT_EQ(2, ESimple(ESimple::Z).ToValue());

    EXPECT_EQ(0, EColor( ).ToValue());
    EXPECT_EQ(5, EColor(5).ToValue());

    EXPECT_EQ(10, EColor(EColor::Red  ).ToValue());
    EXPECT_EQ(20, EColor(EColor::Green).ToValue());
    EXPECT_EQ(30, EColor(EColor::Blue ).ToValue());
    EXPECT_EQ(31, EColor(EColor::Black).ToValue());
    EXPECT_EQ(32, EColor(EColor::White).ToValue());
}

TEST(TEnumTest, ToString)
{
    EXPECT_EQ("EColor(0)", EColor( ).ToString());
    EXPECT_EQ("EColor(5)", EColor(5).ToString());

    EXPECT_EQ("Red",   EColor(EColor::Red  ).ToString());
    EXPECT_EQ("Green", EColor(EColor::Green).ToString());
    EXPECT_EQ("Blue",  EColor(EColor::Blue ).ToString());
    EXPECT_EQ("Black", EColor(EColor::Black).ToString());
    EXPECT_EQ("White", EColor(EColor::White).ToString());
}

TEST(TEnumTest, FromString)
{
    EXPECT_EQ(EColor::Red  , EColor::FromString("Red"  ));
    EXPECT_EQ(EColor::Green, EColor::FromString("Green"));
    EXPECT_EQ(EColor::Blue , EColor::FromString("Blue" ));
    EXPECT_EQ(EColor::Black, EColor::FromString("Black"));
    EXPECT_EQ(EColor::White, EColor::FromString("White"));

    EXPECT_THROW(EColor::FromString("Pink"), yexception);

    EColor color;
    bool returnValue;

    returnValue = EColor::FromString("Red", &color);
    EXPECT_EQ(EColor::Red, color);
    EXPECT_IS_TRUE(returnValue);

    returnValue = EColor::FromString("Pink", &color);
    EXPECT_EQ(EColor::Red, color);
    EXPECT_IS_FALSE(returnValue);
}

TEST(TEnumTest, Ordering)
{
    ESimple a(ESimple::X);
    ESimple b(ESimple::Y);
    ESimple c(ESimple::Y);
    ESimple d(ESimple::Z);

    EXPECT_IS_FALSE(a < a); EXPECT_IS_FALSE(a > a);
    EXPECT_IS_TRUE (a < b); EXPECT_IS_TRUE (b > a);
    EXPECT_IS_TRUE (a < c); EXPECT_IS_TRUE (c > a);
    EXPECT_IS_TRUE (a < d); EXPECT_IS_TRUE (d > a);

    EXPECT_IS_FALSE(b < a); EXPECT_IS_FALSE(a > b);
    EXPECT_IS_FALSE(b < b); EXPECT_IS_FALSE(b > b);
    EXPECT_IS_FALSE(b < c); EXPECT_IS_FALSE(c > b);
    EXPECT_IS_TRUE (b < d); EXPECT_IS_TRUE (d > b);

    EXPECT_IS_FALSE(c < a); EXPECT_IS_FALSE(a > c);
    EXPECT_IS_FALSE(c < b); EXPECT_IS_FALSE(b > c);
    EXPECT_IS_FALSE(c < c); EXPECT_IS_FALSE(c > c);
    EXPECT_IS_TRUE (c < d); EXPECT_IS_TRUE (d > c);

    EXPECT_IS_FALSE(d < a); EXPECT_IS_FALSE(a > d);
    EXPECT_IS_FALSE(d < b); EXPECT_IS_FALSE(b > d);
    EXPECT_IS_FALSE(d < c); EXPECT_IS_FALSE(c > d);
    EXPECT_IS_FALSE(d < d); EXPECT_IS_FALSE(d > d);

    EXPECT_IS_TRUE (a <= b);
    EXPECT_IS_TRUE (b <= c);
    EXPECT_IS_TRUE (c <= d);

    EXPECT_IS_TRUE (a == a);
    EXPECT_IS_FALSE(a == b);
    EXPECT_IS_TRUE (b == c);
    EXPECT_IS_FALSE(c == d);
    EXPECT_IS_FALSE(d == a);

    EXPECT_IS_FALSE(a != a);
    EXPECT_IS_TRUE (a != b);
    EXPECT_IS_FALSE(b != c);
    EXPECT_IS_TRUE (c != d);
    EXPECT_IS_TRUE (d != a);
}

TEST(TEnumTest, OrderingWithDomainValues)
{
    EColor color(EColor::Black);

    EXPECT_LT(EColor::Red, color);
    EXPECT_LT(color, EColor::White);

    EXPECT_GT(color, EColor::Red);
    EXPECT_GT(EColor::White, color);

    EXPECT_LE(EColor::Red, color);
    EXPECT_LE(color, EColor::White);

    EXPECT_GE(EColor::White, color);
    EXPECT_GE(color, EColor::Red);

    EXPECT_EQ(color, EColor::Black);
    EXPECT_EQ(EColor::Black, color);

    EXPECT_NE(color, EColor::Blue);
    EXPECT_NE(EColor::Blue, color);
}

TEST(TEnumTest, SaveAndLoad)
{
    TStringStream stream;

    EColor first(EColor::Red);
    EColor second(EColor::Black);
    EColor third(0);
    EColor fourth(0);

    ::Save(&stream, first);
    ::Save(&stream, second);

    ::Load(&stream, third);
    ::Load(&stream, fourth);

    EXPECT_EQ(first, third);
    EXPECT_EQ(second, fourth);
}

TEST(TEnumTest, DomainSize)
{
    EXPECT_EQ(3, ESimple::GetDomainSize());
    EXPECT_EQ(5, EColor::GetDomainSize());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

