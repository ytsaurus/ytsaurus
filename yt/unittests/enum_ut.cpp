#include "../ytlib/misc/enum.h"

#include "framework/framework.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ESimple, (X)(Y)(Z));
DECLARE_ENUM(EColor,
    ((Red)  (10))
    ((Green)(20))
    ((Blue) (30))
     (Black)
     (White)
);

DECLARE_POLY_ENUM1(EMyFirst, ((Chip)(1)));
DECLARE_POLY_ENUM2(EMyFirst, EMySecond, ((Dale)(2)));

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

    EXPECT_EQ(0, EMyFirst().ToValue());
    EXPECT_EQ(1, EMyFirst(EMyFirst::Chip).ToValue());
    EXPECT_EQ(2, EMyFirst(2).ToValue());
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

    EXPECT_EQ("EMyFirst(0)", EMyFirst().ToString());
    EXPECT_EQ("Chip", EMyFirst(EMyFirst::Chip).ToString());
    EXPECT_EQ("Foo", EMyFirst(2, "Foo").ToString());
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

TEST(TEnumTest, Polymorphism1)
{
    EMyFirst first(EMyFirst::Chip);
    EMySecond second(first);

    EXPECT_EQ("Chip", first.ToString());
    EXPECT_EQ("Chip", second.ToString());

    second = EMySecond::Dale;
    first = second;

    EXPECT_EQ("Dale", first.ToString());
    EXPECT_EQ("Dale", second.ToString());
}

TEST(TEnumTest, Polymorphism2)
{
    EMyFirst first(2);
    EMySecond second(first);

    EXPECT_EQ("EMyFirst(2)", first.ToString());
    EXPECT_EQ("Dale", second.ToString());

    second = EMySecond(1);
    first = second;

    EXPECT_EQ("Chip", first.ToString());
    EXPECT_EQ("EMySecond(1)", second.ToString());
}

TEST(TEnumTest, Polymorphism3)
{
    EMyFirst first(17);
    EMySecond second(first);

    EXPECT_EQ("EMyFirst(17)", first.ToString());
    EXPECT_EQ("EMySecond(17)", second.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

