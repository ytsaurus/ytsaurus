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

// Mix-in implicit constructor to allow implicit conversions.
BEGIN_DECLARE_ENUM(EMyBase,
    ((Chip)(1))
)
    MIXIN_ENUM__IMPLICIT_INT_CONSTRUCTOR(EMyBase)
END_DECLARE_ENUM();

// By default, explicit constructor is prefered.
BEGIN_DECLARE_DERIVED_ENUM(EMyBase, EMyDerived,
    ((Dale)(2))
)
    MIXIN_ENUM__EXPLICIT_INT_CONSTRUCTOR(EMyDerived)
END_DECLARE_ENUM();

DECLARE_DERIVED_ENUM(EMyBase, EMyOtherDerived, ((Jack)(3)));

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

TEST(TEnumTest, Derived)
{
    EMyBase first;
    EMyDerived second;
    EMyOtherDerived third;

    third = EMyOtherDerived::Jack;
    EXPECT_EQ(3, third.ToValue());
    EXPECT_EQ("Jack", third.ToString());
    third = EMyBase::Chip;
    EXPECT_EQ(1, third.ToValue());
    EXPECT_EQ("Chip", third.ToString());

    second = EMyDerived::Dale;
    EXPECT_EQ(2, second.ToValue());
    EXPECT_EQ("Dale", second.ToString());
    second = EMyBase::Chip;
    EXPECT_EQ(1, second.ToValue());
    EXPECT_EQ("Chip", second.ToString());

    first = EMyOtherDerived::Jack;
    EXPECT_EQ(3, first.ToValue());
    EXPECT_EQ("EMyBase(3)", first.ToString());
    first = EMyDerived::Dale;
    EXPECT_EQ(2, first.ToValue());
    EXPECT_EQ("EMyBase(2)", first.ToString());
    first = EMyBase::Chip;
    EXPECT_EQ(1, first.ToValue());
    EXPECT_EQ("Chip", first.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

