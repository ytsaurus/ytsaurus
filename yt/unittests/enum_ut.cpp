#include "../ytlib/misc/enum.h"

#include "framework/framework.h"

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
}

////////////////////////////////////////////////////////////////////////////////

TEST(TEnumTest, Basic)
{
    ASSERT_EQ(0, ESimple(ESimple::X).ToValue());
    ASSERT_EQ(1, ESimple(ESimple::Y).ToValue());
    ASSERT_EQ(2, ESimple(ESimple::Z).ToValue());

    ASSERT_EQ(0, EColor( ).ToValue());
    ASSERT_EQ(5, EColor(5).ToValue());

    ASSERT_EQ(10, EColor(EColor::Red  ).ToValue());
    ASSERT_EQ(20, EColor(EColor::Green).ToValue());
    ASSERT_EQ(30, EColor(EColor::Blue ).ToValue());
    ASSERT_EQ(31, EColor(EColor::Black).ToValue());
    ASSERT_EQ(32, EColor(EColor::White).ToValue());
}

TEST(TEnumTest, ToString)
{
    ASSERT_EQ("EColor(0)", EColor( ).ToString());
    ASSERT_EQ("EColor(5)", EColor(5).ToString());

    ASSERT_EQ("Red",   EColor(EColor::Red  ).ToString());
    ASSERT_EQ("Green", EColor(EColor::Green).ToString());
    ASSERT_EQ("Blue",  EColor(EColor::Blue ).ToString());
    ASSERT_EQ("Black", EColor(EColor::Black).ToString());
    ASSERT_EQ("White", EColor(EColor::White).ToString());
}

TEST(TEnumTest, FromString)
{
    ASSERT_EQ(EColor::Red  , EColor::FromString("Red"  ));
    ASSERT_EQ(EColor::Green, EColor::FromString("Green"));
    ASSERT_EQ(EColor::Blue , EColor::FromString("Blue" ));
    ASSERT_EQ(EColor::Black, EColor::FromString("Black"));
    ASSERT_EQ(EColor::White, EColor::FromString("White"));

    ASSERT_THROW(EColor::FromString("Pink"), yexception);

    EColor color;
    bool returnValue;

    returnValue = EColor::FromString("Red", &color);
    ASSERT_EQ(EColor::Red, color);
    ASSERT_EQ(true, returnValue);

    returnValue = EColor::FromString("Pink", &color);
    ASSERT_EQ(EColor::Red, color);
    ASSERT_EQ(false, returnValue);
}

TEST(TEnumTest, Derived)
{
    EMyBase first;
    EMyDerived second;
    EMyOtherDerived third;

    third = EMyOtherDerived::Jack;
    ASSERT_EQ(3, third.ToValue());
    ASSERT_EQ("Jack", third.ToString());
    third = EMyBase::Chip;
    ASSERT_EQ(1, third.ToValue());
    ASSERT_EQ("Chip", third.ToString());

    second = EMyDerived::Dale;
    ASSERT_EQ(2, second.ToValue());
    ASSERT_EQ("Dale", second.ToString());
    second = EMyBase::Chip;
    ASSERT_EQ(1, second.ToValue());
    ASSERT_EQ("Chip", second.ToString());

    first = EMyOtherDerived::Jack;
    ASSERT_EQ(3, first.ToValue());
    ASSERT_EQ("EMyBase(3)", first.ToString());
    first = EMyDerived::Dale;
    ASSERT_EQ(2, first.ToValue());
    ASSERT_EQ("EMyBase(2)", first.ToString());
    first = EMyBase::Chip;
    ASSERT_EQ(1, first.ToValue());
    ASSERT_EQ("Chip", first.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

