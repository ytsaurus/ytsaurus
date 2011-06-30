#include "../ytlib/misc/enum.h"

#include <library/unittest/registar.h>

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

namespace NYT {

class TEnumTest
    : public TTestBase
{
    UNIT_TEST_SUITE(TEnumTest);
        UNIT_TEST(TestBasic);
        UNIT_TEST(TestToString);
        UNIT_TEST(TestFromString);
        UNIT_TEST(TestDerived);
    UNIT_TEST_SUITE_END();

public:
    void TestBasic()
    {
        UNIT_ASSERT_EQUAL(ESimple(ESimple::X).ToValue(), 0);
        UNIT_ASSERT_EQUAL(ESimple(ESimple::Y).ToValue(), 1);
        UNIT_ASSERT_EQUAL(ESimple(ESimple::Z).ToValue(), 2);

        UNIT_ASSERT_EQUAL(EColor( ).ToValue(), 0);
        UNIT_ASSERT_EQUAL(EColor(5).ToValue(), 5);

        UNIT_ASSERT_EQUAL(EColor(EColor::Red  ).ToValue(), 10);
        UNIT_ASSERT_EQUAL(EColor(EColor::Green).ToValue(), 20);
        UNIT_ASSERT_EQUAL(EColor(EColor::Blue ).ToValue(), 30);
        UNIT_ASSERT_EQUAL(EColor(EColor::Black).ToValue(), 31);
        UNIT_ASSERT_EQUAL(EColor(EColor::White).ToValue(), 32);
    }

    void TestToString()
    {
        UNIT_ASSERT_EQUAL(EColor( ).ToString(), "EColor(0)");
        UNIT_ASSERT_EQUAL(EColor(5).ToString(), "EColor(5)");

        UNIT_ASSERT_EQUAL(EColor(EColor::Red  ).ToString(), "Red");
        UNIT_ASSERT_EQUAL(EColor(EColor::Green).ToString(), "Green");
        UNIT_ASSERT_EQUAL(EColor(EColor::Blue ).ToString(), "Blue");
        UNIT_ASSERT_EQUAL(EColor(EColor::Black).ToString(), "Black");
        UNIT_ASSERT_EQUAL(EColor(EColor::White).ToString(), "White");
    }

    void TestFromString()
    {
        UNIT_ASSERT_EQUAL(EColor::FromString("Red"  ), EColor::Red  );
        UNIT_ASSERT_EQUAL(EColor::FromString("Green"), EColor::Green);
        UNIT_ASSERT_EQUAL(EColor::FromString("Blue" ), EColor::Blue );
        UNIT_ASSERT_EQUAL(EColor::FromString("Black"), EColor::Black);
        UNIT_ASSERT_EQUAL(EColor::FromString("White"), EColor::White);

        UNIT_ASSERT_EXCEPTION(EColor::FromString("RedBull"), yexception);

        EColor color;
        bool returnValue;

        returnValue = EColor::FromString("Red", &color);
        UNIT_ASSERT_EQUAL(color, EColor::Red);
        UNIT_ASSERT_EQUAL(returnValue, true);

        returnValue = EColor::FromString("RedBull", &color);
        UNIT_ASSERT_EQUAL(color, EColor::Red);
        UNIT_ASSERT_EQUAL(returnValue, false);
    }

    void TestDerived()
    {
        EMyBase first;
        EMyDerived second;
        EMyOtherDerived third;

        third = EMyOtherDerived::Jack;
        UNIT_ASSERT_EQUAL(third.ToValue(), 3);
        UNIT_ASSERT_EQUAL(third.ToString(), "Jack");
        third = EMyBase::Chip;
        UNIT_ASSERT_EQUAL(third.ToValue(), 1);
        UNIT_ASSERT_EQUAL(third.ToString(), "Chip");

        second = EMyDerived::Dale;
        UNIT_ASSERT_EQUAL(second.ToValue(), 2);
        UNIT_ASSERT_EQUAL(second.ToString(), "Dale");
        second = EMyBase::Chip;
        UNIT_ASSERT_EQUAL(second.ToValue(), 1);
        UNIT_ASSERT_EQUAL(second.ToString(), "Chip");

        first = EMyOtherDerived::Jack;
        UNIT_ASSERT_EQUAL(first.ToValue(), 3);
        UNIT_ASSERT_EQUAL(first.ToString(), "EMyBase(3)");
        first = EMyDerived::Dale;
        UNIT_ASSERT_EQUAL(first.ToValue(), 2);
        UNIT_ASSERT_EQUAL(first.ToString(), "EMyBase(2)");
        first = EMyBase::Chip;
        UNIT_ASSERT_EQUAL(first.ToValue(), 1);
        UNIT_ASSERT_EQUAL(first.ToString(), "Chip");
    }
};

UNIT_TEST_SUITE_REGISTRATION(TEnumTest);

} // namespace NYT
