#define ENABLE_REF_COUNTED_TRACKING

#include "../ytlib/misc/ptr.h"
#include "../ytlib/misc/ref_counted_tracker.h"

#include <library/unittest/registar.h>

namespace NYT {

class TRefCountedTrackerTest
    : public TTestBase
{
    UNIT_TEST_SUITE(TRefCountedTrackerTest);
        UNIT_TEST(TestSimple);
    UNIT_TEST_SUITE_END();

private:
    class TSimpleObject : public NYT::TRefCountedBase
    {
        ui32 Foo;
        ui32 Bar;

    public:
        typedef TIntrusivePtr<TSimpleObject> TPtr;
    };

public:
    void TestSimple()
    {
        yvector<TSimpleObject::TPtr> container;
        container.reserve(2000);

        for (size_t i = 0; i < 1000; ++i) {
            container.push_back(new TSimpleObject());
        }

        UNIT_ASSERT_EQUAL(TRefCountedTracker::Get()->GetAliveObjects(typeid(TSimpleObject)), 1000);
        UNIT_ASSERT_EQUAL(TRefCountedTracker::Get()->GetTotalObjects(typeid(TSimpleObject)), 1000);

        for (size_t i = 0; i < 1000; ++i) {
            container.push_back(new TSimpleObject());
        }

        UNIT_ASSERT_EQUAL(TRefCountedTracker::Get()->GetAliveObjects(typeid(TSimpleObject)), 2000);
        UNIT_ASSERT_EQUAL(TRefCountedTracker::Get()->GetTotalObjects(typeid(TSimpleObject)), 2000);

        container.resize(1000);

        UNIT_ASSERT_EQUAL(TRefCountedTracker::Get()->GetAliveObjects(typeid(TSimpleObject)), 1000);
        UNIT_ASSERT_EQUAL(TRefCountedTracker::Get()->GetTotalObjects(typeid(TSimpleObject)), 2000);

        container.resize(0);

        UNIT_ASSERT_EQUAL(TRefCountedTracker::Get()->GetAliveObjects(typeid(TSimpleObject)),    0);
        UNIT_ASSERT_EQUAL(TRefCountedTracker::Get()->GetTotalObjects(typeid(TSimpleObject)), 2000);
    }
};

UNIT_TEST_SUITE_REGISTRATION(TRefCountedTrackerTest);

} // namespace NYT

