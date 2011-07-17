#define ENABLE_REF_COUNTED_TRACKING

#include "../ytlib/misc/ptr.h"
#include "../ytlib/misc/ref_counted_tracker.h"

#include "framework/framework.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {
    class TSimpleObject : public NYT::TRefCountedBase
    {
        ui32 Foo;
        ui32 Bar;

    public:
        typedef TIntrusivePtr<TSimpleObject> TPtr;

        static int GetAliveCount()
        {
            return TRefCountedTracker::Get()->
                GetAliveObjects(typeid(TSimpleObject));
        }

        static int GetTotalCount()
        {
            return TRefCountedTracker::Get()->
                GetTotalObjects(typeid(TSimpleObject));
        }
    };
}

////////////////////////////////////////////////////////////////////////////////

TEST(TRefCountedTrackerTest, Simple)
{
    yvector<TSimpleObject::TPtr> container;
    container.reserve(2000);

    ASSERT_EQ(   0, TSimpleObject::GetAliveCount());
    ASSERT_EQ(   0, TSimpleObject::GetTotalCount());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(new TSimpleObject());
    }

    ASSERT_EQ(1000, TSimpleObject::GetAliveCount());
    ASSERT_EQ(1000, TSimpleObject::GetTotalCount());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(new TSimpleObject());
    }

    ASSERT_EQ(2000, TSimpleObject::GetAliveCount());
    ASSERT_EQ(2000, TSimpleObject::GetTotalCount());

    container.resize(1000);

    ASSERT_EQ(1000, TSimpleObject::GetAliveCount());
    ASSERT_EQ(2000, TSimpleObject::GetTotalCount());

    container.resize(0);

    ASSERT_EQ(   0, TSimpleObject::GetAliveCount());
    ASSERT_EQ(2000, TSimpleObject::GetTotalCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

