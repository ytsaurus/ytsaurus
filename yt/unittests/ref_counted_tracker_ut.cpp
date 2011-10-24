#define ENABLE_REF_COUNTED_TRACKING

#include "../ytlib/misc/ptr.h"
#include "../ytlib/misc/ref_counted_tracker.h"

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSimpleObject
    : public NYT::TRefCountedBase
{
    ui32 Foo;
    ui32 Bar;

public:
    typedef TIntrusivePtr<TSimpleObject> TPtr;

    static i64 GetAliveCount()
    {
        return TRefCountedTracker::GetAliveObjects(&typeid(TSimpleObject));
    }

    static i64 GetTotalCount()
    {
        return TRefCountedTracker::GetCreatedObjects(&typeid(TSimpleObject));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TRefCountedTrackerTest, Simple)
{
    yvector<TSimpleObject::TPtr> container;
    container.reserve(2000);

    EXPECT_EQ(   0, TSimpleObject::GetAliveCount());
    EXPECT_EQ(   0, TSimpleObject::GetTotalCount());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(New<TSimpleObject>());
    }

    EXPECT_EQ(1000, TSimpleObject::GetAliveCount());
    EXPECT_EQ(1000, TSimpleObject::GetTotalCount());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(New<TSimpleObject>());
    }

    EXPECT_EQ(2000, TSimpleObject::GetAliveCount());
    EXPECT_EQ(2000, TSimpleObject::GetTotalCount());

    container.resize(1000);

    EXPECT_EQ(1000, TSimpleObject::GetAliveCount());
    EXPECT_EQ(2000, TSimpleObject::GetTotalCount());

    container.resize(0);

    EXPECT_EQ(   0, TSimpleObject::GetAliveCount());
    EXPECT_EQ(2000, TSimpleObject::GetTotalCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

