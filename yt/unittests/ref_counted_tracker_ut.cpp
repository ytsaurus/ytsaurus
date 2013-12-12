#include "stdafx.h"
#include "framework.h"

#define ENABLE_REF_COUNTED_TRACKING

#include <core/misc/common.h>
#include <core/misc/ref_counted_tracker.h>
#include <core/misc/ref_counted.h>
#include <core/misc/new.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSimpleObject
    : public TRefCounted
{
    ui32 Foo;
    ui32 Bar;

public:
    static i64 GetAliveCount()
    {
        return TRefCountedTracker::Get()->GetObjectsAlive(&typeid(TSimpleObject));
    }

    static i64 GetAllocatedCount()
    {
        return TRefCountedTracker::Get()->GetObjectsAllocated(&typeid(TSimpleObject));
    }
};

typedef TIntrusivePtr<TSimpleObject> TSimpleObjectPtr;

////////////////////////////////////////////////////////////////////////////////

TEST(TRefCountedTrackerTest, Simple)
{
    std::vector<TSimpleObjectPtr> container;
    container.reserve(2000);

    EXPECT_EQ(   0, TSimpleObject::GetAliveCount());
    EXPECT_EQ(   0, TSimpleObject::GetAllocatedCount());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(New<TSimpleObject>());
    }

    EXPECT_EQ(1000, TSimpleObject::GetAliveCount());
    EXPECT_EQ(1000, TSimpleObject::GetAllocatedCount());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(New<TSimpleObject>());
    }

    EXPECT_EQ(2000, TSimpleObject::GetAliveCount());
    EXPECT_EQ(2000, TSimpleObject::GetAllocatedCount());

    container.resize(1000);

    EXPECT_EQ(1000, TSimpleObject::GetAliveCount());
    EXPECT_EQ(2000, TSimpleObject::GetAllocatedCount());

    container.resize(0);

    EXPECT_EQ(   0, TSimpleObject::GetAliveCount());
    EXPECT_EQ(2000, TSimpleObject::GetAllocatedCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
