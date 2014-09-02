#include "stdafx.h"
#include "framework.h"

#define YT_ENABLE_REF_COUNTED_TRACKING

#include <core/misc/common.h>
#include <core/misc/ref_counted_tracker.h>
#include <core/misc/ref_counted.h>
#include <core/misc/new.h>

#include <core/actions/future.h>

#include <core/concurrency/action_queue.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSimpleObject
    : public TRefCounted
{
public:
    static i64 GetAliveCount()
    {
        return TRefCountedTracker::Get()->GetObjectsAlive(GetRefCountedTypeKey<TSimpleObject>());
    }

    static i64 GetAllocatedCount()
    {
        return TRefCountedTracker::Get()->GetObjectsAllocated(GetRefCountedTypeKey<TSimpleObject>());
    }
};

typedef TIntrusivePtr<TSimpleObject> TSimpleObjectPtr;

////////////////////////////////////////////////////////////////////////////////

TEST(TRefCountedTrackerTest, Singlethreaded)
{
    auto allocatedBase = TSimpleObject::GetAllocatedCount();

    std::vector<TSimpleObjectPtr> container;
    container.reserve(2000);

    EXPECT_EQ(0, TSimpleObject::GetAliveCount());
    EXPECT_EQ(allocatedBase, TSimpleObject::GetAllocatedCount());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(New<TSimpleObject>());
    }

    EXPECT_EQ(1000, TSimpleObject::GetAliveCount());
    EXPECT_EQ(allocatedBase + 1000, TSimpleObject::GetAllocatedCount());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(New<TSimpleObject>());
    }

    EXPECT_EQ(2000, TSimpleObject::GetAliveCount());
    EXPECT_EQ(allocatedBase + 2000, TSimpleObject::GetAllocatedCount());

    container.resize(1000);

    EXPECT_EQ(1000, TSimpleObject::GetAliveCount());
    EXPECT_EQ(allocatedBase + 2000, TSimpleObject::GetAllocatedCount());

    container.resize(0);

    EXPECT_EQ(0, TSimpleObject::GetAliveCount());
    EXPECT_EQ(allocatedBase + 2000, TSimpleObject::GetAllocatedCount());
}

TEST(TRefCountedTrackerTest, Multithreaded)
{
    auto allocatedBase = TSimpleObject::GetAllocatedCount();

    auto obj1 = New<TSimpleObject>();

    auto queue = New<TActionQueue>();
    BIND([&] () {
        auto obj2 = New<TSimpleObject>();
        EXPECT_EQ(allocatedBase + 2, TSimpleObject::GetAllocatedCount());
        EXPECT_EQ(2, TSimpleObject::GetAliveCount());
    })
        .AsyncVia(queue->GetInvoker())
        .Run()
        .Get();
    queue->Shutdown();

    EXPECT_EQ(allocatedBase + 2, TSimpleObject::GetAllocatedCount());
    EXPECT_EQ(1, TSimpleObject::GetAliveCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
