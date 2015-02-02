#include "stdafx.h"
#include "framework.h"

#define YT_ENABLE_REF_COUNTED_TRACKING

#include <core/misc/public.h>
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

class TBlobTag
{
public:
    static i64 GetAliveObjectsCount()
    {
        return TRefCountedTracker::Get()->GetObjectsAlive(GetRefCountedTypeKey<TBlobTag>());
    }

    static i64 GetAllocatedObjectsCount()
    {
        return TRefCountedTracker::Get()->GetObjectsAllocated(GetRefCountedTypeKey<TBlobTag>());
    }

    static i64 GetAliveBytesCount()
    {
        return TRefCountedTracker::Get()->GetAliveBytes(GetRefCountedTypeKey<TBlobTag>());
    }

    static i64 GetAllocatedBytesCount()
    {
        return TRefCountedTracker::Get()->GetAllocatedBytes(GetRefCountedTypeKey<TBlobTag>());
    }
};

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

TEST(TRefCountedTrackerTest, TBlobAllocatedMemoryTracker)
{
    auto allocatedBytesBase = TBlobTag::GetAllocatedBytesCount();
    auto allocatedObjectsBase = TBlobTag::GetAllocatedObjectsCount();

    EXPECT_EQ(0, TBlobTag::GetAliveBytesCount());
    EXPECT_EQ(0, TBlobTag::GetAliveObjectsCount());
    EXPECT_EQ(allocatedBytesBase, TBlobTag::GetAllocatedBytesCount());
    EXPECT_EQ(allocatedObjectsBase, TBlobTag::GetAllocatedObjectsCount());

    auto blob = TBlob(TBlobTag(), 1);
    auto blobCapacity1 = blob.Capacity();

    EXPECT_EQ(blobCapacity1, TBlobTag::GetAliveBytesCount());
    EXPECT_EQ(1, TBlobTag::GetAliveObjectsCount());
    EXPECT_EQ(allocatedBytesBase + blobCapacity1, TBlobTag::GetAllocatedBytesCount());
    EXPECT_EQ(allocatedObjectsBase + 1, TBlobTag::GetAllocatedObjectsCount());

    blob.Resize(3000);
    auto blobCapacity2 = blob.Capacity();

    EXPECT_EQ(blobCapacity2, TBlobTag::GetAliveBytesCount());
    EXPECT_EQ(1, TBlobTag::GetAliveObjectsCount());
    EXPECT_EQ(allocatedBytesBase + blobCapacity1 + blobCapacity2, TBlobTag::GetAllocatedBytesCount());
    EXPECT_EQ(allocatedObjectsBase + 1, TBlobTag::GetAllocatedObjectsCount());

    blob = TBlob(TBlobTag());

    EXPECT_EQ(0, TBlobTag::GetAliveBytesCount());
    EXPECT_EQ(0, TBlobTag::GetAliveObjectsCount());
    EXPECT_EQ(allocatedBytesBase + blobCapacity1 + blobCapacity2, TBlobTag::GetAllocatedBytesCount());
    EXPECT_EQ(allocatedObjectsBase + 1, TBlobTag::GetAllocatedObjectsCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
