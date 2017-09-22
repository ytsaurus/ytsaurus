#include <yt/core/test_framework/framework.h>

#define YT_ENABLE_REF_COUNTED_TRACKING

#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/new.h>
#include <yt/core/misc/blob.h>

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/action_queue.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

template <class T>
size_t GetAliveCount()
{
    return TRefCountedTracker::Get()->GetInstancesAlive(GetRefCountedTypeKey<T>());
}

template <class T>
size_t GetAliveBytes()
{
    return TRefCountedTracker::Get()->GetBytesAlive(GetRefCountedTypeKey<T>());
}

template <class T>
size_t GetAllocatedCount()
{
    return TRefCountedTracker::Get()->GetInstancesAllocated(GetRefCountedTypeKey<T>());
}

template <class T>
size_t GetAllocatedBytes()
{
    return TRefCountedTracker::Get()->GetBytesAllocated(GetRefCountedTypeKey<T>());
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleRefCountedObject
    : public TRefCounted
{ };

TEST(TRefCountedTrackerTest, SinglethreadedRefCounted)
{
    auto countBase = GetAllocatedCount<TSimpleRefCountedObject>();
    auto bytesBase = GetAllocatedBytes<TSimpleRefCountedObject>();

    std::vector<TIntrusivePtr<TSimpleRefCountedObject>> container;
    container.reserve(2000);

    EXPECT_EQ(0, GetAliveCount<TSimpleRefCountedObject>());
    EXPECT_EQ(0, GetAliveBytes<TSimpleRefCountedObject>());
    EXPECT_EQ(countBase, GetAllocatedCount<TSimpleRefCountedObject>());
    EXPECT_EQ(bytesBase, GetAllocatedBytes<TSimpleRefCountedObject>());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(New<TSimpleRefCountedObject>());
    }

    EXPECT_EQ(1000, GetAliveCount<TSimpleRefCountedObject>());
    EXPECT_EQ(1000 * sizeof(TSimpleRefCountedObject), GetAliveBytes<TSimpleRefCountedObject>());
    EXPECT_EQ(countBase + 1000, GetAllocatedCount<TSimpleRefCountedObject>());
    EXPECT_EQ(bytesBase + 1000 * sizeof(TSimpleRefCountedObject), GetAllocatedBytes<TSimpleRefCountedObject>());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(New<TSimpleRefCountedObject>());
    }

    EXPECT_EQ(2000, GetAliveCount<TSimpleRefCountedObject>());
    EXPECT_EQ(countBase + 2000, GetAllocatedCount<TSimpleRefCountedObject>());
    EXPECT_EQ(2000 * sizeof(TSimpleRefCountedObject), GetAliveBytes<TSimpleRefCountedObject>());
    EXPECT_EQ(bytesBase + 2000 * sizeof(TSimpleRefCountedObject), GetAllocatedBytes<TSimpleRefCountedObject>());

    container.resize(1000);

    EXPECT_EQ(1000, GetAliveCount<TSimpleRefCountedObject>());
    EXPECT_EQ(countBase + 2000, GetAllocatedCount<TSimpleRefCountedObject>());
    EXPECT_EQ(1000 * sizeof(TSimpleRefCountedObject), GetAliveBytes<TSimpleRefCountedObject>());
    EXPECT_EQ(bytesBase + 2000 * sizeof(TSimpleRefCountedObject), GetAllocatedBytes<TSimpleRefCountedObject>());

    container.resize(0);

    EXPECT_EQ(0, GetAliveCount<TSimpleRefCountedObject>());
    EXPECT_EQ(countBase + 2000, GetAllocatedCount<TSimpleRefCountedObject>());
    EXPECT_EQ(0, GetAliveBytes<TSimpleRefCountedObject>());
    EXPECT_EQ(bytesBase + 2000 * sizeof(TSimpleRefCountedObject), GetAllocatedBytes<TSimpleRefCountedObject>());
}

TEST(TRefCountedTrackerTest, MultithreadedRefCounted)
{
    auto countBase = GetAllocatedCount<TSimpleRefCountedObject>();
    auto bytesBase = GetAllocatedBytes<TSimpleRefCountedObject>();

    auto obj1 = New<TSimpleRefCountedObject>();

    auto queue = New<TActionQueue>();
    BIND([&] () {
        auto obj2 = New<TSimpleRefCountedObject>();
        EXPECT_EQ(countBase + 2, GetAllocatedCount<TSimpleRefCountedObject>());
        EXPECT_EQ(2, GetAliveCount<TSimpleRefCountedObject>());
    })
        .AsyncVia(queue->GetInvoker())
        .Run()
        .Get();
    queue->Shutdown();

    EXPECT_EQ(countBase + 2, GetAllocatedCount<TSimpleRefCountedObject>());
    EXPECT_EQ(1, GetAliveCount<TSimpleRefCountedObject>());
    EXPECT_EQ(bytesBase + 2 * sizeof(TSimpleRefCountedObject), GetAllocatedBytes<TSimpleRefCountedObject>());
    EXPECT_EQ(sizeof(TSimpleRefCountedObject), GetAliveBytes<TSimpleRefCountedObject>());
}

////////////////////////////////////////////////////////////////////////////////

struct TBlobTag
{ };

TEST(TRefCountedTrackerTest, TBlobAllocatedMemoryTracker)
{
    auto allocatedBytesBase = GetAllocatedBytes<TBlobTag>();
    auto allocatedObjectsBase = GetAllocatedCount<TBlobTag>();

    EXPECT_EQ(0, GetAliveBytes<TBlobTag>());
    EXPECT_EQ(0, GetAliveCount<TBlobTag>());
    EXPECT_EQ(allocatedBytesBase, GetAllocatedBytes<TBlobTag>());
    EXPECT_EQ(allocatedObjectsBase, GetAllocatedCount<TBlobTag>());

    auto blob = TBlob(TBlobTag(), 1);
    auto blobCapacity1 = blob.Capacity();

    EXPECT_EQ(blobCapacity1, GetAliveBytes<TBlobTag>());
    EXPECT_EQ(1, GetAliveCount<TBlobTag>());
    EXPECT_EQ(allocatedBytesBase + blobCapacity1, GetAllocatedBytes<TBlobTag>());
    EXPECT_EQ(allocatedObjectsBase + 1, GetAllocatedCount<TBlobTag>());

    blob.Resize(3000);
    auto blobCapacity2 = blob.Capacity();

    EXPECT_EQ(blobCapacity2, GetAliveBytes<TBlobTag>());
    EXPECT_EQ(1, GetAliveCount<TBlobTag>());
    EXPECT_EQ(allocatedBytesBase + blobCapacity1 + blobCapacity2, GetAllocatedBytes<TBlobTag>());
    EXPECT_EQ(allocatedObjectsBase + 1, GetAllocatedCount<TBlobTag>());

    blob = TBlob(TBlobTag());

    EXPECT_EQ(0, GetAliveBytes<TBlobTag>());
    EXPECT_EQ(0, GetAliveCount<TBlobTag>());
    EXPECT_EQ(allocatedBytesBase + blobCapacity1 + blobCapacity2, GetAllocatedBytes<TBlobTag>());
    EXPECT_EQ(allocatedObjectsBase + 1, GetAllocatedCount<TBlobTag>());
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TThrowingConstructorObject);
TThrowingConstructorObjectPtr GlobalObject;

class TThrowingConstructorObject
    : public TRefCounted
{
public:
    explicit TThrowingConstructorObject(bool passThisToSomebodyElse)
    {
        if (passThisToSomebodyElse) {
            GlobalObject = this;
        }
        THROW_ERROR_EXCEPTION("Some error");
    }
};

DEFINE_REFCOUNTED_TYPE(TThrowingConstructorObject);

////////////////////////////////////////////////////////////////////////////////

TEST(TRefCountedTrackerTest, ThrowingExceptionsInConstructor)
{
    TThrowingConstructorObjectPtr object;
    EXPECT_THROW(object = New<TThrowingConstructorObject>(false), std::exception);
    // TODO(max42): enable this when death tests are allowed in unittests.
    // ASSERT_DEATH(object = New<TThrowingConstructorObject>(true), "YCHECK\\(GetRefCount\\(\\) == 1\\).*");
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleRefTrackedObject
    : public TRefTracked<TSimpleRefTrackedObject>
{ };

TEST(TRefCountedTrackerTest, RefTracked)
{
    auto countBase = GetAllocatedCount<TSimpleRefTrackedObject>();
    auto bytesBase = GetAllocatedBytes<TSimpleRefTrackedObject>();

    {
        TSimpleRefTrackedObject obj;
        EXPECT_EQ(countBase + 1, GetAllocatedCount<TSimpleRefTrackedObject>());
        EXPECT_EQ(bytesBase + sizeof(TSimpleRefTrackedObject), GetAllocatedBytes<TSimpleRefTrackedObject>());
        EXPECT_EQ(1, GetAliveCount<TSimpleRefTrackedObject>());
        EXPECT_EQ(sizeof(TSimpleRefTrackedObject), GetAliveBytes<TSimpleRefTrackedObject>());
    }

    EXPECT_EQ(countBase + 1, GetAllocatedCount<TSimpleRefTrackedObject>());
    EXPECT_EQ(bytesBase + sizeof(TSimpleRefTrackedObject), GetAllocatedBytes<TSimpleRefTrackedObject>());
    EXPECT_EQ(0, GetAliveCount<TSimpleRefTrackedObject>());
    EXPECT_EQ(0, GetAliveBytes<TSimpleRefTrackedObject>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
