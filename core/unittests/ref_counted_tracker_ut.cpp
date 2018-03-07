#include <yt/core/test_framework/framework.h>

#include <yt/core/unittests/proto/ref_counted_tracker_ut.pb.h>

#define YT_ENABLE_REF_COUNTED_TRACKING

#include <yt/core/misc/blob.h>
#include <yt/core/misc/new.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/ref_counted.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_tracked.h>

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

template <class T>
class TRefCountedTraits;

////////////////////////////////////////////////////////////////////////////////

class TSimpleRefCountedObject
    : public TRefCounted
{ };

template <>
class TRefCountedTraits<TSimpleRefCountedObject>
{
public:
    static TIntrusivePtr<TSimpleRefCountedObject> Create()
    {
        return New<TSimpleRefCountedObject>();
    }

    static size_t GetInstanceSize()
    {
        return sizeof(TSimpleRefCountedObject);
    }
};

////////////////////////////////////////////////////////////////////////////////

using TProtoRefCountedObject = TRefCountedProto<TRefCountedMessage>;

template <>
class TRefCountedTraits<TProtoRefCountedObject>
{
public:
    static TIntrusivePtr<TProtoRefCountedObject> Create()
    {
        static auto message = CreateMessage();
        return New<TProtoRefCountedObject>(message);
    }

    static size_t GetInstanceSize()
    {
        static auto message = CreateMessage();
        return message.SpaceUsed() - sizeof(TRefCountedMessage) + sizeof(TProtoRefCountedObject);
    }

private:
    static TRefCountedMessage CreateMessage()
    {
        TRefCountedMessage message;
        message.set_a("string");
        message.mutable_c()->set_a(10);
        message.add_d()->set_a(1);
        message.add_d()->set_a(2);
        message.add_d()->set_a(3);

        return message;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TRefCountedTrackerTest
    : public ::testing::Test
{ };

typedef ::testing::Types<TSimpleRefCountedObject, TProtoRefCountedObject> TypeList;
TYPED_TEST_CASE(TRefCountedTrackerTest, TypeList);

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TRefCountedTrackerTest, SinglethreadedRefCounted)
{
    const auto instanceSize = TRefCountedTraits<TypeParam>::GetInstanceSize();
    auto create = [] () {
        return TRefCountedTraits<TypeParam>::Create();
    };

    auto countBase = GetAllocatedCount<TypeParam>();
    auto bytesBase = GetAllocatedBytes<TypeParam>();

    std::vector<TIntrusivePtr<TypeParam>> container;
    container.reserve(2000);

    EXPECT_EQ(0, GetAliveCount<TypeParam>());
    EXPECT_EQ(0, GetAliveBytes<TypeParam>());
    EXPECT_EQ(countBase, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(bytesBase, GetAllocatedBytes<TypeParam>());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(create());
    }

    EXPECT_EQ(1000, GetAliveCount<TypeParam>());
    EXPECT_EQ(1000 * instanceSize, GetAliveBytes<TypeParam>());
    EXPECT_EQ(countBase + 1000, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(bytesBase + 1000 * instanceSize, GetAllocatedBytes<TypeParam>());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(create());
    }

    EXPECT_EQ(2000, GetAliveCount<TypeParam>());
    EXPECT_EQ(countBase + 2000, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(2000 * instanceSize, GetAliveBytes<TypeParam>());
    EXPECT_EQ(bytesBase + 2000 * instanceSize, GetAllocatedBytes<TypeParam>());

    container.resize(1000);

    EXPECT_EQ(1000, GetAliveCount<TypeParam>());
    EXPECT_EQ(countBase + 2000, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(1000 * instanceSize, GetAliveBytes<TypeParam>());
    EXPECT_EQ(bytesBase + 2000 * instanceSize, GetAllocatedBytes<TypeParam>());

    container.resize(0);

    EXPECT_EQ(0, GetAliveCount<TypeParam>());
    EXPECT_EQ(countBase + 2000, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(0, GetAliveBytes<TypeParam>());
    EXPECT_EQ(bytesBase + 2000 * instanceSize, GetAllocatedBytes<TypeParam>());
}

TYPED_TEST(TRefCountedTrackerTest, MultithreadedRefCounted)
{
    const auto instanceSize = TRefCountedTraits<TypeParam>::GetInstanceSize();
    auto create = [] () {
        return TRefCountedTraits<TypeParam>::Create();
    };

    auto countBase = GetAllocatedCount<TypeParam>();
    auto bytesBase = GetAllocatedBytes<TypeParam>();

    auto obj1 = create();

    auto queue = New<TActionQueue>();
    BIND([&] () {
        auto obj2 = create();
        EXPECT_EQ(countBase + 2, GetAllocatedCount<TypeParam>());
        EXPECT_EQ(2, GetAliveCount<TypeParam>());
    })
        .AsyncVia(queue->GetInvoker())
        .Run()
        .Get();
    queue->Shutdown();

    EXPECT_EQ(countBase + 2, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(1, GetAliveCount<TypeParam>());
    EXPECT_EQ(bytesBase + 2 * instanceSize, GetAllocatedBytes<TypeParam>());
    EXPECT_EQ(instanceSize, GetAliveBytes<TypeParam>());
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
