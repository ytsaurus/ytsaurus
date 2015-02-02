#include "stdafx.h"
#include "framework.h"

#include <core/misc/public.h>
#include <core/misc/weak_ptr.h>
#include <core/misc/format.h>

#include <core/concurrency/event_count.h>

#include <util/system/thread.h>

#include <array>

namespace NYT {
namespace {

using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::InSequence;
using ::testing::MockFunction;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////
// Auxiliary types and functions.
////////////////////////////////////////////////////////////////////////////////

static int ConstructorShadowState = 0;
static int DestructorShadowState = 0;

std::unique_ptr<NConcurrency::TEvent> DeathEvent;

void ResetShadowState()
{
    ConstructorShadowState = 0;
    DestructorShadowState = 0;

    DeathEvent.reset(new NConcurrency::TEvent());
}

class TIntricateObject
    : public TExtrinsicRefCounted
{
public:
    TIntricateObject()
    {
        ++ConstructorShadowState;
    }

    virtual ~TIntricateObject()
    {
        ++DestructorShadowState;
    }

    // Prevent the counter from destruction by holding an additional
    // reference to the counter.
    void LockCounter()
    {
        GetRefCounter()->WeakRef();
    }

    // Release an additional reference to the reference counter acquired by
    // #LockCounter().
    void UnlockCounter()
    {
        GetRefCounter()->WeakUnref();
    }

private:
    // Explicitly non-copyable.
    TIntricateObject(const TIntricateObject&);
    TIntricateObject(TIntricateObject&&);
    TIntricateObject& operator=(const TIntricateObject&);
    TIntricateObject& operator=(TIntricateObject&&);
};

typedef TIntrusivePtr<TIntricateObject> TIntricateObjectPtr;
typedef TWeakPtr<TIntricateObject> TIntricateObjectWkPtr;

class TDerivedIntricateObject
    : public TIntricateObject
{
private:
    // Payload.
    std::array<char, 32> Payload;
};

typedef TIntrusivePtr<TDerivedIntricateObject> TDerivedIntricateObjectPtr;
typedef TWeakPtr<TDerivedIntricateObject> TDerivedIntricateObjectWkPtr;

MATCHER_P2(HasRefCounts, strongRefs, weakRefs,
    "The object has "
        + ::testing::PrintToString(strongRefs) + " strong and "
        + ::testing::PrintToString(weakRefs) + " weak references")
{
    UNUSED(result_listener);
    return
        arg.GetRefCounter()->GetRefCount() == strongRefs &&
        arg.GetRefCounter()->GetWeakRefCount() == weakRefs;
}

class TSlowlyDyingObject
    : public TExtrinsicRefCounted
{
public:
    TSlowlyDyingObject()
    {
        ++ConstructorShadowState;
    }

    virtual ~TSlowlyDyingObject()
    {
        ++DestructorShadowState;
        DeathEvent->Wait();
        ++DestructorShadowState;
    }
};

typedef TIntrusivePtr<TSlowlyDyingObject> TSlowlyDyingObjectPtr;
typedef TWeakPtr<TSlowlyDyingObject> TSlowlyDyingObjectWkPtr;

template <class T>
void PrintExtrinsicRefCounted(const T& arg, ::std::ostream* os)
{
    *os << Format("%v strong and %v weak references",
        arg.GetRefCounter()->GetRefCount(),
        arg.GetRefCounter()->GetWeakRefCount());
}

void PrintTo(const TIntricateObject& arg, ::std::ostream* os)
{
    PrintExtrinsicRefCounted(arg, os);
}

void PrintTo(const TSlowlyDyingObject& arg, ::std::ostream* os)
{
    PrintExtrinsicRefCounted(arg, os);
}

////////////////////////////////////////////////////////////////////////////////

class TWeakPtrTest
    : public ::testing::Test
{
public:
    virtual void SetUp()
    {
        ResetShadowState();
    }
};

TEST_F(TWeakPtrTest, Empty)
{
    TIntricateObjectWkPtr emptyPointer;
    EXPECT_EQ(TIntricateObjectPtr(), emptyPointer.Lock());
}

TEST_F(TWeakPtrTest, Basic)
{
    TIntricateObjectPtr object = New<TIntricateObject>();
    TIntricateObject* objectPtr = object.Get();

    EXPECT_THAT(*object, HasRefCounts(1, 1));

    {
        TIntricateObjectWkPtr ptr(objectPtr);
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        EXPECT_EQ(object, ptr.Lock());
    }

    EXPECT_THAT(*object, HasRefCounts(1, 1));

    {
        TIntricateObjectWkPtr ptr(object);
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        EXPECT_EQ(object, ptr.Lock());
    }

    EXPECT_THAT(*object, HasRefCounts(1, 1));

    object.Reset();

    EXPECT_EQ(1, ConstructorShadowState);
    EXPECT_EQ(1, DestructorShadowState);
}

TEST_F(TWeakPtrTest, ResetToNull)
{
    TIntricateObjectPtr object = New<TIntricateObject>();
    TIntricateObjectWkPtr ptr(object);

    EXPECT_THAT(*object, HasRefCounts(1, 2));
    EXPECT_EQ(object, ptr.Lock());

    ptr.Reset();

    EXPECT_THAT(*object, HasRefCounts(1, 1));
    EXPECT_EQ(TIntricateObjectPtr(), ptr.Lock());
}

TEST_F(TWeakPtrTest, ResetToOtherObject)
{
    TIntricateObjectPtr firstObject = New<TIntricateObject>();
    TIntricateObjectPtr secondObject = New<TIntricateObject>();

    {
        TIntricateObjectWkPtr ptr(firstObject);

        EXPECT_THAT(*firstObject, HasRefCounts(1, 2));
        EXPECT_THAT(*secondObject, HasRefCounts(1, 1));
        EXPECT_EQ(firstObject, ptr.Lock());

        ptr.Reset(secondObject);

        EXPECT_THAT(*firstObject, HasRefCounts(1, 1));
        EXPECT_THAT(*secondObject, HasRefCounts(1, 2));
        EXPECT_EQ(secondObject, ptr.Lock());
    }

    TIntricateObject* firstObjectPtr = firstObject.Get();
    TIntricateObject* secondObjectPtr = secondObject.Get();

    {
        TIntricateObjectWkPtr ptr(firstObjectPtr);

        EXPECT_THAT(*firstObject, HasRefCounts(1, 2));
        EXPECT_THAT(*secondObject, HasRefCounts(1, 1));
        EXPECT_EQ(firstObject, ptr.Lock());

        ptr.Reset(secondObjectPtr);

        EXPECT_THAT(*firstObject, HasRefCounts(1, 1));
        EXPECT_THAT(*secondObject, HasRefCounts(1, 2));
        EXPECT_EQ(secondObject, ptr.Lock());
    }
}

TEST_F(TWeakPtrTest, CopySemantics)
{
    TIntricateObjectPtr object = New<TIntricateObject>();
    TIntricateObjectWkPtr foo(object);

    {
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        TIntricateObjectWkPtr bar(foo);
        EXPECT_THAT(*object, HasRefCounts(1, 3));

        EXPECT_EQ(object, foo.Lock());
        EXPECT_EQ(object, bar.Lock());
    }

    {
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        TIntricateObjectWkPtr bar;
        bar = foo;
        EXPECT_THAT(*object, HasRefCounts(1, 3));

        EXPECT_EQ(object, foo.Lock());
        EXPECT_EQ(object, bar.Lock());
    }
}

TEST_F(TWeakPtrTest, MoveSemantics)
{
    TIntricateObjectPtr object = New<TIntricateObject>();
    TIntricateObjectWkPtr foo(object);

    {
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        TIntricateObjectWkPtr bar(std::move(foo));
        EXPECT_THAT(*object, HasRefCounts(1, 2));

        EXPECT_EQ(TIntricateObjectPtr(), foo.Lock());
        EXPECT_EQ(object, bar.Lock());
    }

    foo.Reset(object);

    {
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        TIntricateObjectWkPtr bar;
        bar = std::move(foo);
        EXPECT_THAT(*object, HasRefCounts(1, 2));

        EXPECT_EQ(TIntricateObjectPtr(), foo.Lock());
        EXPECT_EQ(object, bar.Lock());
    }
}

TEST_F(TWeakPtrTest, OutOfScope)
{
    TIntricateObjectWkPtr ptr;

    EXPECT_EQ(TIntricateObjectPtr(), ptr.Lock());
    {
        TIntricateObjectPtr object = New<TIntricateObject>();
        ptr = object;
        EXPECT_EQ(object, ptr.Lock());
    }
    EXPECT_EQ(TIntricateObjectPtr(), ptr.Lock());
}

TEST_F(TWeakPtrTest, OutOfNestedScope)
{
    TIntricateObjectWkPtr foo;

    EXPECT_EQ(TIntricateObjectPtr(), foo.Lock());
    {
        TIntricateObjectPtr object = New<TIntricateObject>();
        foo = object;

        EXPECT_EQ(object, foo.Lock());
        {
            TIntricateObjectWkPtr bar;
            bar = object;

            EXPECT_EQ(object, bar.Lock());
        }
        EXPECT_EQ(object, foo.Lock());
    }
    EXPECT_EQ(TIntricateObjectPtr(), foo.Lock());

    EXPECT_EQ(1, ConstructorShadowState);
    EXPECT_EQ(1, DestructorShadowState);
}

TEST_F(TWeakPtrTest, IsExpired)
{
    TIntricateObjectWkPtr ptr;

    EXPECT_TRUE(ptr.IsExpired());
    {
        TIntricateObjectPtr object = New<TIntricateObject>();
        ptr = object;
        EXPECT_FALSE(ptr.IsExpired());
    }
    EXPECT_TRUE(ptr.IsExpired());
}

TEST_F(TWeakPtrTest, UpCast)
{
    TDerivedIntricateObjectPtr object = New<TDerivedIntricateObject>();
    TIntricateObjectWkPtr ptr = object;

    EXPECT_EQ(object.Get(), ptr.Lock().Get());
}

static void* AsynchronousDeleter(void* param)
{
    TSlowlyDyingObjectPtr* indirectObject =
        reinterpret_cast<TSlowlyDyingObjectPtr*>(param);
    indirectObject->Reset();
    return NULL;
}

TEST_F(TWeakPtrTest, AcquisionOfSlowlyDyingObject)
{
    TSlowlyDyingObjectPtr object = New<TSlowlyDyingObject>();
    TSlowlyDyingObjectWkPtr ptr(object);

    TSlowlyDyingObject* objectPtr = object.Get();

    EXPECT_EQ(object, ptr.Lock());
    EXPECT_THAT(*objectPtr, HasRefCounts(1, 2));

    ASSERT_EQ(1, ConstructorShadowState);
    ASSERT_EQ(0, DestructorShadowState);

    // Kick off object deletion in the background.
    TThread thread(&AsynchronousDeleter, &object);
    thread.Start();
    Sleep(TDuration::Seconds(0.100));

    ASSERT_EQ(1, ConstructorShadowState);
    ASSERT_EQ(1, DestructorShadowState);

    EXPECT_EQ(TSlowlyDyingObjectPtr(), ptr.Lock());
    EXPECT_THAT(*objectPtr, HasRefCounts(0, 2));

    // Finalize object destruction.
    DeathEvent->NotifyAll();
    thread.Join();

    ASSERT_EQ(1, ConstructorShadowState);
    ASSERT_EQ(2, DestructorShadowState);

    EXPECT_EQ(TSlowlyDyingObjectPtr(), ptr.Lock());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
