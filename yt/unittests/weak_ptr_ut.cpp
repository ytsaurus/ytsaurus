#include "stdafx.h"

#include <ytlib/misc/common.h>
#include <ytlib/misc/weak_ptr.h>

#include <util/system/thread.h>

#include <contrib/testing/framework.h>

using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::InSequence;
using ::testing::MockFunction;
using ::testing::StrictMock;

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////
// Auxiliary types and functions.
////////////////////////////////////////////////////////////////////////////////

static int ConstructorShadowState = 0;
static int DestructorShadowState = 0;

THolder<Event> DeathEvent;

void ResetShadowState()
{
    ConstructorShadowState = 0;
    DestructorShadowState = 0;

    DeathEvent.Reset(new Event());
}

class TIntricateObject
    : public TExtrinsicRefCounted
{
public:
    typedef TIntrusivePtr<TIntricateObject> TPtr;
    typedef TWeakPtr<TIntricateObject> TWkPtr;

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

class TDerivedIntricateObject
    : public TIntricateObject
{
public:
    typedef TIntrusivePtr<TDerivedIntricateObject> TPtr;
    typedef TWeakPtr<TDerivedIntricateObject> TWkPtr;

private:
    // Payload.
    char Payload[32];
};

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
    typedef TIntrusivePtr<TSlowlyDyingObject> TPtr;
    typedef TWeakPtr<TSlowlyDyingObject> TWkPtr;

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

template <class T>
void PrintExtrinsicRefCounted(const T& arg, ::std::ostream* os)
{
    Stroka repr = Sprintf(
        "%d strong and %d weak references",
        arg.GetRefCounter()->GetRefCount(),
        arg.GetRefCounter()->GetWeakRefCount());
    *os << repr.c_str();
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
    TIntricateObject::TWkPtr emptyPointer;
    EXPECT_EQ(TIntricateObject::TPtr(), emptyPointer.Lock());
}

TEST_F(TWeakPtrTest, Basic)
{
    TIntricateObject::TPtr object = New<TIntricateObject>();
    TIntricateObject* objectPtr = object.Get();

    EXPECT_THAT(*object, HasRefCounts(1, 1));

    {
        TIntricateObject::TWkPtr ptr(objectPtr);
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        EXPECT_EQ(object, ptr.Lock());
    }
    
    EXPECT_THAT(*object, HasRefCounts(1, 1));

    {
        TIntricateObject::TWkPtr ptr(object);
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
    TIntricateObject::TPtr object = New<TIntricateObject>();
    TIntricateObject::TWkPtr ptr(object);

    EXPECT_THAT(*object, HasRefCounts(1, 2));
    EXPECT_EQ(object, ptr.Lock());

    ptr.Reset();

    EXPECT_THAT(*object, HasRefCounts(1, 1));
    EXPECT_EQ(TIntricateObject::TPtr(), ptr.Lock());
}

TEST_F(TWeakPtrTest, ResetToOtherObject)
{
    TIntricateObject::TPtr firstObject = New<TIntricateObject>();
    TIntricateObject::TPtr secondObject = New<TIntricateObject>();

    {
        TIntricateObject::TWkPtr ptr(firstObject);

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
        TIntricateObject::TWkPtr ptr(firstObjectPtr);

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
    TIntricateObject::TPtr object = New<TIntricateObject>();
    TIntricateObject::TWkPtr foo(object);

    {
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        TIntricateObject::TWkPtr bar(foo);
        EXPECT_THAT(*object, HasRefCounts(1, 3));

        EXPECT_EQ(object, foo.Lock());
        EXPECT_EQ(object, bar.Lock());
    }

    {
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        TIntricateObject::TWkPtr bar;
        bar = foo;
        EXPECT_THAT(*object, HasRefCounts(1, 3));

        EXPECT_EQ(object, foo.Lock());
        EXPECT_EQ(object, bar.Lock());
    }
}

TEST_F(TWeakPtrTest, MoveSemantics)
{
    TIntricateObject::TPtr object = New<TIntricateObject>();
    TIntricateObject::TWkPtr foo(object);

    {
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        TIntricateObject::TWkPtr bar(MoveRV(foo));
        EXPECT_THAT(*object, HasRefCounts(1, 2));

        EXPECT_EQ(TIntricateObject::TPtr(), foo.Lock());
        EXPECT_EQ(object, bar.Lock());
    }

    foo.Reset(object);

    {
        EXPECT_THAT(*object, HasRefCounts(1, 2));
        TIntricateObject::TWkPtr bar;
        bar = MoveRV(foo);
        EXPECT_THAT(*object, HasRefCounts(1, 2));

        EXPECT_EQ(TIntricateObject::TPtr(), foo.Lock());
        EXPECT_EQ(object, bar.Lock());
    }
}

TEST_F(TWeakPtrTest, OutOfScope)
{
    TIntricateObject::TWkPtr ptr;

    EXPECT_EQ(TIntricateObject::TPtr(), ptr.Lock());
    {
        TIntricateObject::TPtr object = New<TIntricateObject>();
        ptr = object;
        EXPECT_EQ(object, ptr.Lock());
    }
    EXPECT_EQ(TIntricateObject::TPtr(), ptr.Lock());
}

TEST_F(TWeakPtrTest, OutOfNestedScope)
{
    TIntricateObject::TWkPtr foo;

    EXPECT_EQ(TIntricateObject::TPtr(), foo.Lock());
    {
        TIntricateObject::TPtr object = New<TIntricateObject>();
        foo = object;

        EXPECT_EQ(object, foo.Lock());
        {
            TIntricateObject::TWkPtr bar;
            bar = object;

            EXPECT_EQ(object, bar.Lock());
        }
        EXPECT_EQ(object, foo.Lock());
    }
    EXPECT_EQ(TIntricateObject::TPtr(), foo.Lock());

    EXPECT_EQ(1, ConstructorShadowState);
    EXPECT_EQ(1, DestructorShadowState);
}

TEST_F(TWeakPtrTest, IsExpired)
{
    TIntricateObject::TWkPtr ptr;

    EXPECT_IS_TRUE(ptr.IsExpired());
    {
        TIntricateObject::TPtr object = New<TIntricateObject>();
        ptr = object;
        EXPECT_IS_FALSE(ptr.IsExpired());
    }
    EXPECT_IS_TRUE(ptr.IsExpired());
}

TEST_F(TWeakPtrTest, UpCast)
{
    TDerivedIntricateObject::TPtr object = New<TDerivedIntricateObject>();
    TIntricateObject::TWkPtr ptr = object;

    EXPECT_EQ(object.Get(), ptr.Lock().Get());
}

static void* AsynchronousDeleter(void* param)
{
    TSlowlyDyingObject::TPtr* indirectObject =
        reinterpret_cast<TSlowlyDyingObject::TPtr*>(param);
    indirectObject->Reset();
    return NULL;
}

TEST_F(TWeakPtrTest, AcquisionOfSlowlyDyingObject)
{
    TSlowlyDyingObject::TPtr object = New<TSlowlyDyingObject>();
    TSlowlyDyingObject::TWkPtr ptr(object);

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

    EXPECT_EQ(TSlowlyDyingObject::TPtr(), ptr.Lock());
    EXPECT_THAT(*objectPtr, HasRefCounts(0, 2));

    // Finalize object destruction.
    DeathEvent->Signal();
    thread.Join();

    ASSERT_EQ(1, ConstructorShadowState);
    ASSERT_EQ(2, DestructorShadowState);

    EXPECT_EQ(TSlowlyDyingObject::TPtr(), ptr.Lock());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
