#include "stdafx.h"

#include <ytlib/misc/common.h>
#include <ytlib/misc/ref_counted_base.h>
#include <ytlib/misc/new.h>

#include <contrib/testing/framework.h>

using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::InSequence;
using ::testing::MockFunction;
using ::testing::StrictMock;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {
    //! This object tracks number of incremenets and decrements to the reference
    //! counter.
    class TIntricateObject
    {
    public:
        typedef TIntrusivePtr<TIntricateObject> TPtr;

        int Increments;
        int Decrements;
        int Zeros;

    public:
        TIntricateObject()
            : Increments(0)
            , Decrements(0)
            , Zeros(0)
        { }

        template<typename T>
        void BindToCookie(const T&)
        { }

        void Increment()
        {
            ++Increments;
        }

        void Decrement()
        {
            ++Decrements;

            if (Increments == Decrements) {
                ++Zeros;
            }
        }

    private:
        TIntricateObject(const TIntricateObject&);
        TIntricateObject(const TIntricateObject&&);
        TIntricateObject& operator=(const TIntricateObject&);
        TIntricateObject& operator=(const TIntricateObject&&);
    };

    MATCHER_P3(HasReferenceCounters, increments, decrements, zeros,
        "Reference counter " \
        "was incremented " + ::testing::PrintToString(increments) + " times, " +
        "was decremented " + ::testing::PrintToString(decrements) + " times, " +
        "vanished to zero " + ::testing::PrintToString(zeros) + " times")
    {
        UNUSED(result_listener);
        return
            arg.Increments == increments &&
            arg.Decrements == decrements &&
            arg.Zeros == zeros;
    }

    //! This is a simple typical reference-counted object.
    class TSimpleObject : public TRefCountedBase
    {};

    //! This is a simple inherited reference-counted object.
    class TAnotherObject : public TSimpleObject
    {};

    //! This is an object which creates intrusive pointers to the self
    //! during its construction and also fires some events.
    class TObjectWithEventsAndSelfPointers : public TRefCountedBase
    {
    public:
        typedef StrictMock< MockFunction<void()> > TEvent;
        typedef TIntrusivePtr<TObjectWithEventsAndSelfPointers> TPtr;

    private:
        TEvent* BeforeCreate;
        TEvent* AfterCreate;
        TEvent* OnDestroy;

    public:
        TObjectWithEventsAndSelfPointers(
            TEvent* beforeCreate,
            TEvent* afterCreate,
            TEvent* onDestroy)
            : BeforeCreate(beforeCreate)
            , AfterCreate(afterCreate)
            , OnDestroy(onDestroy)
        {
            BeforeCreate->Call();

            for (int i = 0; i < 3; ++i) {
                TPtr ptr(this);
            }

            AfterCreate->Call();
        }

        virtual ~TObjectWithEventsAndSelfPointers()
        {
            OnDestroy->Call();
        }
    };
} // namespace <anonymous>

template<>
struct TIntrusivePtrTraits<TIntricateObject>
{
    static void Ref(TIntricateObject* object)
    {
        object->Increment();
    }

    static void UnRef(TIntricateObject* object)
    {
        object->Decrement();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TIntrusivePtrTest, Basic)
{
    TIntricateObject object;

    EXPECT_THAT(object, HasReferenceCounters(0, 0, 0));
    {
        TIntricateObject::TPtr foo(&object);

        EXPECT_THAT(object, HasReferenceCounters(1, 0, 0));
        EXPECT_EQ(&object, foo.Get());
    }
    EXPECT_THAT(object, HasReferenceCounters(1, 1, 1));
}

TEST(TIntrusivePtrTest, Reset)
{
    TIntricateObject object;

    TIntricateObject::TPtr ptr(&object);
    EXPECT_THAT(object, HasReferenceCounters(1, 0, 0));
    ptr.Reset();
    EXPECT_THAT(object, HasReferenceCounters(1, 1, 1));
    ptr.Reset(&object);
    EXPECT_THAT(object, HasReferenceCounters(2, 1, 1));
}

TEST(TIntrusivePtrTest, CopySemantics)
{
    TIntricateObject object;

    TIntricateObject::TPtr foo(&object);
    EXPECT_THAT(object, HasReferenceCounters(1, 0, 0));

    {
        TIntricateObject::TPtr bar(foo);
        EXPECT_THAT(object, HasReferenceCounters(2, 0, 0));
        EXPECT_EQ(foo.Get(), bar.Get());
    }

    EXPECT_THAT(object, HasReferenceCounters(2, 1, 0));

    {
        TIntricateObject::TPtr bar;
        bar = foo;

        EXPECT_THAT(object, HasReferenceCounters(3, 1, 0));
        EXPECT_EQ(foo.Get(), bar.Get());
    }

    EXPECT_THAT(object, HasReferenceCounters(3, 2, 0));
}

TEST(TIntrusivePtrTest, MoveSemantics)
{
    TIntricateObject object;

    TIntricateObject::TPtr foo(&object);
    EXPECT_THAT(object, HasReferenceCounters(1, 0, 0));

    {
        TIntricateObject::TPtr bar(MoveRV(foo));
        EXPECT_THAT(object, HasReferenceCounters(1, 0, 0));
        EXPECT_THAT(foo.Get(), IsNull());
        EXPECT_EQ(&object, bar.Get());
    }

    EXPECT_THAT(object, HasReferenceCounters(1, 1, 1));
    foo.Reset(&object);
    EXPECT_THAT(object, HasReferenceCounters(2, 1, 1));

    {
        TIntricateObject::TPtr bar;
        bar = MoveRV(foo);
        EXPECT_THAT(object, HasReferenceCounters(2, 1, 1));
        EXPECT_THAT(foo.Get(), IsNull());
        EXPECT_EQ(&object, bar.Get());
    }
}

TEST(TIntrusivePtrTest, Swap)
{
    TIntricateObject object;

    TIntricateObject::TPtr foo(&object);
    TIntricateObject::TPtr bar;

    EXPECT_THAT(object, HasReferenceCounters(1, 0, 0));
    EXPECT_THAT(foo.Get(), NotNull());
    EXPECT_THAT(bar.Get(), IsNull());

    foo.Swap(bar);

    EXPECT_THAT(object, HasReferenceCounters(1, 0, 0));
    EXPECT_THAT(foo.Get(), IsNull());
    EXPECT_THAT(bar.Get(), NotNull());

    foo.Swap(bar);

    EXPECT_THAT(object, HasReferenceCounters(1, 0, 0));
    EXPECT_THAT(foo.Get(), NotNull());
    EXPECT_THAT(bar.Get(), IsNull());
}

TEST(TIntrusivePtrTest, Cast)
{
    TIntrusivePtr<TSimpleObject> foo = New<TSimpleObject>();
    TIntrusivePtr<TSimpleObject> bar = New<TAnotherObject>();

    SUCCEED();
}

TEST(TIntrusivePtrTest, NewDoesNotAcquireAdditionalReferences)
{
    TIntricateObject* rawPtr = NULL;
    TIntricateObject::TPtr ptr = New<TIntricateObject>();

    // There was no acquision during construction. Note that
    // TRefCountedBase has initial reference counter set to 1,
    // so there will be no memory leaks.
    rawPtr = ptr.Get();
    EXPECT_THAT(*rawPtr, HasReferenceCounters(0, 0, 0));
    ptr.Reset();
    EXPECT_THAT(*rawPtr, HasReferenceCounters(0, 1, 0));
    delete rawPtr;
}

TEST(TIntrusivePtrTest, ObjectIsNotDestroyedPrematurely)
{
    TObjectWithEventsAndSelfPointers::TEvent beforeCreate;
    TObjectWithEventsAndSelfPointers::TEvent afterCreate;
    TObjectWithEventsAndSelfPointers::TEvent destroy;

    InSequence dummy;
    EXPECT_CALL(beforeCreate, Call());
    EXPECT_CALL(afterCreate, Call());
    EXPECT_CALL(destroy, Call());

    {
        TObjectWithEventsAndSelfPointers::TPtr ptr =
            New<TObjectWithEventsAndSelfPointers>(
            &beforeCreate, &afterCreate, &destroy);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
