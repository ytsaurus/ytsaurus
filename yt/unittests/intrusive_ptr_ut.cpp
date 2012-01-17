#include "stdafx.h"

#include "../ytlib/misc/common.h"
#include "../ytlib/misc/ref_counted_base.h"
#include "../ytlib/misc/new.h"

#include <contrib/testing/framework.h>

using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::InSequence;
using ::testing::MockFunction;
using ::testing::StrictMock;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {
    // This object tracks number of incremenets and decrements
    // to the reference counter.
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

        // TRefCountedTracker calls BindToCookie() on object creation.
        // So we have to stub it. 
        template<typename T>
        void BindToCookie(const T&)
        { }

        void CountedIncrement()
        {
            ++Increments;
        }

        void CountedDecrement()
        {
            ++Decrements;

            if (Increments == Decrements) {
                ++Zeros;
            }
        }

    private:
        // Explicitly non-copyable.
        TIntricateObject(const TIntricateObject&);
        TIntricateObject(const TIntricateObject&&);
        TIntricateObject& operator=(const TIntricateObject&);
        TIntricateObject& operator=(const TIntricateObject&&);
    };

    MATCHER_P3(HasRefCounts, increments, decrements, zeros,
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

    void PrintTo(const TIntricateObject& arg, ::std::ostream* os)
    {
        Stroka repr = Sprintf(
            "%d increments, %d decrements and %d times vanished",
            arg.Increments, arg.Decrements, arg.Zeros);
        *os << repr.c_str();
    }

    //! This is an object which creates intrusive pointers to the self
    //! during its construction and also fires some events.
    class TObjectWithEventsAndSelfPointers
        : public TRefCountedBase
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

    class TObjectWithIntrinsicRC
        : public TIntrinsicRefCounted
    {
    private:
        TOutputStream* Output;
    public:
        TObjectWithIntrinsicRC(TOutputStream* output)
            : Output(output)
        {
            *Output << "+";           
        }
        virtual ~TObjectWithIntrinsicRC()
        {
            *Output << "-";
        }
        void DoSomething()
        {
            *Output << "!";
        }
    };

    class TObjectWithExtrinsicRC
        : public TExtrinsicRefCounted
    {
    private:
        TOutputStream* Output;
    public:
        TObjectWithExtrinsicRC(TOutputStream* output)
            : Output(output)
        {
            *Output << "+";
        }
        virtual ~TObjectWithExtrinsicRC()
        {
            *Output << "-";
        }
        void DoSomething()
        {
            *Output << "!";
        }
    };

} // namespace <anonymous>

template<>
struct TIntrusivePtrTraits<TIntricateObject>
{
    static void Ref(TIntricateObject* object)
    {
        object->CountedIncrement();
    }

    static void UnRef(TIntricateObject* object)
    {
        object->CountedDecrement();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TIntrusivePtrTest, Empty)
{
    TIntricateObject::TPtr emptyPointer;
    EXPECT_EQ(NULL, emptyPointer.Get());
}

TEST(TIntrusivePtrTest, Basic)
{
    TIntricateObject object;

    EXPECT_THAT(object, HasRefCounts(0, 0, 0));

    {
        TIntricateObject::TPtr owningPointer(&object);
        EXPECT_THAT(object, HasRefCounts(1, 0, 0));
        EXPECT_EQ(&object, owningPointer.Get());
    }

    EXPECT_THAT(object, HasRefCounts(1, 1, 1));

    {
        TIntricateObject::TPtr nonOwningPointer(&object, false);
        EXPECT_THAT(object, HasRefCounts(1, 1, 1));
        EXPECT_EQ(&object, nonOwningPointer.Get());
    }

    EXPECT_THAT(object, HasRefCounts(1, 2, 1));
}

TEST(TIntrusivePtrTest, ResetToNull)
{
    TIntricateObject object;
    TIntricateObject::TPtr ptr(&object);

    EXPECT_THAT(object, HasRefCounts(1, 0, 0));
    EXPECT_EQ(&object, ptr.Get());

    ptr.Reset();

    EXPECT_THAT(object, HasRefCounts(1, 1, 1));
    EXPECT_EQ(NULL, ptr.Get());
}

TEST(TIntrusivePtrTest, ResetToOtherObject)
{
    TIntricateObject firstObject;
    TIntricateObject secondObject;

    TIntricateObject::TPtr ptr(&firstObject);

    EXPECT_THAT(firstObject, HasRefCounts(1, 0, 0));
    EXPECT_THAT(secondObject, HasRefCounts(0, 0, 0));
    EXPECT_EQ(&firstObject, ptr.Get());

    ptr.Reset(&secondObject);

    EXPECT_THAT(firstObject, HasRefCounts(1, 1, 1));
    EXPECT_THAT(secondObject, HasRefCounts(1, 0, 0));
    EXPECT_EQ(&secondObject, ptr.Get());
}

TEST(TIntrusivePtrTest, CopySemantics)
{
    TIntricateObject object;

    TIntricateObject::TPtr foo(&object);
    EXPECT_THAT(object, HasRefCounts(1, 0, 0));

    {
        TIntricateObject::TPtr bar(foo);
        EXPECT_THAT(object, HasRefCounts(2, 0, 0));
        EXPECT_EQ(&object, foo.Get());
        EXPECT_EQ(&object, bar.Get());
    }

    EXPECT_THAT(object, HasRefCounts(2, 1, 0));

    {
        TIntricateObject::TPtr bar;
        bar = foo;

        EXPECT_THAT(object, HasRefCounts(3, 1, 0));
        EXPECT_EQ(&object, foo.Get());
        EXPECT_EQ(&object, bar.Get());
    }

    EXPECT_THAT(object, HasRefCounts(3, 2, 0));
}

TEST(TIntrusivePtrTest, MoveSemantics)
{
    TIntricateObject object;

    TIntricateObject::TPtr foo(&object);
    EXPECT_THAT(object, HasRefCounts(1, 0, 0));

    {
        TIntricateObject::TPtr bar(MoveRV(foo));
        EXPECT_THAT(object, HasRefCounts(1, 0, 0));
        EXPECT_THAT(foo.Get(), IsNull());
        EXPECT_EQ(&object, bar.Get());
    }

    EXPECT_THAT(object, HasRefCounts(1, 1, 1));
    foo.Reset(&object);
    EXPECT_THAT(object, HasRefCounts(2, 1, 1));

    {
        TIntricateObject::TPtr bar;
        bar = MoveRV(foo);
        EXPECT_THAT(object, HasRefCounts(2, 1, 1));
        EXPECT_THAT(foo.Get(), IsNull());
        EXPECT_EQ(&object, bar.Get());
    }
}

TEST(TIntrusivePtrTest, Swap)
{
    TIntricateObject object;

    TIntricateObject::TPtr foo(&object);
    TIntricateObject::TPtr bar;

    EXPECT_THAT(object, HasRefCounts(1, 0, 0));
    EXPECT_THAT(foo.Get(), NotNull());
    EXPECT_THAT(bar.Get(), IsNull());

    foo.Swap(bar);

    EXPECT_THAT(object, HasRefCounts(1, 0, 0));
    EXPECT_THAT(foo.Get(), IsNull());
    EXPECT_THAT(bar.Get(), NotNull());

    foo.Swap(bar);

    EXPECT_THAT(object, HasRefCounts(1, 0, 0));
    EXPECT_THAT(foo.Get(), NotNull());
    EXPECT_THAT(bar.Get(), IsNull());
}

TEST(TIntrusivePtrTest, UpCast)
{
    //! This is a simple typical reference-counted object.
    class TSimpleObject : public TRefCountedBase
    {};

    //! This is a simple inherited reference-counted object.
    class TAnotherObject : public TSimpleObject
    {};

    TIntrusivePtr<TSimpleObject>  foo = New<TSimpleObject>();
    TIntrusivePtr<TSimpleObject>  bar = New<TAnotherObject>();
    TIntrusivePtr<TAnotherObject> baz = New<TAnotherObject>();

    Cerr << "Assignment goes here" << Endl;
    foo = baz;

    EXPECT_IS_TRUE(foo == baz);
}

TEST(TIntrusivePtrTest, UnspecifiedBoolType)
{
    TIntricateObject object;

    TIntricateObject::TPtr foo;
    TIntricateObject::TPtr bar(&object);

    EXPECT_IS_FALSE(foo);
    EXPECT_IS_TRUE(bar);
}

TEST(TIntrusivePtrTest, NewDoesNotAcquireAdditionalReferences)
{
    TIntricateObject* rawPtr = NULL;
    TIntricateObject::TPtr ptr = New<TIntricateObject>();

    // There was no acquision during construction. Note that
    // TRefCountedBase has initial reference counter set to 1,
    // so there will be no memory leaks.
    rawPtr = ptr.Get();
    EXPECT_THAT(*rawPtr, HasRefCounts(0, 0, 0));
    ptr.Reset();
    EXPECT_THAT(*rawPtr, HasRefCounts(0, 1, 0));
    delete rawPtr;
}

TEST(TIntrusivePtrTest, ObjectIsNotDestroyedPrematurely)
{
    TObjectWithEventsAndSelfPointers::TEvent beforeCreate;
    TObjectWithEventsAndSelfPointers::TEvent afterCreate;
    TObjectWithEventsAndSelfPointers::TEvent destroy;

    InSequence dummy;
    EXPECT_CALL(beforeCreate, Call())
        .Times(1);
    EXPECT_CALL(afterCreate, Call())
        .Times(1);
    EXPECT_CALL(destroy, Call())
        .Times(1);

    {
        TObjectWithEventsAndSelfPointers::TPtr ptr =
            New<TObjectWithEventsAndSelfPointers>(
            &beforeCreate, &afterCreate, &destroy);
    }
}

TEST(TIntrusivePtrTest, EqualityOperator)
{
    TIntricateObject object, anotherObject;
    TIntricateObject::TPtr emptyPointer;
    TIntricateObject::TPtr somePointer(&object);
    TIntricateObject::TPtr samePointer(&object);
    TIntricateObject::TPtr anotherPointer(&anotherObject);

    EXPECT_IS_TRUE(NULL == emptyPointer);
    EXPECT_IS_TRUE(emptyPointer == NULL);

    EXPECT_IS_FALSE(somePointer == NULL);
    EXPECT_IS_FALSE(samePointer == NULL);

    EXPECT_IS_TRUE(somePointer != NULL);
    EXPECT_IS_TRUE(samePointer != NULL);

    EXPECT_IS_FALSE(somePointer == emptyPointer);
    EXPECT_IS_FALSE(samePointer == emptyPointer);

    EXPECT_IS_TRUE(somePointer != emptyPointer);
    EXPECT_IS_TRUE(samePointer != emptyPointer);

    EXPECT_IS_TRUE(somePointer == samePointer);

    EXPECT_IS_TRUE(&object == somePointer);
    EXPECT_IS_TRUE(&object == samePointer);

    EXPECT_IS_FALSE(somePointer == anotherPointer);
    EXPECT_IS_TRUE(somePointer != anotherPointer);

    EXPECT_IS_TRUE(&anotherObject == anotherPointer);
}

TEST(TIntrusivePtrTest, IntrisicRCBehaviour)
{
    typedef TIntrusivePtr<TObjectWithIntrinsicRC> TMyPtr;

    TStringStream output;
    {
        TMyPtr pointer = New<TObjectWithIntrinsicRC>(&output);
        {
            TMyPtr anotherPointer(pointer);
            anotherPointer->DoSomething();
        }
        pointer->DoSomething();
    }

    // TObject... appends symbols to the output; see definitions.
    EXPECT_STREQ("+!!-", output.Str().c_str());
}

TEST(TIntrusivePtrTest, ExtrinsicRCBehaviour)
{
    typedef TIntrusivePtr<TObjectWithExtrinsicRC> TMyPtr;

    TStringStream output;
    {
        TMyPtr pointer = New<TObjectWithExtrinsicRC>(&output);
        {
            TMyPtr anotherPointer(pointer);
            anotherPointer->DoSomething();
        }
        pointer->DoSomething();
    }

    // TObject... appends symbols to the output; see definitions.
    EXPECT_STREQ("+!!-", output.Str().c_str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
