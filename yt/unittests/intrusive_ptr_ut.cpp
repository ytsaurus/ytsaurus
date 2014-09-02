#include "stdafx.h"
#include "framework.h"

#include <core/misc/common.h>
#include <core/misc/ref_counted.h>
#include <core/misc/new.h>
#include <core/misc/format.h>

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

// This object tracks number of increments and decrements
// to the reference counter (see traits specialization below).
class TIntricateObject
{
public:
    int Increments;
    int Decrements;
    int Zeros;

public:
    TIntricateObject()
        : Increments(0)
        , Decrements(0)
        , Zeros(0)
    { }

    void Ref()
    {
        ++Increments;
    }

    void Unref()
    {
        ++Decrements;

        if (Increments == Decrements) {
            ++Zeros;
        }
    }

private:
    // Explicitly non-copyable.
    TIntricateObject(const TIntricateObject&);
    TIntricateObject(TIntricateObject&&);
    TIntricateObject& operator=(const TIntricateObject&);
    TIntricateObject& operator=(TIntricateObject&&);
};

typedef TIntrusivePtr<TIntricateObject> TIntricateObjectPtr;


void InitializeTracking(TIntricateObject* /*object*/, TRefCountedTypeCookie /*cookie*/, size_t /*size*/)
{ }

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
    *os << Format(
        "%v increments, %v decrements and %v times vanished",
        arg.Increments, arg.Decrements, arg.Zeros);
}

// This is an object which creates intrusive pointers to the self
// during its construction.
class TObjectWithSelfPointers
    : public TRefCounted
{
private:
    TOutputStream* Output;
public:
    TObjectWithSelfPointers(TOutputStream* output)
        : Output(output)
    {
        *Output << "Cb";

        for (int i = 0; i < 3; ++i) {
            *Output << '!';
            TIntrusivePtr<TObjectWithSelfPointers> ptr(this);
        }

        *Output << "Ca";
    }

    virtual ~TObjectWithSelfPointers()
    {
        *Output << 'D';
    }
};

// This is an object which throws an exception during its construction.
class TObjectThrowingException
    : public TRefCounted
{
public:
    TObjectThrowingException()
    {
        throw std::runtime_error("Sample Exception");
    }

    virtual ~TObjectThrowingException()
    { }
};

// This is a simple object with intrinsic reference counting.
class TObjectWithIntrinsicRC
    : public TIntrinsicRefCounted
{
private:
    TOutputStream* Output;
public:
    TObjectWithIntrinsicRC(TOutputStream* output)
        : Output(output)
    {
        *Output << 'C';
    }
    virtual ~TObjectWithIntrinsicRC()
    {
        *Output << 'D';
    }
    void DoSomething()
    {
        *Output << '!';
    }
};

// This is a simple object with extrinsic reference counting.
class TObjectWithExtrinsicRC
    : public TExtrinsicRefCounted
{
private:
    TOutputStream* Output;
public:
    TObjectWithExtrinsicRC(TOutputStream* output)
        : Output(output)
    {
        *Output << 'C';
    }
    virtual ~TObjectWithExtrinsicRC()
    {
        *Output << 'D';
    }
    void DoSomething()
    {
        *Output << '!';
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TIntrusivePtrTest, Empty)
{
    TIntricateObjectPtr emptyPointer;
    EXPECT_EQ(NULL, emptyPointer.Get());
}

TEST(TIntrusivePtrTest, Basic)
{
    TIntricateObject object;

    EXPECT_THAT(object, HasRefCounts(0, 0, 0));

    {
        TIntricateObjectPtr owningPointer(&object);
        EXPECT_THAT(object, HasRefCounts(1, 0, 0));
        EXPECT_EQ(&object, owningPointer.Get());
    }

    EXPECT_THAT(object, HasRefCounts(1, 1, 1));

    {
        TIntricateObjectPtr nonOwningPointer(&object, false);
        EXPECT_THAT(object, HasRefCounts(1, 1, 1));
        EXPECT_EQ(&object, nonOwningPointer.Get());
    }

    EXPECT_THAT(object, HasRefCounts(1, 2, 1));
}

TEST(TIntrusivePtrTest, ResetToNull)
{
    TIntricateObject object;
    TIntricateObjectPtr ptr(&object);

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

    TIntricateObjectPtr ptr(&firstObject);

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

    TIntricateObjectPtr foo(&object);
    EXPECT_THAT(object, HasRefCounts(1, 0, 0));

    {
        TIntricateObjectPtr bar(foo);
        EXPECT_THAT(object, HasRefCounts(2, 0, 0));
        EXPECT_EQ(&object, foo.Get());
        EXPECT_EQ(&object, bar.Get());
    }

    EXPECT_THAT(object, HasRefCounts(2, 1, 0));

    {
        TIntricateObjectPtr bar;
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

    TIntricateObjectPtr foo(&object);
    EXPECT_THAT(object, HasRefCounts(1, 0, 0));

    {
        TIntricateObjectPtr bar(std::move(foo));
        EXPECT_THAT(object, HasRefCounts(1, 0, 0));
        EXPECT_THAT(foo.Get(), IsNull());
        EXPECT_EQ(&object, bar.Get());
    }

    EXPECT_THAT(object, HasRefCounts(1, 1, 1));
    foo.Reset(&object);
    EXPECT_THAT(object, HasRefCounts(2, 1, 1));

    {
        TIntricateObjectPtr bar;
        bar = std::move(foo);
        EXPECT_THAT(object, HasRefCounts(2, 1, 1));
        EXPECT_THAT(foo.Get(), IsNull());
        EXPECT_EQ(&object, bar.Get());
    }
}

TEST(TIntrusivePtrTest, Swap)
{
    TIntricateObject object;

    TIntricateObjectPtr foo(&object);
    TIntricateObjectPtr bar;

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
    class TSimpleObject : public TRefCounted
    {};

    //! This is a simple inherited reference-counted object.
    class TAnotherObject : public TSimpleObject
    {};

    TIntrusivePtr<TSimpleObject>  foo = New<TSimpleObject>();
    TIntrusivePtr<TSimpleObject>  bar = New<TAnotherObject>();
    TIntrusivePtr<TAnotherObject> baz = New<TAnotherObject>();

    foo = baz;

    EXPECT_TRUE(foo == baz);
}

TEST(TIntrusivePtrTest, UnspecifiedBoolType)
{
    TIntricateObject object;

    TIntricateObjectPtr foo;
    TIntricateObjectPtr bar(&object);

    EXPECT_FALSE(foo);
    EXPECT_TRUE(bar);
}

TEST(TIntrusivePtrTest, NewDoesNotAcquireAdditionalReferences)
{
    TIntricateObject* rawPtr = NULL;
    TIntricateObjectPtr ptr = New<TIntricateObject>();

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
    typedef TIntrusivePtr<TObjectWithSelfPointers> TMyPtr;

    TStringStream output;
    {
        TMyPtr ptr = New<TObjectWithSelfPointers>(&output);
    }

    // TObject... appends symbols to the output; see definitions.
    EXPECT_STREQ("Cb!!!CaD", output.Str().c_str());
}

TEST(TIntrusivePtrTest, EqualityOperator)
{
    TIntricateObject object, anotherObject;

    TIntricateObjectPtr emptyPointer;
    TIntricateObjectPtr somePointer(&object);
    TIntricateObjectPtr samePointer(&object);
    TIntricateObjectPtr anotherPointer(&anotherObject);

    EXPECT_TRUE(NULL == emptyPointer);
    EXPECT_TRUE(emptyPointer == NULL);

    EXPECT_FALSE(somePointer == NULL);
    EXPECT_FALSE(samePointer == NULL);

    EXPECT_TRUE(somePointer != NULL);
    EXPECT_TRUE(samePointer != NULL);

    EXPECT_FALSE(somePointer == emptyPointer);
    EXPECT_FALSE(samePointer == emptyPointer);

    EXPECT_TRUE(somePointer != emptyPointer);
    EXPECT_TRUE(samePointer != emptyPointer);

    EXPECT_TRUE(somePointer == samePointer);

    EXPECT_TRUE(&object == somePointer);
    EXPECT_TRUE(&object == samePointer);

    EXPECT_FALSE(somePointer == anotherPointer);
    EXPECT_TRUE(somePointer != anotherPointer);

    EXPECT_TRUE(&anotherObject == anotherPointer);
}

TEST(TIntrusivePtrTest, IntrisicRCBehaviour)
{
    typedef TIntrusivePtr<TObjectWithIntrinsicRC> TMyPtr;

    TStringStream output;
    {
        TMyPtr ptr = New<TObjectWithIntrinsicRC>(&output);
        {
            TMyPtr anotherPtr(ptr);
            anotherPtr->DoSomething();
        }
        {
            TMyPtr anotherPtr(ptr);
            anotherPtr->DoSomething();
        }
        ptr->DoSomething();
    }

    // TObject... appends symbols to the output; see definitions.
    EXPECT_STREQ("C!!!D", output.Str().c_str());
}

TEST(TIntrusivePtrTest, ExtrinsicRCBehaviour)
{
    typedef TIntrusivePtr<TObjectWithExtrinsicRC> TMyPtr;

    TStringStream output;
    {
        TMyPtr ptr = New<TObjectWithExtrinsicRC>(&output);
        {
            TMyPtr anotherPtr(ptr);
            anotherPtr->DoSomething();
        }
        {
            TMyPtr anotherPtr(ptr);
            anotherPtr->DoSomething();
        }
        ptr->DoSomething();
    }

    // TObject... appends symbols to the output; see definitions.
    EXPECT_STREQ("C!!!D", output.Str().c_str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
