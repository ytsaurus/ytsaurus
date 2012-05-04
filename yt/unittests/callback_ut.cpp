#include "stdafx.h"

#include <ytlib/misc/common.h>
#include <ytlib/actions/callback.h>
#include <ytlib/actions/callback_internal.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

// White-box testpoint.
struct TFakeInvoker
{
    typedef void(Signature)(NDetail::TBindStateBase*);
    static void Run(NDetail::TBindStateBase*)
    { }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class Runnable, class Signature, class BoundArgs>
struct TBindState;

// White-box injection into a #TCallback<> object for checking
// comparators and emptiness APIs. Use a #TBindState<> that is specialized
// based on a type we declared in the anonymous namespace above to remove any
// chance of colliding with another instantiation and breaking the
// one-definition-rule.
template <>
struct TBindState<void(), void(), void(TFakeInvoker)>
    : public TBindStateBase
{
public:
    typedef TFakeInvoker TInvokerType;
#ifdef ENABLE_BIND_LOCATION_TRACKING
    TBindState()
        : TBindStateBase(FROM_HERE)
    { }
#endif
};

template <>
struct TBindState<void(), void(), void(TFakeInvoker, TFakeInvoker)>
    : public TBindStateBase
{
public:
    typedef TFakeInvoker TInvokerType;
#ifdef ENABLE_BIND_LOCATION_TRACKING
    TBindState()
        : TBindStateBase(FROM_HERE)
    { }
#endif
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Implement accurate check on the number of Ref() and Unref()s.

namespace {

typedef NDetail::TBindState<void(), void(), void(TFakeInvoker)>
    TFakeBindState1;
typedef NDetail::TBindState<void(), void(), void(TFakeInvoker, TFakeInvoker)>
    TFakeBindState2;

class TCallbackTest
    : public ::testing::Test
{
public:
    TCallbackTest()
        : FirstCallback(New<TFakeBindState1>())
        , SecondCallback(New<TFakeBindState2>())
    { }

    virtual ~TCallbackTest()
    { }

protected:
    TCallback<void()> FirstCallback;
    const TCallback<void()> SecondCallback;

    TCallback<void()> NullCallback;
};

// Ensure we can create unbound callbacks. We need this to be able to store
// them in class members that can be initialized later.
TEST_F(TCallbackTest, DefaultConstruction)
{
    TCallback<void()> c0;

    TCallback<void(int)> c1;
    TCallback<void(int,int)> c2;
    TCallback<void(int,int,int)> c3;
    TCallback<void(int,int,int,int)> c4;
    TCallback<void(int,int,int,int,int)> c5;
    TCallback<void(int,int,int,int,int,int)> c6;

    EXPECT_IS_TRUE(c0.IsNull());
    EXPECT_IS_TRUE(c1.IsNull());
    EXPECT_IS_TRUE(c2.IsNull());
    EXPECT_IS_TRUE(c3.IsNull());
    EXPECT_IS_TRUE(c4.IsNull());
    EXPECT_IS_TRUE(c5.IsNull());
    EXPECT_IS_TRUE(c6.IsNull());
}

TEST_F(TCallbackTest, IsNull)
{
    EXPECT_IS_TRUE(NullCallback.IsNull());
    EXPECT_IS_FALSE(FirstCallback.IsNull());
    EXPECT_IS_FALSE(SecondCallback.IsNull());
}

TEST_F(TCallbackTest, Move)
{
    EXPECT_IS_FALSE(FirstCallback.IsNull());

    TCallback<void()> localCallback(MoveRV(FirstCallback));
    TCallback<void()> anotherCallback;

    EXPECT_IS_TRUE(FirstCallback.IsNull());
    EXPECT_IS_FALSE(localCallback.IsNull());
    EXPECT_IS_TRUE(anotherCallback.IsNull());

    anotherCallback = MoveRV(localCallback);

    EXPECT_IS_TRUE(FirstCallback.IsNull());
    EXPECT_IS_TRUE(localCallback.IsNull());
    EXPECT_IS_FALSE(anotherCallback.IsNull());
}

TEST_F(TCallbackTest, Equals)
{
    EXPECT_IS_TRUE(FirstCallback.Equals(FirstCallback));
    EXPECT_IS_FALSE(FirstCallback.Equals(SecondCallback));
    EXPECT_IS_FALSE(SecondCallback.Equals(FirstCallback));

    // We should compare based on instance, not type.
    TCallback<void()> localCallback(New<TFakeBindState1>());
    TCallback<void()> anotherCallback = FirstCallback;
   
    EXPECT_IS_TRUE(FirstCallback.Equals(anotherCallback));
    EXPECT_IS_FALSE(FirstCallback.Equals(localCallback));

    // Empty, however, is always equal to empty.
    TCallback<void()> localNullCallback;
    EXPECT_IS_TRUE(NullCallback.Equals(localNullCallback));
}

TEST_F(TCallbackTest, Reset)
{
    // Resetting should bring us back to empty.
    ASSERT_FALSE(FirstCallback.IsNull());
    ASSERT_FALSE(FirstCallback.Equals(NullCallback));

    FirstCallback.Reset();

    EXPECT_IS_TRUE(FirstCallback.IsNull());
    EXPECT_IS_TRUE(FirstCallback.Equals(NullCallback));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
