#include "stdafx.h"

#include <ytlib/actions/future.h>

#include <util/system/thread.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TFutureTest
    : public ::testing::Test
{
protected:
    TFuture<int>::TPtr Result;

    virtual void SetUp()
    {
        Result = New< TFuture<int> >();
    }

    virtual void TearDown()
    {
        Result.Reset();
    }
};

TEST_F(TFutureTest, SimpleGet)
{
    Result->Set(57);

    EXPECT_EQ(57, Result->Get());
}

TEST_F(TFutureTest, SimpleTryGet)
{
    int value = 17;

    EXPECT_IS_FALSE(Result->TryGet(&value));
    EXPECT_EQ(17, value);

    Result->Set(42);

    EXPECT_IS_TRUE(Result->TryGet(&value));
    EXPECT_EQ(42, value);
}

class TMockSubscriber
    : public TCallback<void(int)>
{
public:

    MOCK_METHOD1(Run, void(int value));
};

TEST_F(TFutureTest, Subscribe)
{
    TMockSubscriber firstSubscriber;
    TMockSubscriber secondSubscriber;

    EXPECT_CALL(firstSubscriber, Run(42)).Times(1);
    EXPECT_CALL(secondSubscriber, Run(42)).Times(1);

    Result->Subscribe(firstSubscriber);
    Result->Set(42);
    Result->Subscribe(secondSubscriber);
}

static void* AsynchronousSetter(void* param)
{
    Sleep(TDuration::Seconds(0.100));

    TFuture<int>* result = reinterpret_cast<TFuture<int>*>(param);
    result->Set(42);

    return NULL;
}

TEST_F(TFutureTest, SubscribeWithAsynchronousSet)
{
    TMockSubscriber firstSubscriber;
    TMockSubscriber secondSubscriber;

    EXPECT_CALL(firstSubscriber, Run(42)).Times(1);
    EXPECT_CALL(secondSubscriber, Run(42)).Times(1);

    Result->Subscribe(firstSubscriber);

    TThread thread(&AsynchronousSetter, Result.Get());
    thread.Start();
    thread.Join();

    Result->Subscribe(secondSubscriber);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

