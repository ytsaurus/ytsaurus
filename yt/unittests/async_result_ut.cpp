#include "stdafx.h"

#include "../ytlib/actions/future.h"

#include <util/system/thread.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TFutureTest
    : public ::testing::Test
{
protected:
    TFuture<int> Result;
};

TEST_F(TFutureTest, SimpleGet)
{
    Result.Set(57);

    EXPECT_EQ(57, Result.Get());
}

TEST_F(TFutureTest, SimpleTryGet)
{
    int value = 17;

    EXPECT_IS_FALSE(Result.TryGet(&value));
    EXPECT_EQ(17, value);

    Result.Set(42);

    EXPECT_IS_TRUE(Result.TryGet(&value));
    EXPECT_EQ(42, value);
}

class TMockSubscriber : public IParamAction<int>
{
public:
    typedef TIntrusivePtr<TMockSubscriber> TPtr;
    MOCK_METHOD1(Do, void(int value));
};

TEST_F(TFutureTest, Subscribe)
{
    TMockSubscriber::TPtr firstSubscriber = New<TMockSubscriber>();
    TMockSubscriber::TPtr secondSubscriber = New<TMockSubscriber>();

    EXPECT_CALL(*firstSubscriber, Do(42)).Times(1);
    EXPECT_CALL(*secondSubscriber, Do(42)).Times(1);

    Result.Subscribe(firstSubscriber.Get());
    Result.Set(42);
    Result.Subscribe(secondSubscriber.Get());
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
    TMockSubscriber::TPtr firstSubscriber = New<TMockSubscriber>();
    TMockSubscriber::TPtr secondSubscriber = New<TMockSubscriber>();

    EXPECT_CALL(*firstSubscriber, Do(42)).Times(1);
    EXPECT_CALL(*secondSubscriber, Do(42)).Times(1);

    Result.Subscribe(firstSubscriber.Get());

    TThread thread(&AsynchronousSetter, &Result);
    thread.Start();
    thread.Join();
    
    Result.Subscribe(secondSubscriber.Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

