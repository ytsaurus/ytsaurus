#include "../ytlib/actions/async_result.h"

#include <util/system/thread.h>

#include "framework/framework.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TAsyncResultTest : public ::testing::Test {
protected:
    TAsyncResult<int> Result;
};

TEST_F(TAsyncResultTest, SimpleGet)
{
    Result.Set(57);

    EXPECT_EQ(57, Result.Get());
}

TEST_F(TAsyncResultTest, SimpleTryGet)
{
    int value = 17;

    EXPECT_FALSE(Result.TryGet(&value));
    EXPECT_EQ(17, value);

    Result.Set(42);

    EXPECT_TRUE(Result.TryGet(&value));
    EXPECT_EQ(42, value);
}

class TMockSubscriber : public IParamAction<int>
{
public:
    typedef TIntrusivePtr<TMockSubscriber> TPtr;

    MOCK_METHOD1(Do, void(int value));
};

TEST_F(TAsyncResultTest, Subscribe)
{
    TMockSubscriber::TPtr firstSubscriber = new TMockSubscriber();
    TMockSubscriber::TPtr secondSubscriber = new TMockSubscriber();

    EXPECT_CALL(*firstSubscriber, Do(42)).Times(1);
    EXPECT_CALL(*secondSubscriber, Do(42)).Times(1);

    Result.Subscribe(firstSubscriber.Get());
    Result.Set(42);
    Result.Subscribe(secondSubscriber.Get());
}

static void* AsynchronousSetter(void* param)
{
    Sleep(TDuration::Seconds(0.125));

    TAsyncResult<int>* result = reinterpret_cast<TAsyncResult<int>*>(param);
    result->Set(42);

    return NULL;
}

TEST_F(TAsyncResultTest, SubscribeWithAsynchronousSet)
{
    TMockSubscriber::TPtr firstSubscriber = new TMockSubscriber();
    TMockSubscriber::TPtr secondSubscriber = new TMockSubscriber();

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

