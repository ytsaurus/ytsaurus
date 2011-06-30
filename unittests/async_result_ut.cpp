#include "../ytlib/actions/async_result.h"

#include <library/unittest/registar.h>
#include <util/system/thread.h>

namespace NYT {

class TAsyncResultTest
    : public TTestBase
{
    UNIT_TEST_SUITE(TAsyncResultTest);
        UNIT_TEST(TestTryGet);
        UNIT_TEST(TestGet);
        UNIT_TEST(TestSubsribe);
        UNIT_TEST(TestAsync);
    UNIT_TEST_SUITE_END();

public:
    void TestTryGet()
    {
        TAsyncResult<int> result;
        int value;
        UNIT_ASSERT(!result.TryGet(&value));
        result.Set(57);
        UNIT_ASSERT(result.TryGet(&value));
        UNIT_ASSERT(value == 57);
    }

    void TestGet()
    {
        TAsyncResult<int> result;
        result.Set(57);
        UNIT_ASSERT(result.Get() == 57);
    }

    class TTestSubscriber
        : public IParamAction<int>
    {
    public:
        typedef TIntrusivePtr<TTestSubscriber> TPtr;

        bool IsCalled;

        TTestSubscriber()
            : IsCalled(false)
        {
        }

        virtual void Do(int value)
        {
            UNIT_ASSERT(!IsCalled);
            UNIT_ASSERT(value == 57);
            IsCalled = true;
        }
    };

    void TestSubsribe()
    {
        TAsyncResult<int> result;
        TTestSubscriber::TPtr subscriber1 = new TTestSubscriber(),
                              subscriber2 = new TTestSubscriber();
        result.Subscribe(subscriber1.Get());
        result.Set(57);
        UNIT_ASSERT(subscriber1->IsCalled);
        result.Subscribe(subscriber2.Get());
        UNIT_ASSERT(subscriber2->IsCalled);
    }

    static void* TestThreadRun(void* param)
    {
        Sleep(TDuration::Seconds(0.1));
        TAsyncResult<int>* result = static_cast<TAsyncResult<int>*>(param);
        result->Set(57);
        return NULL;
    }

    void TestAsync()
    {
        TAsyncResult<int>::TPtr result = new TAsyncResult<int>();
        TTestSubscriber::TPtr subscriber1 = new TTestSubscriber(),
                                subscriber2 = new TTestSubscriber();
        TThread thread(&TestThreadRun, result.Get());
        result->Subscribe(subscriber1.Get());
        thread.Start();
        thread.Join();
        UNIT_ASSERT(subscriber1->IsCalled);
        result->Subscribe(subscriber2.Get());
        UNIT_ASSERT(subscriber2->IsCalled);
    }
};

UNIT_TEST_SUITE_REGISTRATION(TAsyncResultTest);

} // namespace NYT
