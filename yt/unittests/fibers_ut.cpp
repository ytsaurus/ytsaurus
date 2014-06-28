#include "stdafx.h"
#include "framework.h"
#include "probe.h"

#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TFiberTest
    : public ::testing::Test
{
protected:
    TLazyIntrusivePtr<TActionQueue> Queue1;
    TLazyIntrusivePtr<TActionQueue> Queue2;

    virtual void TearDown()
    {
        if (Queue1.HasValue()) {
            Queue1->Shutdown();
        }
        if (Queue2.HasValue()) {
            Queue2->Shutdown();
        }
    }

};

TEST_F(TFiberTest, WaitForInSerializedInvoker)
{
    auto invoker = CreateSerializedInvoker(Queue1->GetInvoker());
    BIND([&] () {
        for (int i = 0; i < 10; ++i) {
            WaitFor(MakeDelayed(TDuration::MilliSeconds(10)));
        }
    }).AsyncVia(invoker).Run().Get();
}

TEST_F(TFiberTest, WaitForInBoundedConcurrencyInvoker1)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);
    BIND([&] () {
        for (int i = 0; i < 10; ++i) {
            WaitFor(MakeDelayed(TDuration::MilliSeconds(10)));
        }
    }).AsyncVia(invoker).Run().Get();
}

TEST_F(TFiberTest, WaitForInBoundedConcurrencyInvoker2)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 2);

    auto promise = NewPromise<void>();

    auto a1 = BIND([&] () {
        promise.Set();
    });

    auto a2 = BIND([&] () {
        invoker->Invoke(a1);
        WaitFor(promise);
    });

    a2.AsyncVia(invoker).Run().Get();
}

TEST_F(TFiberTest, WaitForInBoundedConcurrencyInvoker3)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);

    auto promise = NewPromise<void>();

    bool a1finished = false;
    auto a1 = BIND([&] () {
        WaitFor(promise);
        a1finished = true;
    });

    bool a2called = false;
    auto a2 = BIND([&] () {
        a2called = true;
    });

    invoker->Invoke(a1);
    invoker->Invoke(a2);

    Sleep(TDuration::MilliSeconds(10));
    EXPECT_FALSE(a1finished);
    EXPECT_FALSE(a2called);
    promise.Set();
    Sleep(TDuration::MilliSeconds(10));
    EXPECT_TRUE(a1finished);
    EXPECT_TRUE(a2called);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NConcurrency
} // namespace NYT

