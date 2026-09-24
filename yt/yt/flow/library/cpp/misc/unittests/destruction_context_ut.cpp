#include <yt/yt/flow/library/cpp/misc/destruction_context.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/system/event.h>

#include <functional>
#include <stdexcept>
#include <vector>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestructionObservedValue
    : public TRefCounted
{
public:
    explicit TDestructionObservedValue(std::function<void()> onDestroyed)
        : OnDestroyed_(std::move(onDestroyed))
    { }

    ~TDestructionObservedValue() override
    {
        OnDestroyed_();
    }

private:
    const std::function<void()> OnDestroyed_;
};

TEST(TDestructionContextGuardTest, DestroysImmediatelyWithoutGuard)
{
    bool destroyed = false;
    TDestructionContextGuard::Add(New<TDestructionObservedValue>([&] {
        destroyed = true;
    }));
    EXPECT_TRUE(destroyed);
}

TEST(TDestructionContextGuardTest, RetainsValuesUntilOutermostScopeFinishes)
{
    for (bool fail : {false, true}) {
        SCOPED_TRACE(fail);
        bool destroyed = false;
        auto operation = [&] {
            TDestructionContextGuard outer;
            {
                TDestructionContextGuard inner;
                auto value = New<TDestructionObservedValue>([&] {
                    destroyed = true;
                });
                TDestructionContextGuard::Add(value);
                TDestructionContextGuard::Add(std::move(value));
            }
            EXPECT_FALSE(destroyed);
            if (fail) {
                throw std::runtime_error("Operation failed");
            }
        };
        if (fail) {
            EXPECT_THROW(operation(), std::runtime_error);
        } else {
            EXPECT_NO_THROW(operation());
        }
        EXPECT_TRUE(destroyed);
    }
}

TEST(TDestructionContextGuardTest, DestroysValuesInOrder)
{
    std::vector<int> order;
    order.reserve(3);
    {
        TDestructionContextGuard guard;
        for (int index = 0; index < 3; ++index) {
            TDestructionContextGuard::Add(New<TDestructionObservedValue>([&, index] {
                order.push_back(index);
            }));
        }
        EXPECT_TRUE(order.empty());
    }
    EXPECT_EQ(order, (std::vector<int>{0, 1, 2}));
}

TEST(TDestructionContextGuardTest, IsolatesInterleavedFibers)
{
    auto queue = New<NConcurrency::TActionQueue>();
    auto resumeFirst = NewPromise<void>();
    auto resumeSecond = NewPromise<void>();
    TManualEvent firstSuspended;
    TManualEvent secondSuspended;
    TManualEvent firstFinished;
    TManualEvent secondFinished;
    bool firstDestroyed = false;
    bool secondDestroyed = false;

    auto first = BIND([&] {
        TDestructionContextGuard guard;
        TDestructionContextGuard::Add(New<TDestructionObservedValue>([&] {
            firstDestroyed = true;
        }));
        firstSuspended.Signal();
        NConcurrency::WaitFor(resumeFirst.ToFuture())
            .ThrowOnError();
        EXPECT_FALSE(firstDestroyed);
    }).AsyncVia(queue->GetInvoker())
        .Run();
    first.Subscribe(BIND([&] (const TError&) {
        firstFinished.Signal();
    }));

    auto second = BIND([&] {
        TDestructionContextGuard guard;
        TDestructionContextGuard::Add(New<TDestructionObservedValue>([&] {
            secondDestroyed = true;
        }));
        secondSuspended.Signal();
        NConcurrency::WaitFor(resumeSecond.ToFuture())
            .ThrowOnError();
        EXPECT_FALSE(secondDestroyed);
    }).AsyncVia(queue->GetInvoker())
        .Run();
    second.Subscribe(BIND([&] (const TError&) {
        secondFinished.Signal();
    }));

    auto cleanup = Finally([&] {
        resumeFirst.TrySet();
        resumeSecond.TrySet();
        firstFinished.WaitI();
        secondFinished.WaitI();
        queue->Shutdown();
    });
    ASSERT_TRUE(firstSuspended.WaitT(TDuration::Seconds(5)));
    ASSERT_TRUE(secondSuspended.WaitT(TDuration::Seconds(5)));
    resumeFirst.Set();
    ASSERT_TRUE(firstFinished.WaitT(TDuration::Seconds(5)));
    EXPECT_TRUE(firstDestroyed);
    EXPECT_FALSE(secondDestroyed);
    resumeSecond.Set();
    ASSERT_TRUE(secondFinished.WaitT(TDuration::Seconds(5)));
    EXPECT_TRUE(secondDestroyed);
    first.GetOrCrash().ThrowOnError();
    second.GetOrCrash().ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
