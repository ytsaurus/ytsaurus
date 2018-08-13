#include <yt/core/test_framework/framework.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/fair_share_invoker_pool.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/actions/invoker_util.h>

#include <yt/core/misc/lazy_ptr.h>

#include <util/datetime/base.h>

#include <algorithm>
#include <array>
#include <utility>

namespace NYT {
namespace NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

static const auto Quantum = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TMockFairShareCallbackQueue
    : public IFairShareCallbackQueue
{
public:
    explicit TMockFairShareCallbackQueue(int bucketCount)
        : UnderlyingCallbackQueue_(CreateFairShareCallbackQueue(bucketCount))
        , TotalCpuTime_(bucketCount)
    { }

    virtual void Enqueue(TClosure callback, int bucketIndex) override
    {
        UnderlyingCallbackQueue_->Enqueue(std::move(callback), bucketIndex);
    }

    virtual bool TryDequeue(TClosure* resultCallback, int* resultBucketIndex) override
    {
        return UnderlyingCallbackQueue_->TryDequeue(resultCallback, resultBucketIndex);
    }

    virtual void AccountCpuTime(int bucketIndex, NProfiling::TCpuDuration cpuTime) override
    {
        YCHECK(IsValidBucketIndex(bucketIndex));
        TotalCpuTime_[bucketIndex] += cpuTime;
        UnderlyingCallbackQueue_->AccountCpuTime(bucketIndex, cpuTime);
    }

    NProfiling::TCpuDuration GetTotalCpuTime(int bucketIndex) const
    {
        YCHECK(IsValidBucketIndex(bucketIndex));
        return TotalCpuTime_[bucketIndex];
    }

private:
    const IFairShareCallbackQueuePtr UnderlyingCallbackQueue_;
    std::vector<std::atomic<NProfiling::TCpuDuration>> TotalCpuTime_;

    bool IsValidBucketIndex(int bucketIndex) const
    {
        return 0 <= bucketIndex && bucketIndex < TotalCpuTime_.size();
    }
};

using TMockFairShareCallbackQueuePtr = TIntrusivePtr<TMockFairShareCallbackQueue>;

////////////////////////////////////////////////////////////////////////////////

class TFairShareInvokerPoolTest
    : public ::testing::Test
{
protected:
    std::array<TLazyIntrusivePtr<TActionQueue>, 2> Queues_;

    THashMap<IInvoker*, int> InvokerToIndex_;

    TMockFairShareCallbackQueuePtr MockCallbackQueue;

    struct TInvocationOrder
    {
        TSpinLock Lock_;
        std::vector<int> InvokerIndexes_;
    } InvocationOrder_;

    virtual void TearDown() override
    {
        for (int i = 0; i < Queues_.size(); ++i) {
            if (Queues_[i]) {
                Queues_[i]->Shutdown();
            }
        }
    }

    template <typename TInvokerPoolPtr>
    void InitializeInvokerToIndexMapping(const TInvokerPoolPtr& invokerPool, int invokerCount)
    {
        YCHECK(invokerCount > 0);
        InvokerToIndex_.clear();
        for (int i = 0; i < invokerCount; ++i) {
            auto invoker = invokerPool->GetInvoker(i);
            InvokerToIndex_[invoker.Get()] = i;
        }
    }

    int GetInvokerIndex(IInvoker* invokerAddress) const
    {
        auto it = InvokerToIndex_.find(invokerAddress);
        YCHECK(it != InvokerToIndex_.end());
        return it->second;
    }

    int GetCurrentInvokerIndex() const
    {
        return GetInvokerIndex(GetCurrentInvoker().Get());
    }

    void ClearInvocationOrder()
    {
        auto guard = Guard(InvocationOrder_.Lock_);
        InvocationOrder_.InvokerIndexes_.clear();
    }

    void PushInvokerIndexToInvocationOrder()
    {
        auto currentInvokerIndex = GetCurrentInvokerIndex();
        auto guard = Guard(InvocationOrder_.Lock_);
        InvocationOrder_.InvokerIndexes_.push_back(currentInvokerIndex);
    }

    std::vector<int> GetInvocationOrder()
    {
        auto guard = Guard(InvocationOrder_.Lock_);
        return InvocationOrder_.InvokerIndexes_;
    }

    IInvokerPoolPtr CreateInvokerPool(IInvokerPtr underlyingInvoker, int invokerCount)
    {
        auto result = CreateFairShareInvokerPool(
            std::move(underlyingInvoker),
            invokerCount,
            [this] (int bucketCount) {
                YCHECK(bucketCount > 0);
                MockCallbackQueue = New<TMockFairShareCallbackQueue>(bucketCount);
                return MockCallbackQueue;
            });
        InitializeInvokerToIndexMapping(result, invokerCount);
        return result;
    }

    void ExpectInvokerIndex(int invokerIndex)
    {
        EXPECT_EQ(invokerIndex, GetCurrentInvokerIndex());
    }

    void ExpectTotalCpuTime(int bucketIndex, TDuration expectedCpuTime, TDuration precision = Quantum / 2)
    {
        // Push dummy callback to the scheduler queue and synchronously wait for it
        // to ensure that all possible cpu time accounters were destroyed during fiber stack unwinding.
        for (int i = 0; i < Queues_.size(); ++i) {
            if (Queues_[i]) {
                auto invoker = Queues_[i]->GetInvoker();
                BIND([] { }).AsyncVia(invoker).Run().Get().ThrowOnError();
            }
        }

        auto precisionValue = NProfiling::DurationToValue(precision);
        auto expectedValue = NProfiling::DurationToValue(expectedCpuTime);
        auto actualValue = NProfiling::CpuDurationToValue(MockCallbackQueue->GetTotalCpuTime(bucketIndex));
        EXPECT_GT(precisionValue, std::abs(expectedValue - actualValue));
    }

    void DoTestFairness(IInvokerPoolPtr invokerPool, int invokerCount)
    {
        YCHECK(1 < invokerCount && invokerCount < 5);

        // Each invoker executes some number of callbacks of the same duration |Quantum * (2 ^ #invokerIndex)|.
        // Individual duration of callback and number of callbacks chosen
        // such that total duration is same for all invokers.
        auto getWeight = [] (int invokerIndex) {
            return (1 << invokerIndex);
        };
        auto getSpinDuration = [getWeight] (int invokerIndex) {
            return Quantum * getWeight(invokerIndex);
        };
        auto getCallbackCount = [getWeight, invokerCount] (int invokerIndex) {
            // Weights are supposed to be in the ascending order.
            return 4 * getWeight(invokerCount - 1) / getWeight(invokerIndex);
        };

        std::vector<TFuture<void>> futures;
        for (int i = 0; i < invokerCount; ++i) {
            for (int j = 0, callbackCount = getCallbackCount(i); j < callbackCount; ++j) {
                futures.push_back(
                    BIND([this, spinDuration = getSpinDuration(i)] {
                        PushInvokerIndexToInvocationOrder();
                        Spin(spinDuration);
                    }).AsyncVia(invokerPool->GetInvoker(i)).Run());
            }
        }

        Combine(futures).Get().ThrowOnError();

        auto invocationOrder = GetInvocationOrder();

        // Test is considered successful if at any moment of the execution
        // deviation of the weighted count of executed callbacks per invoker
        // is not greater than the threshold (see in the code below).
        std::vector<int> invocationCount(invokerCount);
        for (auto invokerIndex : invocationOrder) {
            YCHECK(0 <= invokerIndex && invokerIndex < invokerCount);

            ++invocationCount[invokerIndex];

            auto getWeightedInvocationCount = [getWeight, &invocationCount] (int invokerIndex) {
                return invocationCount[invokerIndex] * getWeight(invokerIndex);
            };

            auto minWeightedInvocationCount = getWeightedInvocationCount(0);
            auto maxWeightedInvocationCount = minWeightedInvocationCount;
            for (int i = 0; i < invokerCount; ++i) {
                auto weightedInvocationCount = getWeightedInvocationCount(i);
                minWeightedInvocationCount = std::min(minWeightedInvocationCount, weightedInvocationCount);
                maxWeightedInvocationCount = std::max(maxWeightedInvocationCount, weightedInvocationCount);
            }

            // Compare threshold and deviation.
            EXPECT_GE(getWeight(invokerCount - 1), maxWeightedInvocationCount - minWeightedInvocationCount);
        }

        for (int i = 0; i < invokerCount; ++i) {
            EXPECT_EQ(getCallbackCount(i), invocationCount[i]);
        }
    }

    void DoTestFairness(int invokerCount)
    {
        DoTestFairness(
            CreateInvokerPool(Queues_[0]->GetInvoker(), invokerCount),
            invokerCount);
    }

    void DoTestSwitchTo(int switchToCount)
    {
        YCHECK(switchToCount > 0);

        auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), switchToCount + 1);

        auto callback = BIND([this, invokerPool, switchToCount] () {
            for (int i = 1; i <= switchToCount; ++i) {
                ExpectInvokerIndex(i - 1);
                Spin(Quantum * i);
                SwitchTo(invokerPool->GetInvoker(i));
            }
            ExpectInvokerIndex(switchToCount);
            Spin(Quantum * (switchToCount + 1));
        }).AsyncVia(invokerPool->GetInvoker(0));

        callback.Run().Get().ThrowOnError();

        for (int i = 0; i <= switchToCount; ++i) {
            ExpectTotalCpuTime(i, Quantum * (i + 1));
        }
    }

    void DoTestWaitFor(int waitForCount)
    {
        YCHECK(waitForCount > 0);

        auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 2);

        auto callback = BIND([waitForCount] {
            Spin(Quantum);
            for (int i = 0; i < waitForCount; ++i) {
                TDelayedExecutor::WaitForDuration(Quantum);
                Spin(Quantum);
            }
        }).AsyncVia(invokerPool->GetInvoker(0));

        callback.Run().Get().ThrowOnError();

        ExpectTotalCpuTime(0, Quantum * (waitForCount + 1));
        ExpectTotalCpuTime(1, TDuration::Zero());
    }

    static void Spin(TDuration duration)
    {
        NProfiling::TCpuTimer timer;
        while (timer.GetElapsedTime() < duration) {
        }
    }
};

TEST_F(TFairShareInvokerPoolTest, Fairness2)
{
    DoTestFairness(2);
}

TEST_F(TFairShareInvokerPoolTest, Fairness3)
{
    DoTestFairness(3);
}

TEST_F(TFairShareInvokerPoolTest, Fairness4)
{
    DoTestFairness(4);
}

TEST_F(TFairShareInvokerPoolTest, SwitchTo12)
{
    DoTestSwitchTo(1);
}

TEST_F(TFairShareInvokerPoolTest, SwitchTo123)
{
    DoTestSwitchTo(2);
}

TEST_F(TFairShareInvokerPoolTest, SwitchTo1234)
{
    DoTestSwitchTo(3);
}

TEST_F(TFairShareInvokerPoolTest, SwitchTo121)
{
    auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 2);

    auto callback = BIND([this, invokerPool] {
        SwitchTo(invokerPool->GetInvoker(0));
        ExpectInvokerIndex(0);
        Spin(Quantum);

        SwitchTo(invokerPool->GetInvoker(1));
        ExpectInvokerIndex(1);
        Spin(Quantum * 3);

        SwitchTo(invokerPool->GetInvoker(0));
        ExpectInvokerIndex(0);
        Spin(Quantum);
    }).AsyncVia(invokerPool->GetInvoker(0));

    callback.Run().Get().ThrowOnError();

    ExpectTotalCpuTime(0, Quantum * 2);
    ExpectTotalCpuTime(1, Quantum * 3);
}

TEST_F(TFairShareInvokerPoolTest, SwitchTo111AndSwitchTo222)
{
    auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 2);

    std::vector<TFuture<void>> futures;

    futures.push_back(
        BIND([this] {
            ExpectInvokerIndex(0);
            Spin(Quantum);
            SwitchTo(GetCurrentInvoker());

            ExpectInvokerIndex(0);
            Spin(Quantum);
            SwitchTo(GetCurrentInvoker());

            ExpectInvokerIndex(0);
            Spin(Quantum);
        }).AsyncVia(invokerPool->GetInvoker(0)).Run());

    futures.push_back(
        BIND([this] {
            ExpectInvokerIndex(1);
            Spin(Quantum);
            SwitchTo(GetCurrentInvoker());

            ExpectInvokerIndex(1);
            Spin(Quantum);
            SwitchTo(GetCurrentInvoker());

            ExpectInvokerIndex(1);
            Spin(Quantum);
        }).AsyncVia(invokerPool->GetInvoker(1)).Run());

    Combine(futures).Get().ThrowOnError();

    ExpectTotalCpuTime(0, Quantum * 3);
    ExpectTotalCpuTime(1, Quantum * 3);
}

TEST_F(TFairShareInvokerPoolTest, WaitFor1)
{
    DoTestWaitFor(1);
}

TEST_F(TFairShareInvokerPoolTest, WaitFor2)
{
    DoTestWaitFor(2);
}

TEST_F(TFairShareInvokerPoolTest, WaitFor3)
{
    DoTestWaitFor(3);
}

TEST_F(TFairShareInvokerPoolTest, CpuTimeAccountingBetweenContextSwitchesIsNotSupportedYet)
{
    auto threadPool = New<TThreadPool>(2, "ThreadPool");
    auto invokerPool = CreateInvokerPool(threadPool->GetInvoker(), 2);

    TEvent started;

    // Start busy loop in the first thread via first fair share invoker.
    auto future = BIND([this, &started] {
        Spin(Quantum * 10);

        auto invocationOrder = GetInvocationOrder();
        EXPECT_TRUE(invocationOrder.empty());

        started.NotifyOne();

        Spin(Quantum * 50);

        invocationOrder = GetInvocationOrder();
        EXPECT_TRUE(!invocationOrder.empty());
    }).AsyncVia(invokerPool->GetInvoker(0)).Run();

    YCHECK(started.Wait(TInstant::Now() + Quantum * 100));

    // After 10 quantums of time (see notification of the #started variable) we start Fairness test in the second thread.
    // In case of better implementation we expect to have non-fair cpu time distribution between first and second invokers,
    // because first invoker is given more cpu time in the first thread (at least within margin of 10 quantums).
    // But cpu accounting is not supported for running callbacks, therefore we expect Fairness test to pass.
    DoTestFairness(invokerPool, 2);

    future.Get().ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NConcurrency
} // namespace NYT
