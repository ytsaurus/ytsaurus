#include <yt/yt/flow/library/cpp/distributed_throttler/bucket.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow::NDistributedThrottler {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerBucketTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ActionQueue_ = New<TActionQueue>("Test");
    }

    void TearDown() override
    {
        // Fire Stop(); ActionQueue shutdown drains the fiber before the queue dies.
        for (const auto& bucket : Buckets_) {
            bucket->Stop();
        }
        Buckets_.clear();
        ActionQueue_->Shutdown();
    }

    TDistributedThrottlerBucketPtr CreateBucket(std::optional<double> limit = std::nullopt)
    {
        auto config = New<TThroughputThrottlerConfig>();
        config->Limit = limit;
        auto bucket = New<TDistributedThrottlerBucket>(
            std::move(config),
            TDuration::MilliSeconds(50),
            ActionQueue_->GetInvoker(),
            NLogging::TLogger("Test"));
        bucket->Start();
        Buckets_.push_back(bucket);
        return bucket;
    }

    std::vector<TDistributedThrottlerBucketPtr> Buckets_;

    // Wait for real time to pass so tokens refill naturally.
    // Unlike SetLastUpdated, this affects already-pending Throttle() calls.
    void WaitForRefill(TDuration duration = TDuration::MilliSeconds(500))
    {
        Sleep(duration);
    }

    TActionQueuePtr ActionQueue_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedThrottlerBucketTest, SingleRequest)
{
    auto bucket = CreateBucket(/*limit*/ 1000);
    auto future = bucket->RequestQuota("client1", 1, /*timestamp*/ 100);
    Sleep(TDuration::MilliSeconds(200));
    ASSERT_TRUE(future.IsSet());
    ASSERT_TRUE(future.TryGet()->IsOK());
}

TEST_F(TDistributedThrottlerBucketTest, UnlimitedBucket)
{
    auto bucket = CreateBucket(/*limit*/ std::nullopt);
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 100; ++i) {
        futures.push_back(bucket->RequestQuota("client1", 1000, /*timestamp*/ i));
    }
    Sleep(TDuration::MilliSeconds(500));
    for (const auto& f : futures) {
        ASSERT_TRUE(f.IsSet());
        ASSERT_TRUE(f.TryGet()->IsOK());
    }
}

TEST_F(TDistributedThrottlerBucketTest, PriorityOrdering)
{
    // Limit=1 token / 1s. Request amount=2 each, so token accounting always enters
    // the token bucket's slow lane and strict sequential resolution is observable.
    auto bucket = CreateBucket(/*limit*/ 1);

    auto futureC = bucket->RequestQuota("C", 2, /*timestamp*/ 300);
    auto futureA = bucket->RequestQuota("A", 2, /*timestamp*/ 100);
    auto futureB = bucket->RequestQuota("B", 2, /*timestamp*/ 200);

    std::vector<std::string> resolveOrder;
    std::mutex orderMutex;
    futureA.Subscribe(BIND([&] (const TError&) {
        std::lock_guard lk(orderMutex);
        resolveOrder.push_back("A");
    }));
    futureB.Subscribe(BIND([&] (const TError&) {
        std::lock_guard lk(orderMutex);
        resolveOrder.push_back("B");
    }));
    futureC.Subscribe(BIND([&] (const TError&) {
        std::lock_guard lk(orderMutex);
        resolveOrder.push_back("C");
    }));

    // Each amount=2 needs ~2s to accumulate.
    WaitForRefill(TDuration::Seconds(8));

    ASSERT_TRUE(futureA.IsSet());
    ASSERT_TRUE(futureB.IsSet());
    ASSERT_TRUE(futureC.IsSet());

    std::lock_guard lk(orderMutex);
    ASSERT_EQ(resolveOrder.size(), 3u);
    EXPECT_EQ(resolveOrder[0], "A") << "A has lowest timestamp, should resolve first";
    EXPECT_EQ(resolveOrder[1], "B");
    EXPECT_EQ(resolveOrder[2], "C");
}

TEST_F(TDistributedThrottlerBucketTest, CancelledRequest)
{
    auto bucket = CreateBucket(/*limit*/ 1);

    // Exhaust initial token.
    Y_UNUSED(bucket->RequestQuota("warmup", 1, 0));

    auto futureA = bucket->RequestQuota("A", 1, /*timestamp*/ 100);
    auto futureB = bucket->RequestQuota("B", 1, /*timestamp*/ 200);

    // Cancel A. Fiber may already be blocked in WaitFor(Throttle(1)) for A, but
    // when A's token eventually arrives, TrySet on the cancelled promise is a no-op
    // and fiber proceeds to B.
    Sleep(TDuration::MilliSeconds(100));
    futureA.Cancel(TError("cancelled"));

    WaitForRefill(TDuration::MilliSeconds(2500));

    EXPECT_TRUE(futureB.IsSet()) << "B should eventually be served";
}

TEST_F(TDistributedThrottlerBucketTest, ReconfigureToUnlimited)
{
    auto bucket = CreateBucket(/*limit*/ 1);

    // Exhaust.
    Y_UNUSED(bucket->RequestQuota("warmup", 1, 0));

    // Enqueue many.
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 50; ++i) {
        futures.push_back(bucket->RequestQuota("client", 1, i));
    }

    // Reconfigure to unlimited.
    auto unlimitedConfig = New<TThroughputThrottlerConfig>();
    bucket->Reconfigure(unlimitedConfig);
    Sleep(TDuration::MilliSeconds(200));

    // All should be resolved.
    for (const auto& f : futures) {
        EXPECT_TRUE(static_cast<bool>(f.IsSet()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDistributedThrottler
