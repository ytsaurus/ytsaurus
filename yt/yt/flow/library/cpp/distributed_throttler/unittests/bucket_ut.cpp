#include <yt/yt/flow/library/cpp/distributed_throttler/bucket.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <atomic>

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
        auto config = New<TDistributedThrottlerBucketConfig>();
        config->Throttler->Limit = limit;
        return CreateBucket(std::move(config), true);
    }

    TDistributedThrottlerBucketPtr CreateBucket(
        TDistributedThrottlerBucketConfigPtr config,
        bool start)
    {
        auto bucket = New<TDistributedThrottlerBucket>(
            std::move(config),
            TDuration::MilliSeconds(50),
            ActionQueue_->GetInvoker(),
            NLogging::TLogger("Test"));
        if (start) {
            bucket->Start();
        }
        Buckets_.push_back(bucket);
        return bucket;
    }

    static TDistributedThrottlerBucketConfigPtr MakeClassConfig(
        THashMap<TQuotaClassId, double> classWeights,
        std::optional<i64> maxGrantAmount = {})
    {
        auto config = New<TDistributedThrottlerBucketConfig>();
        config->ClassWeights = std::move(classWeights);
        config->MaxGrantAmount = maxGrantAmount;
        return config;
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
    auto future = bucket->RequestQuota("client1", DefaultQuotaClassId, 1, /*timestamp*/ 100);
    Sleep(TDuration::MilliSeconds(200));
    ASSERT_TRUE(future.IsSet());
    ASSERT_TRUE(future.TryGet()->IsOK());
}

TEST_F(TDistributedThrottlerBucketTest, UnlimitedBucket)
{
    auto bucket = CreateBucket(/*limit*/ std::nullopt);
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 100; ++i) {
        futures.push_back(bucket->RequestQuota("client1", DefaultQuotaClassId, 1000, /*timestamp*/ i));
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

    auto futureC = bucket->RequestQuota("C", DefaultQuotaClassId, 2, /*timestamp*/ 300);
    auto futureA = bucket->RequestQuota("A", DefaultQuotaClassId, 2, /*timestamp*/ 100);
    auto futureB = bucket->RequestQuota("B", DefaultQuotaClassId, 2, /*timestamp*/ 200);

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
    Y_UNUSED(bucket->RequestQuota("warmup", DefaultQuotaClassId, 1, 0));

    auto futureA = bucket->RequestQuota("A", DefaultQuotaClassId, 1, /*timestamp*/ 100);
    auto futureB = bucket->RequestQuota("B", DefaultQuotaClassId, 1, /*timestamp*/ 200);

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
    Y_UNUSED(bucket->RequestQuota("warmup", DefaultQuotaClassId, 1, 0));

    // Enqueue many.
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 50; ++i) {
        futures.push_back(bucket->RequestQuota("client", DefaultQuotaClassId, 1, i));
    }

    // Reconfigure to unlimited.
    auto unlimitedConfig = New<TDistributedThrottlerBucketConfig>();
    bucket->Reconfigure(unlimitedConfig);
    Sleep(TDuration::MilliSeconds(200));

    // All should be resolved.
    for (const auto& f : futures) {
        EXPECT_TRUE(static_cast<bool>(f.IsSet()));
    }
}

TEST_F(TDistributedThrottlerBucketTest, SameClassKeepsLagOrder)
{
    auto bucket = CreateBucket(MakeClassConfig({{"a", 1.0}}), false);
    std::vector<std::string> order;
    auto first = bucket->RequestQuota("first", "a", 1, 300);
    auto second = bucket->RequestQuota("second", "a", 1, 100);
    auto third = bucket->RequestQuota("third", "a", 1, 200);
    first.Subscribe(BIND([&] (const TError&) {
        order.push_back("first");
    }));
    second.Subscribe(BIND([&] (const TError&) {
        order.push_back("second");
    }));
    third.Subscribe(BIND([&] (const TError&) {
        order.push_back("third");
    }));

    bucket->Start();
    WaitFor(AllSucceeded(std::vector{first, second, third})).ThrowOnError();

    EXPECT_EQ(order, (std::vector<std::string>{"second", "third", "first"}));
}

TEST_F(TDistributedThrottlerBucketTest, NoConfiguredClassesMatchesLegacyOrder)
{
    auto bucket = CreateBucket(MakeClassConfig({}), false);
    std::vector<std::string> order;
    auto newer = bucket->RequestQuota("newer", DefaultQuotaClassId, 1, 200);
    auto older = bucket->RequestQuota("older", DefaultQuotaClassId, 1, 100);
    newer.Subscribe(BIND([&] (const TError&) {
        order.push_back("newer");
    }));
    older.Subscribe(BIND([&] (const TError&) {
        order.push_back("older");
    }));

    bucket->Start();
    WaitFor(AllSucceeded(std::vector{newer, older})).ThrowOnError();

    EXPECT_EQ(order, (std::vector<std::string>{"older", "newer"}));
}

TEST_F(TDistributedThrottlerBucketTest, UnknownAndEmptyClassUseDefault)
{
    auto bucket = CreateBucket(MakeClassConfig({{"known", 1.0}}), false);
    std::vector<std::string> order;
    auto unknown = bucket->RequestQuota("unknown", "unknown", 1, 300);
    auto empty = bucket->RequestQuota("empty", "", 1, 100);
    unknown.Subscribe(BIND([&] (const TError&) {
        order.push_back("unknown");
    }));
    empty.Subscribe(BIND([&] (const TError&) {
        order.push_back("empty");
    }));

    bucket->Start();
    WaitFor(AllSucceeded(std::vector{unknown, empty})).ThrowOnError();

    EXPECT_EQ(order, (std::vector<std::string>{"empty", "unknown"}));
}

TEST_F(TDistributedThrottlerBucketTest, CanceledBeforeDispatchDoesNotBlockOtherClass)
{
    auto bucket = CreateBucket(MakeClassConfig({{"a", 1.0}, {"b", 1.0}}), false);
    auto canceled = bucket->RequestQuota("canceled", "a", 10, 0);
    auto other = bucket->RequestQuota("other", "b", 1, 1000);
    canceled.Cancel(TError("canceled"));

    bucket->Start();

    EXPECT_TRUE(WaitFor(other).IsOK());
    EXPECT_FALSE(WaitFor(canceled).IsOK());
}

TEST_F(TDistributedThrottlerBucketTest, ChunkingInterleavesClasses)
{
    auto bucket = CreateBucket(
        MakeClassConfig({{"bulk", 1.0}, {"vip", 1.0}}, 1),
        false);
    std::vector<std::string> order;
    auto bulk = bucket->RequestQuota("bulk", "bulk", 10, 0);
    auto vip = bucket->RequestQuota("vip", "vip", 1, 1000);
    bulk.Subscribe(BIND([&] (const TError&) {
        order.push_back("bulk");
    }));
    vip.Subscribe(BIND([&] (const TError&) {
        order.push_back("vip");
    }));

    bucket->Start();
    WaitFor(AllSucceeded(std::vector{bulk, vip})).ThrowOnError();

    ASSERT_EQ(order.size(), 2u);
    EXPECT_EQ(order.front(), "vip");
}

TEST_F(TDistributedThrottlerBucketTest, ChunkedRequestResolvesOnceAfterFullAmount)
{
    auto bucket = CreateBucket(MakeClassConfig({{"bulk", 1.0}}, 1), false);
    std::atomic<int> completionCount = 0;
    auto future = bucket->RequestQuota("bulk", "bulk", 5, 0);
    future.Subscribe(BIND([&] (const TError&) {
        ++completionCount;
    }));

    bucket->Start();
    EXPECT_TRUE(WaitFor(future).IsOK());
    EXPECT_EQ(completionCount.load(), 1);
}

TEST_F(TDistributedThrottlerBucketTest, UnchunkedRequestKeepsCompatibilityGranularity)
{
    auto bucket = CreateBucket(
        MakeClassConfig({{"bulk", 1.0}, {"vip", 1.0}}),
        false);
    std::vector<std::string> order;
    auto bulk = bucket->RequestQuota("bulk", "bulk", 10, 0);
    auto vip = bucket->RequestQuota("vip", "vip", 1, 1000);
    bulk.Subscribe(BIND([&] (const TError&) {
        order.push_back("bulk");
    }));
    vip.Subscribe(BIND([&] (const TError&) {
        order.push_back("vip");
    }));

    bucket->Start();
    WaitFor(AllSucceeded(std::vector{bulk, vip})).ThrowOnError();

    ASSERT_EQ(order.size(), 2u);
    EXPECT_EQ(order.front(), "bulk");
}

TEST_F(TDistributedThrottlerBucketTest, StopWithoutStartIsNoop)
{
    auto bucket = CreateBucket(MakeClassConfig({}), false);
    bucket->Stop();
}

TEST_F(TDistributedThrottlerBucketTest, CanceledDispatchedRequestUnblocksDrainFiber)
{
    auto bucket = CreateBucket(/*limit*/ 1);

    // Overdraft the token bucket so subsequent requests really wait in it.
    Y_UNUSED(bucket->RequestQuota("warmup", DefaultQuotaClassId, 5, 0));

    // Without cancellation propagating into the token-bucket wait, this grant
    // consumes 1000 tokens once the deficit clears and the next request then
    // waits ~1000s; with propagation it is dropped without consuming anything.
    auto big = bucket->RequestQuota("big", DefaultQuotaClassId, 1000, /*timestamp*/ 100);
    auto small = bucket->RequestQuota("small", DefaultQuotaClassId, 1, /*timestamp*/ 200);

    Sleep(TDuration::MilliSeconds(200));
    big.Cancel(TError("canceled"));

    // Deficit from the warmup clears in ~5s; give it a bit more.
    WaitForRefill(TDuration::Seconds(12));
    EXPECT_TRUE(small.IsSet()) << "canceled in-flight request must not block the queue";
}

TEST_F(TDistributedThrottlerBucketTest, ChunkedRequestCanceledMidWayDoesNotBlockOthers)
{
    auto config = MakeClassConfig({{"a", 1.0}, {"b", 1.0}}, /*maxGrantAmount*/ 1);
    config->Throttler->Limit = 1;
    auto bucket = CreateBucket(std::move(config), true);

    auto chunked = bucket->RequestQuota("chunked", "a", 1000, /*timestamp*/ 0);

    // Let a couple of chunks through, then cancel with granted history behind.
    Sleep(TDuration::Seconds(2));
    chunked.Cancel(TError("canceled"));

    auto other = bucket->RequestQuota("other", "b", 1, /*timestamp*/ 0);
    WaitForRefill(TDuration::Seconds(5));

    EXPECT_TRUE(other.IsSet());
    EXPECT_FALSE(WaitFor(chunked).IsOK());
}

TEST_F(TDistributedThrottlerBucketTest, RetiredClassIsErasedWhenIdleAtReconfigure)
{
    // Reconfigure marks a dropped class non-accepting rather than erasing it,
    // because it may still have queued requests. An idle one must be gone by
    // the time Reconfigure returns.
    auto bucket = CreateBucket(MakeClassConfig({{"a", 1.0}, {"b", 1.0}}), /*start*/ true);
    EXPECT_TRUE(WaitFor(bucket->RequestQuota("a", "a", 1, /*timestamp*/ 0)).IsOK());

    bucket->Reconfigure(MakeClassConfig({{"b", 1.0}}));

    // A request for the erased class is served from the default class, not
    // from a stale entry that kept its old weight.
    EXPECT_TRUE(WaitFor(bucket->RequestQuota("a", "a", 1, /*timestamp*/ 0)).IsOK());
}

TEST_F(TDistributedThrottlerBucketTest, RetiredClassWithBacklogIsErasedAfterDraining)
{
    // Same, but the class still has queued work when it is dropped: it must
    // drain first and only then disappear.
    auto config = MakeClassConfig({{"a", 1.0}, {"b", 1.0}}, /*maxGrantAmount*/ 1);
    config->Throttler->Limit = 20;
    config->Throttler->Period = TDuration::MilliSeconds(100);
    auto bucket = CreateBucket(std::move(config), /*start*/ true);

    auto pending = bucket->RequestQuota("a", "a", 4, /*timestamp*/ 0);
    auto retiring = MakeClassConfig({{"b", 1.0}}, /*maxGrantAmount*/ 1);
    retiring->Throttler->Limit = 20;
    retiring->Throttler->Period = TDuration::MilliSeconds(100);
    bucket->Reconfigure(std::move(retiring));

    // The in-flight request of the retired class still completes.
    EXPECT_TRUE(WaitFor(pending).IsOK());
    EXPECT_TRUE(WaitFor(bucket->RequestQuota("b", "b", 1, /*timestamp*/ 0)).IsOK());
}

TEST_F(TDistributedThrottlerBucketTest, RemovedClassRoutesNewRequestsToDefault)
{
    auto bucket = CreateBucket(MakeClassConfig({{"retired", 1.0}}), false);
    auto oldRequest = bucket->RequestQuota("old", "retired", 1, 100);

    bucket->Reconfigure(MakeClassConfig({}));

    std::vector<std::string> order;
    auto newRequest = bucket->RequestQuota("new", "retired", 1, 0);
    oldRequest.Subscribe(BIND([&] (const TError&) {
        order.push_back("old");
    }));
    newRequest.Subscribe(BIND([&] (const TError&) {
        order.push_back("new");
    }));
    bucket->Start();
    WaitFor(AllSucceeded(std::vector{oldRequest, newRequest})).ThrowOnError();

    ASSERT_EQ(order.size(), 2u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDistributedThrottler
