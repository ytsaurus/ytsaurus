#include <yt/core/test_framework/framework.h>

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/async_expiring_cache.h>

#include <random>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSimpleExpiringCache
    : public TAsyncExpiringCache<int, int>
{
public:
    explicit TSimpleExpiringCache(TAsyncExpiringCacheConfigPtr config, float successProbability = 1.0)
        : TAsyncExpiringCache<int, int>(std::move(config))
        , Generator_(RandomDevice_())
        , Bernoulli_(successProbability)
    { }

    int GetCount()
    {
        return Count_;
    }

protected:
    virtual TFuture<int> DoGet(const int& /*key*/, bool /*isPeriodicUpdate*/) noexcept override
    {
        ++Count_;

        return Bernoulli_(Generator_)
            ? MakeFuture<int>(0)
            : MakeFuture<int>(TErrorOr<int>(TError("error")));
    }

private:
    std::random_device RandomDevice_;
    std::mt19937 Generator_;
    std::bernoulli_distribution Bernoulli_;
    std::atomic<int> Count_ = {0};
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T> MakeDelayedFuture(const TDuration& duration, T x)
{
    return TDelayedExecutor::MakeDelayed(duration)
        .Apply(BIND([=] () { return x; }));
}

class TDelayedExpiringCache
    : public TAsyncExpiringCache<int, int>
{
public:
    TDelayedExpiringCache(TAsyncExpiringCacheConfigPtr config, const TDuration& delay)
        : TAsyncExpiringCache<int, int>(std::move(config))
        , Delay_(delay)
    { }

    int GetCount()
    {
        return Count_;
    }

protected:
    virtual TFuture<int> DoGet(const int& /*key*/, bool /*isPeriodicUpdate*/) noexcept override
    {
        int count = ++Count_;
        return MakeDelayedFuture(Delay_, count);
    }

private:
    TDuration Delay_;
    std::atomic<int> Count_ = {0};
};

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncExpiringCacheTest, TestBackgroundUpdate)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->RefreshTime = TDuration::MilliSeconds(100);
    auto cache = New<TSimpleExpiringCache>(config);

    auto start = Now();
    cache->Get(0);
    Sleep(TDuration::MilliSeconds(500));
    int actual = cache->GetCount();
    auto end = Now();

    int duration = (end - start).MilliSeconds();
    int expected = duration / config->RefreshTime->MilliSeconds();

    EXPECT_LE(std::abs(expected - actual), 50);
}

TEST(TAsyncExpiringCacheTest, TestEntryRemoval)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->RefreshTime = TDuration::MilliSeconds(100);
    auto cache = New<TSimpleExpiringCache>(config, 0.9);

    auto threadPool = New<TThreadPool>(10, "CacheAccessorPool");
    std::vector<TFuture<void>> asyncResult;

    for (int i = 0; i < 10; ++i) {
        auto callback = BIND([=] () {
            for (int j = 0; j < 1000; ++j) {
                cache->Get(0);

                if (rand() % 20 == 0) {
                    cache->Invalidate(0);
                }
            }
        });
        asyncResult.push_back(
            callback
            .AsyncVia(threadPool->GetInvoker())
            .Run());
    }
    WaitFor(Combine(asyncResult))
        .ThrowOnError();

    Sleep(TDuration::MilliSeconds(200));

    auto start = Now();
    int begin = cache->GetCount();
    Sleep(TDuration::Seconds(1));
    int actual = cache->GetCount() - begin;
    auto end = Now();

    int duration = (end - start).MilliSeconds();
    int expected = duration / 100;

    EXPECT_GE(expected, actual);
}

TEST(TAsyncExpiringCacheTest, TestAccessTime1)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->RefreshTime = TDuration::MilliSeconds(200);
    config->ExpireAfterAccessTime = TDuration::Zero();
    auto cache = New<TSimpleExpiringCache>(config);

    EXPECT_TRUE(cache->Get(0).IsSet());
    Sleep(TDuration::MilliSeconds(300));

    EXPECT_EQ(1, cache->GetCount());
}

TEST(TAsyncExpiringCacheTest, TestAccessTime2)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->ExpireAfterAccessTime = TDuration::MilliSeconds(100);
    auto cache = New<TSimpleExpiringCache>(config);

    for (int i = 0; i < 10; ++i) {
        cache->Get(0);
        Sleep(TDuration::MilliSeconds(50));
    }


    EXPECT_EQ(1, cache->GetCount());
}

TEST(TAsyncExpiringCacheTest, TestAccessTime3)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->ExpireAfterAccessTime = TDuration::MilliSeconds(50);
    auto cache = New<TSimpleExpiringCache>(config);

    for (int i = 0; i < 10; ++i) {
        cache->Get(0);
        Sleep(TDuration::MilliSeconds(100));
    }

    EXPECT_EQ(10, cache->GetCount());
}

TEST(TAsyncExpiringCacheTest, CacheDoesntRefreshExpiredItem)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->RefreshTime = TDuration::MilliSeconds(200);
    config->ExpireAfterAccessTime = TDuration::MilliSeconds(100);
    auto cache = New<TSimpleExpiringCache>(config);

    EXPECT_TRUE(cache->Get(0).IsSet());
    Sleep(TDuration::MilliSeconds(500));

    EXPECT_EQ(1, cache->GetCount());
}

TEST(TAsyncExpiringCacheTest, TestUpdateTime1)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->ExpireAfterSuccessfulUpdateTime = TDuration::MilliSeconds(50);
    auto cache = New<TSimpleExpiringCache>(config, 1.0);

    for (int i = 0; i < 10; ++i) {
        cache->Get(0);
        Sleep(TDuration::MilliSeconds(100));
    }

    EXPECT_EQ(10, cache->GetCount());
}

TEST(TAsyncExpiringCacheTest, TestUpdateTime2)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->ExpireAfterFailedUpdateTime = TDuration::MilliSeconds(50);
    auto cache = New<TSimpleExpiringCache>(config, 0.0);

    for (int i = 0; i < 10; ++i) {
        cache->Get(0);
        Sleep(TDuration::MilliSeconds(100));
    }

    EXPECT_EQ(10, cache->GetCount());
}

TEST(TAsyncExpiringCacheTest, TestZeroCache1)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->ExpireAfterAccessTime = TDuration::Zero();
    config->ExpireAfterSuccessfulUpdateTime = TDuration::Zero();
    config->ExpireAfterFailedUpdateTime = TDuration::Zero();

    auto cache = New<TDelayedExpiringCache>(config, TDuration::MilliSeconds(200));
    for (int i = 0; i < 10; ++i) {
        auto future = cache->Get(0);
        EXPECT_EQ(i + 1, cache->GetCount());
        Sleep(TDuration::MilliSeconds(100));
        auto valueOrError = future.Get();
        EXPECT_TRUE(valueOrError.IsOK());
        EXPECT_EQ(i + 1, valueOrError.Value());
        Sleep(TDuration::MilliSeconds(100));
    }

    EXPECT_EQ(10, cache->GetCount());
}

TEST(TAsyncExpiringCacheTest, TestZeroCache2)
{
    auto config = New<TAsyncExpiringCacheConfig>();
    config->ExpireAfterAccessTime = TDuration::Zero();
    config->ExpireAfterSuccessfulUpdateTime = TDuration::Zero();
    config->ExpireAfterFailedUpdateTime = TDuration::Zero();

    auto cache = New<TDelayedExpiringCache>(config, TDuration::MilliSeconds(100));
    std::vector<TFuture<int>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(cache->Get(0));
    }

    for (const auto& future : futures) {
        auto result = future.Get();
        EXPECT_TRUE(result.IsOK());
        EXPECT_EQ(1, result.Value());
    }

    EXPECT_EQ(1, cache->GetCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
