#include "framework.h"

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/expiring_cache.h>

#include <random>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSimpleExpiringCache
    : public TExpiringCache<int, int>
{
public:
    TSimpleExpiringCache(TExpiringCacheConfigPtr config, float successProbability = 1.0)
        : TExpiringCache<int, int>(std::move(config))
        , Generator_(RandomDevice_())
        , Bernoulli_(successProbability)
    { }

    int GetCount()
    {
        return Count_;
    }

protected:
    virtual TFuture<int> DoGet(const int&) override
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
    : public TExpiringCache<int, int>
{
public:
    TDelayedExpiringCache(TExpiringCacheConfigPtr config, const TDuration& delay)
        : TExpiringCache<int, int>(std::move(config))
        , Delay_(delay)
    { }

    int GetCount()
    {
        return Count_;
    }

protected:
    virtual TFuture<int> DoGet(const int&) override
    {
        ++Count_;

        int count = Count_;
        return MakeDelayedFuture(Delay_, count);
    }

private:
    TDuration Delay_;
    std::atomic<int> Count_ = {0};
};

////////////////////////////////////////////////////////////////////////////////

TEST(TExpiringCacheTest, TestBackgroundUpdate)
{
    int interval = 100;
    auto config = New<TExpiringCacheConfig>();
    config->RefreshTime = TDuration::MilliSeconds(interval);
    auto cache = New<TSimpleExpiringCache>(config);

    auto start = Now();
    cache->Get(0);
    Sleep(TDuration::Seconds(1));
    int actual = cache->GetCount();
    auto end = Now();

    int duration = (end - start).MilliSeconds();
    int expected = duration / interval;

    EXPECT_EQ(expected, actual);
}

TEST(TExpiringCacheTest, TestEntryRemoval)
{
    int interval = 20;
    auto config = New<TExpiringCacheConfig>();
    config->RefreshTime = TDuration::MilliSeconds(interval);
    auto cache = New<TSimpleExpiringCache>(config, 0.9);

    auto threadPool = New<TThreadPool>(10, "CacheAccessorPool");
    std::vector<TFuture<void>> asyncResult;

    for (int i = 0; i < 10; ++i) {
        auto callback = BIND([=] () {
            for (int i = 0; i < 1000; ++i) {
                cache->Get(0);

                if (rand() % 20 == 0) {
                    cache->TryRemove(0);
                }
            }
        });
        asyncResult.push_back(
            callback
            .AsyncVia(threadPool->GetInvoker())
            .Run());
    }
    WaitFor(Combine(asyncResult));

    Sleep(TDuration::MilliSeconds(2 * interval));

    auto start = Now();
    int begin = cache->GetCount();
    Sleep(TDuration::Seconds(1));
    int actual = cache->GetCount() - begin;
    auto end = Now();

    int duration = (end - start).MilliSeconds();
    int expected = duration / interval;

    EXPECT_GE(expected, actual);
}

TEST(TExpiringCacheTest, TestAccessTime1)
{
    auto config = New<TExpiringCacheConfig>();
    config->RefreshTime = TDuration::MilliSeconds(10);
    config->ExpireAfterAccessTime = TDuration::MilliSeconds(0);
    auto cache = New<TSimpleExpiringCache>(config);

    cache->Get(0);
    Sleep(TDuration::Seconds(1));
    int actual = cache->GetCount();

    EXPECT_EQ(1, actual);
}

TEST(TExpiringCacheTest, TestAccessTime2)
{
    auto config = New<TExpiringCacheConfig>();
    config->ExpireAfterAccessTime = TDuration::MilliSeconds(50);
    auto cache = New<TSimpleExpiringCache>(config);

    for (int i = 0; i < 10; ++i) {
        cache->Get(0);
        Sleep(TDuration::MilliSeconds(30));
    }

    int actual = cache->GetCount();

    EXPECT_EQ(1, actual);
}

TEST(TExpiringCacheTest, TestAccessTime3)
{
    auto config = New<TExpiringCacheConfig>();
    config->ExpireAfterAccessTime = TDuration::MilliSeconds(30);
    auto cache = New<TSimpleExpiringCache>(config);

    for (int i = 0; i < 10; ++i) {
        cache->Get(0);
        Sleep(TDuration::MilliSeconds(50));
    }

    int actual = cache->GetCount();

    EXPECT_EQ(10, actual);
}

TEST(TExpiringCacheTest, TestUpdateTime1)
{
    auto config = New<TExpiringCacheConfig>();
    config->ExpireAfterSuccessfulUpdateTime = TDuration::MilliSeconds(30);
    auto cache = New<TSimpleExpiringCache>(config);

    for (int i = 0; i < 10; ++i) {
        cache->Get(0);
        Sleep(TDuration::MilliSeconds(50));
    }

    int actual = cache->GetCount();

    EXPECT_EQ(10, actual);
}

TEST(TExpiringCacheTest, TestUpdateTime2)
{
    auto config = New<TExpiringCacheConfig>();
    config->ExpireAfterFailedUpdateTime = TDuration::MilliSeconds(30);
    auto cache = New<TSimpleExpiringCache>(config, 0.0);

    for (int i = 0; i < 10; ++i) {
        cache->Get(0);
        Sleep(TDuration::MilliSeconds(50));
    }

    int actual = cache->GetCount();

    EXPECT_EQ(10, actual);
}

TEST(TExpiringCacheTest, TestZeroCache1)
{
    auto config = New<TExpiringCacheConfig>();
    config->ExpireAfterAccessTime = TDuration::Seconds(0);
    config->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(0);
    config->ExpireAfterFailedUpdateTime = TDuration::Seconds(0);

    auto cache = New<TDelayedExpiringCache>(config, TDuration::MilliSeconds(5));
    for (int i = 0; i < 10; ++i) {
        auto future = cache->Get(0);
        EXPECT_EQ(i + 1, cache->GetCount());
        Sleep(TDuration::MilliSeconds(1));
        auto valueOrError = future.Get();
        EXPECT_TRUE(valueOrError.IsOK());
        EXPECT_EQ(i + 1, valueOrError.Value());
        Sleep(TDuration::MilliSeconds(1));
    }

    int actual = cache->GetCount();
    EXPECT_EQ(10, actual);
}

TEST(TExpiringCacheTest, TestZeroCache2)
{
    auto config = New<TExpiringCacheConfig>();
    config->ExpireAfterAccessTime = TDuration::Seconds(0);
    config->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(0);
    config->ExpireAfterFailedUpdateTime = TDuration::Seconds(0);

    auto cache = New<TDelayedExpiringCache>(config, TDuration::MilliSeconds(10));
    std::vector<TFuture<int>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(cache->Get(0));
    }

    for (auto future : futures) {
        auto result = future.Get();
        EXPECT_TRUE(result.IsOK());
        EXPECT_EQ(1, result.Value());
    }

    int actual = cache->GetCount();
    EXPECT_EQ(1, actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
