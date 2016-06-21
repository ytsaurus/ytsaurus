#include "framework.h"

#include <yt/core/concurrency/action_queue.h>
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

TEST(TExpiringCacheTest, TestBackgroundUpdate)
{
    int interval = 100;
    auto config = New<TExpiringCacheConfig>();
    config->SuccessProbationTime = TDuration::MilliSeconds(interval);
    auto cache = New<TSimpleExpiringCache>(config);

    auto start = Now();
    cache->Get(0);
    sleep(1);
    int actual = cache->GetCount();
    auto end = Now();

    int duration = (end - start).MilliSeconds();
    int expected = duration / interval;

    EXPECT_EQ(expected, actual);
}

TEST(TExpiringCacheTest, TestEntryRemoval)
{
    int interval = 100;
    auto config = New<TExpiringCacheConfig>();
    config->SuccessProbationTime = TDuration::MilliSeconds(interval);
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

    auto start = Now();
    int begin = cache->GetCount();
    sleep(1);
    int actual = cache->GetCount() - begin;
    auto end = Now();

    int duration = (end - start).MilliSeconds();
    int expected = duration / interval;

    EXPECT_GE(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
