#include "stdafx.h"
#include "throughput_throttler.h"
#include "periodic_executor.h"

#include <core/misc/singleton.h>

#include <core/concurrency/thread_affinity.h>

#include <core/actions/invoker_util.h>

#include <core/profiling/profiler.h>

#include <queue>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TLimitedThroughputThrottler
    : public IThroughputThrottler
{
public:
    explicit TLimitedThroughputThrottler(TThroughputThrottlerConfigPtr config)
        : ThroughputPerPeriod(static_cast<i64>(config->Period.SecondsFloat() * (*config->Limit)))
        , Available(ThroughputPerPeriod)
    {
        PeriodicExecutor = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TLimitedThroughputThrottler::OnTick, MakeWeak(this)),
            config->Period);
        PeriodicExecutor->Start();
    }

    virtual TFuture<void> Throttle(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        if (count == 0) {
            return VoidFuture;
        }

        TGuard<TSpinLock> guard(SpinLock);

        if (Available > 0) {
            // Execute immediately.
            Available -= count;
            return VoidFuture;
        }

        // Enqueue request to be executed later.
        TRequest request;
        request.Count = count;
        request.Promise = NewPromise();
        Requests.push(request);
        return request.Promise;
    }

private:
    struct TRequest
    {
        i64 Count;
        TPromise<void> Promise;
    };

    i64 ThroughputPerPeriod;
    TPeriodicExecutorPtr PeriodicExecutor;

    //! Protects the section immediately following it.
    TSpinLock SpinLock;
    i64 Available;
    std::queue<TRequest> Requests;

    void OnTick()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TPromise<void>> releaseList;

        {
            TGuard<TSpinLock> guard(SpinLock);
            Available += ThroughputPerPeriod;
            while (!Requests.empty() && Available > 0) {
                auto& request = Requests.front();
                Available -= request.Count;
                releaseList.push_back(std::move(request.Promise));
                Requests.pop();
            }
        }

        for (auto promise : releaseList) {
            promise.Set();
        }
    }
};

IThroughputThrottlerPtr CreateLimitedThrottler(TThroughputThrottlerConfigPtr config)
{
    return config->Limit
           ? New<TLimitedThroughputThrottler>(config)
           : GetUnlimitedThrottler();
}

////////////////////////////////////////////////////////////////////////////////

class TProfilingThrottlerWrapper
    : public IThroughputThrottler
{
public:
    TProfilingThrottlerWrapper(
        IThroughputThrottlerPtr underlyingThrottler,
        const NYPath::TYPath& pathPrefix)
        : UnderlyingThrottler(underlyingThrottler)
        , Profiler(pathPrefix)
        , TotalCounter("/total")
        , RateCounter("/rate")
    { }

    virtual TFuture<void> Throttle(i64 count) override
    {
        Profiler.Increment(TotalCounter, count);
        Profiler.Increment(RateCounter, count);
        return UnderlyingThrottler->Throttle(count);
    }

private:
    IThroughputThrottlerPtr UnderlyingThrottler;
    NProfiling::TProfiler Profiler;
    NProfiling::TAggregateCounter TotalCounter;
    NProfiling::TRateCounter RateCounter;

};

IThroughputThrottlerPtr CreateProfilingThrottlerWrapper(
    IThroughputThrottlerPtr underlyingThrottler,
    const NYPath::TYPath& pathPrefix)
{
    return New<TProfilingThrottlerWrapper>(
        underlyingThrottler,
        pathPrefix);
}

////////////////////////////////////////////////////////////////////////////////

class TUnlimitedThroughtputThrottler
    : public IThroughputThrottler
{
public:
    virtual TFuture<void> Throttle(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        return VoidFuture;
    }

    static TUnlimitedThroughtputThrottler* Get()
    {
        return TSingleton::Get();
    }

    DECLARE_SINGLETON_MIXIN(TUnlimitedThroughtputThrottler, TRefCountedInstanceMixin);
};

IThroughputThrottlerPtr GetUnlimitedThrottler()
{
    return TUnlimitedThroughtputThrottler::Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
