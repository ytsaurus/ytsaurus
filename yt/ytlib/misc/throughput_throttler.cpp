#include "stdafx.h"
#include "throughput_throttler.h"
#include "periodic_invoker.h"
#include "singleton.h"
#include "thread_affinity.h"

#include <ytlib/actions/invoker_util.h>

#include <queue>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static TFuture<void> PresetResult = MakePromise();

class TThroughputThrottler
    : public IThroughputThrottler
{
public:
    explicit TThroughputThrottler(TThroughputThrottlerConfigPtr config)
        : ThroughputPerPeriod(static_cast<i64>(config->Period.SecondsFloat() * (*config->Limit)))
        , Available(ThroughputPerPeriod)
    {
        Invoker = New<TPeriodicInvoker>(
            GetSyncInvoker(),
            BIND(&TThroughputThrottler::OnTick, MakeWeak(this)),
            config->Period);
        Invoker->Start();
    }

    virtual TFuture<void> Throttle(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        if (count == 0) {
            return PresetResult;
        }

        TGuard<TSpinLock> guard(SpinLock);

        if (Available > 0) {
            // Execute immediately.
            Available -= count;
            return PresetResult;
        }

        // Enqueue request to be executed later.
        TRequest request;
        request.Count = count;
        request.Promise = MakePromise();
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
    TPeriodicInvokerPtr Invoker;

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

        FOREACH (auto promise, releaseList) {
            promise.Set();
        }
    }
};

IThroughputThrottlerPtr CreateThrottler(TThroughputThrottlerConfigPtr config)
{
    return config->Limit ? New<TThroughputThrottler>(config) : GetUnlimitedThrottler();
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

        return PresetResult;
    }
};

IThroughputThrottlerPtr GetUnlimitedThrottler()
{
    return RefCountedSingleton<TUnlimitedThroughtputThrottler>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
