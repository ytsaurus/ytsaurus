#include "batching_secret_vault_service.h"
#include "secret_vault_service.h"
#include "config.h"
#include "private.h"

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/rpc/dispatcher.h>

#include <queue>

namespace NYT {
namespace NAuth {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TBatchingSecretVaultService
    : public ISecretVaultService
{
public:
    TBatchingSecretVaultService(
        TBatchingSecretVaultServiceConfigPtr config,
        ISecretVaultServicePtr underlying,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , Underlying_(std::move(underlying))
        , Profiler_(std::move(profiler))
        , TickExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            BIND(&TBatchingSecretVaultService::OnTick, MakeWeak(this)),
            Config_->BatchDelay))
        , RequestThrottler_(CreateReconfigurableThroughputThrottler(
            Config_->RequestsThrottler,
            NLogging::TLogger(),
            NProfiling::TProfiler(Profiler_.GetPathPrefix() + "/request_throttler")))
    {
        TickExecutor_->Start();
    }

    virtual TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(const std::vector<TSecretSubrequest>& subrequests) override
    {
        std::vector<TFuture<TSecretSubresponse>> asyncResults;
        asyncResults.reserve(subrequests.size());
        auto guard = Guard(SpinLock_);
        for (const auto& subrequest : subrequests) {
            asyncResults.push_back(DoGetSecret(subrequest, guard));
        }
        return CombineAll(asyncResults);
    }

private:
    const TBatchingSecretVaultServiceConfigPtr Config_;
    const ISecretVaultServicePtr Underlying_;
    const NProfiling::TProfiler Profiler_;

    const TPeriodicExecutorPtr TickExecutor_;
    const IThroughputThrottlerPtr RequestThrottler_;

    TSpinLock SpinLock_;

    struct TQueueItem
    {
        TSecretSubrequest Subrequest;
        TPromise<TSecretSubresponse> Promise;
        TInstant EnqueueTime;
    };
    std::queue<TQueueItem> SubrequestQueue_;

    NProfiling::TAggregateGauge BatchingLatencyGauge_{"/batching_latency"};


    TFuture<TSecretSubresponse> DoGetSecret(const TSecretSubrequest& subrequest, TGuard<TSpinLock>& guard)
    {
        auto promise = NewPromise<TSecretSubresponse>();
        SubrequestQueue_.push(TQueueItem{
            subrequest,
            promise,
            TInstant::Now()
        });
        return promise.ToFuture();
    }

    void OnTick()
    {
        while (true) {
            {
                auto guard = Guard(SpinLock_);
                if (SubrequestQueue_.empty()) {
                    break;
                }
            }

            if (!RequestThrottler_->TryAcquire(1)) {
                break;
            }

            std::vector<TQueueItem> items;
            {
                auto guard = Guard(SpinLock_);
                while (!SubrequestQueue_.empty() && static_cast<int>(items.size()) < Config_->MaxSubrequestsPerRequest) {
                    items.push_back(SubrequestQueue_.front());
                    SubrequestQueue_.pop();
                }
            }

            if (items.empty()) {
                break;
            }

            std::vector<TSecretSubrequest> subrequests;
            subrequests.reserve(items.size());
            auto now = TInstant::Now();
            for (const auto& item : items) {
                subrequests.push_back(item.Subrequest);
                Profiler_.Update(BatchingLatencyGauge_, NProfiling::DurationToValue(now - item.EnqueueTime));
            }

            Underlying_->GetSecrets(subrequests).Subscribe(
                BIND([=, this_= MakeStrong(this), items = std::move(items)] (const TErrorOr<std::vector<TErrorOrSecretSubresponse>>& result) mutable {
                    if (result.IsOK()) {
                        const auto& subresponses = result.Value();
                        for (size_t index = 0; index < items.size(); ++index) {
                            auto& item = items[index];
                            item.Promise.Set(subresponses[index]);
                        }
                    } else {
                        for (auto& item : items) {
                            item.Promise.Set(TError(result));
                        }
                    }
                }));
        }
    }
};

ISecretVaultServicePtr CreateBatchingSecretVaultService(
    TBatchingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying,
    NProfiling::TProfiler profiler)
{
    return New<TBatchingSecretVaultService>(
        std::move(config),
        std::move(underlying),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
