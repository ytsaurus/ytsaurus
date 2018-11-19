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

//static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TBatchingSecretVaultService
    : public ISecretVaultService
{
public:
    TBatchingSecretVaultService(
        TBatchingSecretVaultServiceConfigPtr config,
        ISecretVaultServicePtr underlying)
        : Config_(std::move(config))
        , Underlying_(std::move(underlying))
        , TickExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            BIND(&TBatchingSecretVaultService::OnTick, MakeWeak(this)),
            Config_->BatchDelay))
        , RequestThrottler_(CreateReconfigurableThroughputThrottler(
            // TODO(babenko): profiling
            Config_->RequestsThrottler))
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

    const TPeriodicExecutorPtr TickExecutor_;
    const IThroughputThrottlerPtr RequestThrottler_;

    TSpinLock SpinLock_;

    using TQueueItem = std::pair<TSecretSubrequest, TPromise<TSecretSubresponse>>;
    std::queue<TQueueItem> SubrequestQueue_;


    TFuture<TSecretSubresponse> DoGetSecret(const TSecretSubrequest& subrequest, TGuard<TSpinLock>& guard)
    {
        auto promise = NewPromise<TSecretSubresponse>();
        SubrequestQueue_.emplace(subrequest, std::move(promise));
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
                while (static_cast<int>(items.size()) < Config_->MaxSubrequestsPerRequest) {
                    items.push_back(SubrequestQueue_.front());
                    SubrequestQueue_.pop();
                }
            }

            if (items.empty()) {
                break;
            }

            std::vector<TSecretSubrequest> subrequests;
            subrequests.reserve(items.size());
            for (const auto& item : items) {
                subrequests.push_back(item.first);
            }

            Underlying_->GetSecrets(subrequests).Subscribe(
                BIND([=, this_= MakeStrong(this), items = std::move(items)] (const TErrorOr<std::vector<TErrorOrSecretSubresponse>>& result) mutable {
                    if (result.IsOK()) {
                        const auto& subresponses = result.Value();
                        for (size_t index = 0; index < items.size(); ++index) {
                            auto& item = items[index];
                            item.second.Set(subresponses[index]);
                        }
                    } else {
                        for (auto& item : items) {
                            item.second.Set(TError(result));
                        }
                    }
                }));
        }
    }
};

ISecretVaultServicePtr CreateBatchingSecretVaultService(
    TBatchingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying)
{
    return New<TBatchingSecretVaultService>(
        std::move(config),
        std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
