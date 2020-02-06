#include "batching_timestamp_provider.h"
#include "timestamp_provider.h"
#include "private.h"

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NTransactionClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TBatchingTimestampProvider
    : public ITimestampProvider
{
public:
    TBatchingTimestampProvider(
        ITimestampProviderPtr underlying,
        TDuration updatePeriod,
        TDuration batchPeriod)
        : Underlying_(std::move(underlying))
        , UpdatePeriod_(updatePeriod)
        , BatchPeriod_(batchPeriod)
    { }

    virtual TFuture<TTimestamp> GenerateTimestamps(int count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TFuture<TTimestamp> result;

        {
            TGuard<TSpinLock> guard(SpinLock_);
            PendingRequests_.emplace_back();
            PendingRequests_.back().Count = count;
            PendingRequests_.back().Promise = NewPromise<TTimestamp>();
            result = PendingRequests_.back().Promise.ToFuture().ToUncancelable();

            MaybeScheduleSendGenerateRequest(guard);
        }
        return result;
    }

    virtual TTimestamp GetLatestTimestamp() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock_);

        auto result = Underlying_->GetLatestTimestamp();

        if (!LatestTimestampExecutor_) {
            LatestTimestampExecutor_ = New<TPeriodicExecutor>(
                GetSyncInvoker(),
                BIND(&TBatchingTimestampProvider::UpdateLatestTimestamp, MakeWeak(this)),
                UpdatePeriod_,
                EPeriodicExecutorMode::Automatic);
            guard.Release();
            LatestTimestampExecutor_->Start();
        }

        return result;
    }

private:
    const ITimestampProviderPtr Underlying_;
    const TDuration UpdatePeriod_;
    const TDuration BatchPeriod_;

    struct TRequest
    {
        int Count;
        TPromise<TTimestamp> Promise;
    };

    TSpinLock SpinLock_;
    bool GenerateInProgress_ = false;
    bool FlushScheduled_ = false;
    std::vector<TRequest> PendingRequests_;

    TPeriodicExecutorPtr LatestTimestampExecutor_;

    TInstant LastRequestTime_;

    void MaybeScheduleSendGenerateRequest(TGuard<TSpinLock>& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (PendingRequests_.empty() || GenerateInProgress_) {
            return;
        }

        auto now = NProfiling::GetInstant();

        if (LastRequestTime_ + BatchPeriod_ < now) {
            SendGenerateRequest(guard);
        } else if (!FlushScheduled_) {
            FlushScheduled_ = true;
            TDelayedExecutor::Submit(BIND([=, this_ = MakeStrong(this)] {
                TGuard<TSpinLock> guard(SpinLock_);
                FlushScheduled_ = false;
                if (GenerateInProgress_) {
                    return;
                }
                SendGenerateRequest(guard);
            }), BatchPeriod_ - (now - LastRequestTime_));
        }
    }

    void SendGenerateRequest(TGuard<TSpinLock>& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        YT_VERIFY(!GenerateInProgress_);
        GenerateInProgress_ = true;
        LastRequestTime_ = NProfiling::GetInstant();

        std::vector<TRequest> requests;
        requests.swap(PendingRequests_);

        guard.Release();

        int count = 0;
        for (const auto& request : requests) {
            count += request.Count;
        }

        Underlying_->GenerateTimestamps(count).Subscribe(BIND(
            &TBatchingTimestampProvider::OnGenerateResponse,
            MakeStrong(this),
            Passed(std::move(requests)),
            count));
    }

    void OnGenerateResponse(
        std::vector<TRequest> requests,
        int count,
        const TErrorOr<TTimestamp>& firstTimestampOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(SpinLock_);

            YT_VERIFY(GenerateInProgress_);
            GenerateInProgress_ = false;

            MaybeScheduleSendGenerateRequest(guard);
        }

        if (firstTimestampOrError.IsOK()) {
            auto timestamp = firstTimestampOrError.Value();
            for (auto& request : requests) {
                request.Promise.Set(timestamp);
                timestamp += request.Count;
            }
        } else {
            for (auto& request : requests) {
                request.Promise.Set(TError(firstTimestampOrError));
            }
        }
    }

    void UpdateLatestTimestamp()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Updating latest timestamp");

        auto timestampOrError = WaitFor(GenerateTimestamps(1));
        if (timestampOrError.IsOK()) {
            YT_LOG_DEBUG("Latest timestamp updated (Timestamp: %llx)",
                timestampOrError.Value());
        } else {
            YT_LOG_WARNING(timestampOrError, "Error updating latest timestamp");
            return;
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateBatchingTimestampProvider(
    ITimestampProviderPtr underlying,
    TDuration updatePeriod,
    TDuration batchPeriod)
{
    return New<TBatchingTimestampProvider>(std::move(underlying), updatePeriod, batchPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
