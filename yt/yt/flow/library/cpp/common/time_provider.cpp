#include "time_provider.h"

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client_spec.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TTimeProvider
    : public ITimeProvider
{
public:
    TTimeProvider(
        NTransactionClient::ITimestampProviderPtr timestampProvider,
        NObjectClient::TCellTag clockClusterTag,
        TDuration timestampCacheTtl)
        : TimestampProvider_(std::move(timestampProvider))
        , ClockClusterTag_(clockClusterTag)
        , TimestampCacheTtl_(timestampCacheTtl)
    { }

    TFuture<TGlobalUniqueSeqNo> GenerateGlobalUniqueSeqNo() const override
    {
        return TimestampProvider_->GenerateTimestamps(/*count*/ 1, ClockClusterTag_)
            .Apply(BIND([this, this_ = MakeStrong(this)] (const NTransactionClient::TTimestamp& timestamp) {
                auto result = TGlobalUniqueSeqNo{
                    .Timestamp = TSystemTimestamp(NTransactionClient::UnixTimeFromTimestamp(timestamp)),
                    .UniqueSeqNo = TUniqueSeqNo(timestamp)};
                UpdateCachedTimestamp(result.Timestamp);
                return result;
            }));
    }

    i64 GenerateSeqNo() override
    {
        while (true) {
            {
                auto guard = Guard(SeqNoLock_);
                if (MaxSeqNo_ > 0 &&
                    LastSeqNoUpdate_ + SeqNoUpdatePeriod_ >= TInstant::Now() &&
                    CurrentSeqNo_ < MaxSeqNo_)
                {
                    return CurrentSeqNo_++;
                }
            }
            RefreshSeqNoRange();
        }
    }

    TFuture<TSystemTimestamp> GetTimestamp(bool barrier) const override
    {
        if (!barrier) {
            auto guard = Guard(TimestampCacheLock_);
            if (CachedTimestampGeneratedAt_ != TInstant::Zero() &&
                CachedTimestampGeneratedAt_ + TimestampCacheTtl_ >= TInstant::Now())
            {
                return MakeFuture(CachedTimestamp_);
            }
        }
        return GenerateGlobalUniqueSeqNo()
            .Apply(BIND([] (const TGlobalUniqueSeqNo& result) {
                return result.Timestamp;
            }));
    }

private:
    static constexpr TDuration SeqNoUpdatePeriod_ = TDuration::Seconds(5);
    static constexpr i64 MaxSeqNoHeadroom = 1 << 30;

    const NTransactionClient::ITimestampProviderPtr TimestampProvider_;
    const NObjectClient::TCellTag ClockClusterTag_;
    const TDuration TimestampCacheTtl_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SeqNoLock_);
    i64 CurrentSeqNo_ = 0;
    i64 MaxSeqNo_ = 0;
    TInstant LastSeqNoUpdate_ = TInstant::Zero();

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TimestampCacheLock_);
    mutable TSystemTimestamp CachedTimestamp_ = ZeroSystemTimestamp;
    mutable TInstant CachedTimestampGeneratedAt_ = TInstant::Zero();

    void UpdateCachedTimestamp(TSystemTimestamp timestamp) const
    {
        auto now = TInstant::Now();
        auto guard = Guard(TimestampCacheLock_);
        if (timestamp >= CachedTimestamp_) {
            CachedTimestamp_ = timestamp;
            CachedTimestampGeneratedAt_ = now;
        }
    }

    void RefreshSeqNoRange()
    {
        // Mirror the original two-phase init: the first fetch establishes the floor
        // (no prior instance can have allocated past the cluster clock at this point),
        // a second fetch then provides the ceiling we need for actual headroom.
        bool isInit;
        {
            auto guard = Guard(SeqNoLock_);
            isInit = (MaxSeqNo_ == 0);
        }
        if (isInit) {
            auto floor = FetchSeqNo();
            auto guard = Guard(SeqNoLock_);
            if (MaxSeqNo_ == 0) {
                CurrentSeqNo_ = floor;
                MaxSeqNo_ = floor;
            }
        }

        while (true) {
            auto newMax = FetchSeqNo();
            {
                auto guard = Guard(SeqNoLock_);
                if (newMax > MaxSeqNo_) {
                    MaxSeqNo_ = newMax;
                }
                if (CurrentSeqNo_ < MaxSeqNo_) {
                    YT_VERIFY(MaxSeqNo_ > MaxSeqNoHeadroom);
                    CurrentSeqNo_ = std::max(CurrentSeqNo_, MaxSeqNo_ - MaxSeqNoHeadroom);
                    LastSeqNoUpdate_ = TInstant::Now();
                    return;
                }
            }
            // Cluster clock didn't advance past CurrentSeqNo_ — wait without holding the lock.
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(500));
        }
    }

    i64 FetchSeqNo()
    {
        // No spin lock held — safe to context-switch in WaitFor.
        auto v = NConcurrency::WaitFor(GenerateGlobalUniqueSeqNo()).ValueOrThrow().UniqueSeqNo.Underlying();
        YT_VERIFY(v > 0);
        return v;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITimeProviderPtr CreateTimeProvider(
    NTransactionClient::ITimestampProviderPtr timestampProvider,
    NObjectClient::TCellTag clockClusterTag,
    TDuration timestampCacheTtl)
{
    return New<TTimeProvider>(std::move(timestampProvider), clockClusterTag, timestampCacheTtl);
}

ITimeProviderPtr CreateRetryingTimeProvider(
    NApi::IClientPtr client,
    NObjectClient::TCellTag clockClusterTag,
    IInvokerPtr invoker,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger)
{
    auto spec = New<TDynamicRetryableClientSpec>();
    spec->Timeout = TDuration::Hours(1);
    spec->MinInnerTimeout = TDuration::Seconds(60);
    spec->Backoff = TExponentialBackoffOptions{
        .InvocationCount = 1000,
        .MinBackoff = TDuration::Seconds(1),
        .MaxBackoff = TDuration::Seconds(60),
        .BackoffMultiplier = 2.0,
    };

    auto retryableClient = CreateRetryableClient(client, invoker, statusProfiler, logger);
    retryableClient->Reconfigure(spec);

    return New<TTimeProvider>(retryableClient->GetTimestampProvider(), clockClusterTag, DefaultTimestampCacheTtl);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
