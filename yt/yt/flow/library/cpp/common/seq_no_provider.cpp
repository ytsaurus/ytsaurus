#include "seq_no_provider.h"

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client_spec.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TUniqueSeqNoProvider
    : public IUniqueSeqNoProvider
{
public:
    explicit TUniqueSeqNoProvider(NTransactionClient::ITimestampProviderPtr timestampProvider, NObjectClient::TCellTag clockClusterTag)
        : TimestampProvider_(std::move(timestampProvider))
        , ClockClusterTag_(clockClusterTag)
    { }

    TFuture<TResult> Generate() const override
    {
        return TimestampProvider_->GenerateTimestamps(/*count*/ 1, ClockClusterTag_).Apply(BIND([] (const NTransactionClient::TTimestamp& timestamp) {
            return TResult{
                .Timestamp = TSystemTimestamp(NTransactionClient::UnixTimeFromTimestamp(timestamp)),
                .UniqueSeqNo = TUniqueSeqNo(timestamp)};
        }));
    }

private:
    const NTransactionClient::ITimestampProviderPtr TimestampProvider_;
    const NObjectClient::TCellTag ClockClusterTag_;
};

////////////////////////////////////////////////////////////////////////////////

class TSeqNoProvider
    : public ISeqNoProvider
{
public:
    explicit TSeqNoProvider(IUniqueSeqNoProviderPtr uniqueProvider)
        : UniqueProvider_(std::move(uniqueProvider))
    { }

    i64 Generate() override
    {
        return GenerateAlignedBatch(/*count*/ 1, /*alignment*/ 1);
    }

    i64 GenerateAlignedBatch(i64 count, i64 alignment) override
    {
        YT_VERIFY(count > 0);
        YT_VERIFY(alignment > 0);
        // Refresh leaves at most MaxSeqNoHeadroom free numbers, so any larger
        // request would loop until the cluster clock catches up — refuse upfront.
        YT_VERIFY(count + alignment < MaxSeqNoHeadroom);

        while (true) {
            {
                auto guard = Guard(SpinLock_);
                if (MaxSeqNo_ > 0 && LastUpdate_ + UpdatePeriod_ >= TInstant::Now()) {
                    auto rem = CurrentSeqNo_ % alignment;
                    auto aligned = CurrentSeqNo_ + (rem == 0 ? 0 : alignment - rem);
                    if (aligned + count <= MaxSeqNo_) {
                        CurrentSeqNo_ = aligned + count;
                        return aligned;
                    }
                }
            }
            Refresh();
        }
    }

private:
    static constexpr TDuration UpdatePeriod_ = TDuration::Seconds(5);
    static constexpr i64 MaxSeqNoHeadroom = 1 << 30;

    const IUniqueSeqNoProviderPtr UniqueProvider_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    i64 CurrentSeqNo_ = 0;
    i64 MaxSeqNo_ = 0;
    TInstant LastUpdate_ = TInstant::Zero();

    void Refresh()
    {
        // Mirror the original two-phase init: the first fetch establishes the floor
        // (no prior instance can have allocated past the cluster clock at this point),
        // a second fetch then provides the ceiling we need for actual headroom.
        bool isInit;
        {
            auto guard = Guard(SpinLock_);
            isInit = (MaxSeqNo_ == 0);
        }
        if (isInit) {
            auto floor = FetchTimestamp();
            auto guard = Guard(SpinLock_);
            if (MaxSeqNo_ == 0) {
                CurrentSeqNo_ = floor;
                MaxSeqNo_ = floor;
            }
        }

        while (true) {
            auto newMax = FetchTimestamp();
            {
                auto guard = Guard(SpinLock_);
                if (newMax > MaxSeqNo_) {
                    MaxSeqNo_ = newMax;
                }
                if (CurrentSeqNo_ < MaxSeqNo_) {
                    YT_VERIFY(MaxSeqNo_ > MaxSeqNoHeadroom);
                    CurrentSeqNo_ = std::max(CurrentSeqNo_, MaxSeqNo_ - MaxSeqNoHeadroom);
                    LastUpdate_ = TInstant::Now();
                    return;
                }
            }
            // Cluster clock didn't advance past CurrentSeqNo_ — wait without holding the lock.
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(500));
        }
    }

    i64 FetchTimestamp()
    {
        // No spin lock held — safe to context-switch in WaitFor.
        auto v = NConcurrency::WaitFor(UniqueProvider_->Generate()).ValueOrThrow().UniqueSeqNo.Underlying();
        YT_VERIFY(v > 0);
        return v;
    }
};

////////////////////////////////////////////////////////////////////////////////

IUniqueSeqNoProviderPtr CreateUniqueSeqNoProvider(
    NTransactionClient::ITimestampProviderPtr timestampProvider,
    NObjectClient::TCellTag clockClusterTag)
{
    return New<TUniqueSeqNoProvider>(std::move(timestampProvider), clockClusterTag);
}

IUniqueSeqNoProviderPtr CreateRetryingUniqueSeqNoProvider(
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

    return New<TUniqueSeqNoProvider>(retryableClient->GetTimestampProvider(), clockClusterTag);
}

ISeqNoProviderPtr CreateSeqNoProvider(IUniqueSeqNoProviderPtr uniqueProvider)
{
    return New<TSeqNoProvider>(std::move(uniqueProvider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
