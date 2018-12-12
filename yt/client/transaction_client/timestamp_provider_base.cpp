#include "timestamp_provider_base.h"
#include "private.h"

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFuture<TTimestamp> TTimestampProviderBase::GenerateTimestamps(int count)
{
    YT_LOG_DEBUG("Generating fresh timestamps (Count: %v)", count);

    return DoGenerateTimestamps(count).Apply(BIND(
        &TTimestampProviderBase::OnGenerateTimestamps,
        MakeStrong(this),
        count));
}

TTimestamp TTimestampProviderBase::GetLatestTimestamp()
{
    return LatestTimestamp_.load(std::memory_order_relaxed);
}

TFuture<TTimestamp> TTimestampProviderBase::OnGenerateTimestamps(
    int count,
    const TErrorOr<TTimestamp>& timestampOrError)
{
    if (!timestampOrError.IsOK()) {
        auto error = TError("Error generating fresh timestamps") << timestampOrError;
        YT_LOG_ERROR(error);
        return MakeFuture<TTimestamp>(error);
    }

    auto firstTimestamp = timestampOrError.Value();
    auto lastTimestamp = firstTimestamp + count - 1;

    YT_LOG_DEBUG("Fresh timestamps generated (Timestamps: %llx-%llx)",
        firstTimestamp,
        lastTimestamp);

    auto latestTimestamp = LatestTimestamp_.load(std::memory_order_relaxed);
    while (true) {
        if (latestTimestamp >= lastTimestamp) {
            break;
        }
        if (LatestTimestamp_.compare_exchange_weak(latestTimestamp, lastTimestamp, std::memory_order_relaxed)) {
            break;
        }
    }

    return MakeFuture<TTimestamp>(firstTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
