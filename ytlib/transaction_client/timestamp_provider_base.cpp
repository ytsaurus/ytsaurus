#include "timestamp_provider_base.h"
#include "private.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFuture<TTimestamp> TTimestampProviderBase::GenerateTimestamps(int count)
{
    LOG_DEBUG("Generating fresh timestamps (Count: %v)", count);

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
        LOG_ERROR(error);
        return MakeFuture<TTimestamp>(error);
    }

    auto firstTimestamp = timestampOrError.Value();
    auto lastTimestamp = firstTimestamp + count;

    LOG_DEBUG("Fresh timestamps generated (Timestamps: %v-%v)",
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

} // namespace NTransactionClient
} // namespace NYT
