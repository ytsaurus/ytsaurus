#pragma once

#include "timestamp_provider.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Mostly implements the tracking of the latest timestamp, thus factoring out
//! common code between the actual implementations.
class TTimestampProviderBase
    : public ITimestampProvider
{
protected:
    virtual TFuture<TTimestamp> DoGenerateTimestamps(int count) = 0;

public:
    virtual TFuture<TTimestamp> GenerateTimestamps(int count) override;
    virtual TTimestamp GetLatestTimestamp() override;

private:
    std::atomic<TTimestamp> LatestTimestamp_ = {MinTimestamp};

    TFuture<TTimestamp> OnGenerateTimestamps(
        int count,
        const TErrorOr<TTimestamp>& timestampOrError);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT

