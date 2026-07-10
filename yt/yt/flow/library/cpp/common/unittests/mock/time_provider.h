#pragma once

#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Drives all methods from a single monotonic counter.
class TFakeTimeProvider
    : public ITimeProvider
{
public:
    TFuture<TGlobalUniqueSeqNo> GenerateGlobalUniqueSeqNo() const override
    {
        const auto next = Counter_++;
        return MakeFuture<TGlobalUniqueSeqNo>({
            .Timestamp = TSystemTimestamp(next),
            .UniqueSeqNo = TUniqueSeqNo(next),
        });
    }

    i64 GenerateSeqNo() override
    {
        return Counter_++;
    }

    TFuture<TSystemTimestamp> GetTimestamp(bool /*barrier*/) const override
    {
        return MakeFuture(TSystemTimestamp(Counter_++));
    }

private:
    mutable std::atomic<i64> Counter_{1};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
