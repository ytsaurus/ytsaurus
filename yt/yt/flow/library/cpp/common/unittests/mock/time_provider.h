#pragma once

#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/client/transaction_client/helpers.h>

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

    TFuture<void> InsertSeqNoBarrier() override
    {
        return OKFuture;
    }

    TFuture<TSystemTimestamp> GetTimestamp(bool /*barrier*/) const override
    {
        return MakeFuture(TSystemTimestamp(Counter_++));
    }

private:
    mutable std::atomic<i64> Counter_{1};
};

////////////////////////////////////////////////////////////////////////////////

class TFakeVersionProvider
    : public IVersionProvider
{
public:
    explicit TFakeVersionProvider(ui64 unixTime = 1'784'633'264)
        : NextVersion_(static_cast<i64>(
            NTransactionClient::TimestampFromUnixTime(unixTime).Underlying()))
    { }

    TVersion GenerateVersion() override
    {
        return TVersion(NextVersion_++);
    }

    void SetUnixTime(ui64 unixTime)
    {
        const auto nextVersion = static_cast<i64>(
            NTransactionClient::TimestampFromUnixTime(unixTime).Underlying());
        YT_VERIFY(nextVersion > NextVersion_);
        NextVersion_ = nextVersion;
    }

private:
    std::atomic<i64> NextVersion_;
};

inline IVersionProviderPtr TestVersionProvider()
{
    static const auto provider = New<TFakeVersionProvider>();
    return provider;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
