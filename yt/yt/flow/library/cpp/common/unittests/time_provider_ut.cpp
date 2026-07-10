#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

//! Hands out strictly increasing timestamps starting from |startTimestamp| and
//! counts the calls.
class TCountingTimestampProvider
    : public ITimestampProvider
{
public:
    explicit TCountingTimestampProvider(TTimestamp startTimestamp)
        : Current_(startTimestamp)
    { }

    TFuture<TTimestamp> GenerateTimestamps(int count, NObjectClient::TCellTag /*clockClusterTag*/) override
    {
        ++CallCount_;
        // Advance by a full second of timestamp space per call so that seqno
        // headroom grows the way it does with a real cluster clock.
        return MakeFuture<TTimestamp>(Current_.fetch_add(static_cast<TTimestamp>(count) << TimestampCounterWidth));
    }

    TTimestamp GetLatestTimestamp(NObjectClient::TCellTag /*clockClusterTag*/) override
    {
        return Current_.load();
    }

    void Reconfigure(const TRemoteTimestampProviderConfigPtr& /*config*/) override
    { }

    int GetCallCount() const
    {
        return CallCount_.load();
    }

private:
    std::atomic<TTimestamp> Current_;
    std::atomic<int> CallCount_{0};
};

DEFINE_REFCOUNTED_TYPE(TCountingTimestampProvider);

constexpr ui64 StartUnixTime = 1'000'000;
constexpr TTimestamp StartTimestamp = StartUnixTime << TimestampCounterWidth;

struct TTimeProviderTest
    : public ::testing::Test
{
    TIntrusivePtr<TCountingTimestampProvider> TimestampProvider_ =
        New<TCountingTimestampProvider>(StartTimestamp);

    ITimeProviderPtr CreateProvider(TDuration timestampCacheTtl = DefaultTimestampCacheTtl)
    {
        return CreateTimeProvider(TimestampProvider_, NObjectClient::InvalidCellTag, timestampCacheTtl);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTimeProviderTest, GlobalUniqueSeqNoDecomposition)
{
    auto provider = CreateProvider();

    auto first = WaitFor(provider->GenerateGlobalUniqueSeqNo()).ValueOrThrow();
    EXPECT_EQ(first.Timestamp, TSystemTimestamp(StartUnixTime));
    EXPECT_EQ(first.UniqueSeqNo, TUniqueSeqNo(StartTimestamp));

    auto second = WaitFor(provider->GenerateGlobalUniqueSeqNo()).ValueOrThrow();
    EXPECT_GT(second.UniqueSeqNo, first.UniqueSeqNo);
}

TEST_F(TTimeProviderTest, TimestampServedFromCache)
{
    auto provider = CreateProvider(/*timestampCacheTtl*/ TDuration::Hours(1));

    auto first = WaitFor(provider->GetTimestamp(/*barrier*/ false)).ValueOrThrow();
    EXPECT_EQ(TimestampProvider_->GetCallCount(), 1);

    auto second = WaitFor(provider->GetTimestamp(/*barrier*/ false)).ValueOrThrow();
    EXPECT_EQ(TimestampProvider_->GetCallCount(), 1);
    EXPECT_EQ(second, first);
}

TEST_F(TTimeProviderTest, TimestampCacheExpires)
{
    auto provider = CreateProvider(/*timestampCacheTtl*/ TDuration::Zero());

    Y_UNUSED(WaitFor(provider->GetTimestamp(/*barrier*/ false)).ValueOrThrow());
    Y_UNUSED(WaitFor(provider->GetTimestamp(/*barrier*/ false)).ValueOrThrow());
    EXPECT_EQ(TimestampProvider_->GetCallCount(), 2);
}

TEST_F(TTimeProviderTest, BarrierAlwaysGenerates)
{
    auto provider = CreateProvider(/*timestampCacheTtl*/ TDuration::Hours(1));

    Y_UNUSED(WaitFor(provider->GetTimestamp(/*barrier*/ true)).ValueOrThrow());
    auto barrier = WaitFor(provider->GetTimestamp(/*barrier*/ true)).ValueOrThrow();
    EXPECT_EQ(TimestampProvider_->GetCallCount(), 2);

    // The barrier calls refreshed the cache.
    auto cached = WaitFor(provider->GetTimestamp(/*barrier*/ false)).ValueOrThrow();
    EXPECT_EQ(TimestampProvider_->GetCallCount(), 2);
    EXPECT_EQ(cached, barrier);
}

TEST_F(TTimeProviderTest, GenerateRefreshesTimestampCache)
{
    auto provider = CreateProvider(/*timestampCacheTtl*/ TDuration::Hours(1));

    auto generated = WaitFor(provider->GenerateGlobalUniqueSeqNo()).ValueOrThrow();
    auto cached = WaitFor(provider->GetTimestamp(/*barrier*/ false)).ValueOrThrow();
    EXPECT_EQ(TimestampProvider_->GetCallCount(), 1);
    EXPECT_EQ(cached, generated.Timestamp);
}

TEST_F(TTimeProviderTest, SeqNoIncreases)
{
    auto provider = CreateProvider();

    auto previous = provider->GenerateSeqNo();
    for (int i = 0; i < 100; ++i) {
        auto next = provider->GenerateSeqNo();
        EXPECT_GT(next, previous);
        previous = next;
    }
}

TEST_F(TTimeProviderTest, SeqNoRangeIsCachedBetweenCalls)
{
    auto provider = CreateProvider();

    Y_UNUSED(provider->GenerateSeqNo());
    const auto callCount = TimestampProvider_->GetCallCount();

    // Subsequent calls are served from the reserved range without clock roundtrips.
    for (int i = 0; i < 100; ++i) {
        Y_UNUSED(provider->GenerateSeqNo());
    }
    EXPECT_EQ(TimestampProvider_->GetCallCount(), callCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
