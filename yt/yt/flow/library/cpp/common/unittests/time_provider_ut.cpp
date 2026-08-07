#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTransactionClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

//! Hands out strictly increasing timestamps starting from |startTimestamp| and
//! counts the calls.
class TCountingTimestampProvider
    : public ITimestampProvider
{
public:
    explicit TCountingTimestampProvider(TTimestamp startTimestamp)
        : Current_(startTimestamp.Underlying())
    { }

    TFuture<TTimestamp> GenerateTimestamps(int count, NObjectClient::TCellTag /*clockClusterTag*/) override
    {
        ++CallCount_;
        // Advance by a full second of timestamp space per call so that seqno
        // headroom grows the way it does with a real cluster clock.
        return MakeFuture<TTimestamp>(NYT::NTransactionClient::TTimestamp(Current_.fetch_add(static_cast<ui64>(count) << TimestampCounterWidth)));
    }

    TTimestamp GetLatestTimestamp(NObjectClient::TCellTag /*clockClusterTag*/) override
    {
        return FromProto<NYT::NTransactionClient::TTimestamp>(Current_.load());
    }

    void Reconfigure(const TRemoteTimestampProviderConfigPtr& /*config*/) override
    { }

    int GetCallCount() const
    {
        return CallCount_.load();
    }

private:
    std::atomic<ui64> Current_;
    std::atomic<int> CallCount_{0};
};

DEFINE_REFCOUNTED_TYPE(TCountingTimestampProvider);

constexpr ui64 StartUnixTime = 1'000'000;
constexpr TTimestamp StartTimestamp = NYT::NTransactionClient::TTimestamp(StartUnixTime << TimestampCounterWidth);

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
    EXPECT_EQ(first.UniqueSeqNo, TUniqueSeqNo(StartTimestamp.Underlying()));

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

TEST_F(TTimeProviderTest, SeqNoCarriesTheClusterWallClock)
{
    auto provider = CreateProvider();

    auto seqNo = provider->GenerateSeqNo();
    EXPECT_EQ(UnixTimeFromTimestamp(TTimestamp(seqNo)), StartUnixTime);
}

TEST_F(TTimeProviderTest, SeqNoNeverRunsAheadOfTheClock)
{
    auto provider = CreateProvider();

    for (int i = 0; i < 100; ++i) {
        auto seqNo = provider->GenerateSeqNo();
        EXPECT_LE(static_cast<TTimestamp>(seqNo), TimestampProvider_->GetLatestTimestamp(NObjectClient::InvalidCellTag));
    }
}

TEST_F(TTimeProviderTest, SeqNoKeepsIncreasingAcrossLeaderChange)
{
    auto leader = CreateProvider();
    i64 last = 0;
    for (int i = 0; i < 100; ++i) {
        last = leader->GenerateSeqNo();
    }

    auto successor = CreateProvider();
    EXPECT_GT(successor->GenerateSeqNo(), last);
}

TEST_F(TTimeProviderTest, SeqNoBarrierOutrunsForeignSeqNo)
{
    // A fenced ex-leader whose range was reserved before the successor appeared.
    auto fenced = CreateProvider();
    Y_UNUSED(fenced->GenerateSeqNo());

    auto successor = CreateProvider();
    auto foreign = successor->GenerateSeqNo();

    // Without a barrier the fenced instance keeps serving from its stale range,
    // below the seqno it can now observe in the successor's persisted state.
    EXPECT_LT(fenced->GenerateSeqNo(), foreign);

    WaitFor(fenced->InsertSeqNoBarrier())
        .ThrowOnError();
    EXPECT_GT(fenced->GenerateSeqNo(), foreign);
}

TEST_F(TTimeProviderTest, SeqNoBarrierOnFreshProvider)
{
    auto provider = CreateProvider();
    WaitFor(provider->InsertSeqNoBarrier())
        .ThrowOnError();

    // The barrier fetch is the very first issued timestamp; seqnos start at or above it.
    EXPECT_GE(provider->GenerateSeqNo(), static_cast<i64>(StartTimestamp.Underlying()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
