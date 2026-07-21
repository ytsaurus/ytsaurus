#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/flow/library/cpp/computation/key_visitor.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/key_states.h>
#include <yt/yt/flow/library/cpp/tables/unittests/mock/key_visitor_states.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/library/profiling/sensor.h>

#include <cmath>
#include <limits>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TKeyVisitorTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId = TComputationId("c");
    const TStreamId StreamId = TStreamId("s");

    NTables::TInMemoryKeyStatesPtr KeyStates_ = New<NTables::TInMemoryKeyStates>();
    NTables::TInMemoryKeyVisitorStatesPtr KeyVisitorStates_ = New<NTables::TInMemoryKeyVisitorStates>();
    ITimeProviderPtr TimeProvider_ = New<TFakeTimeProvider>();
    TActionQueuePtr Queue_ = New<TActionQueue>("KeyVisitorTest");

    void SeedKeys(const std::vector<TKey>& keys, const std::string& name)
    {
        for (const auto& key : keys) {
            KeyStates_->Set({ComputationId, key, name});
        }
    }

    TKeyVisitorContextPtr MakeContext(
        TKeyRange partitionRange,
        std::optional<THashSet<std::string>> names,
        int bucketCount)
    {
        auto spec = New<TKeyVisitorStreamSpec>();
        spec->Names = std::move(names);
        spec->BucketCount = bucketCount;

        auto ctx = New<TKeyVisitorContext>();
        ctx->ComputationId = ComputationId;
        ctx->StreamId = StreamId;
        ctx->Spec = std::move(spec);
        ctx->PartitionRange = std::move(partitionRange);
        ctx->KeyStates = KeyStates_;
        ctx->KeyVisitorStates = KeyVisitorStates_;
        ctx->TimeProvider = TimeProvider_;
        ctx->SerializedInvoker = Queue_->GetInvoker();
        ctx->Logger = NLogging::TLogger("KeyVisitorTest");
        ctx->StatusProfiler = CreateSyncStatusProfiler();
        return ctx;
    }

    TDynamicKeyVisitorContextPtr MakeDynamicContext(
        TDuration period,
        i64 bufferRowLimit,
        i64 maxScanRowsPerIteration = 10'000,
        TDuration backgroundFillPeriod = TDuration::MilliSeconds(50))
    {
        auto dynSpec = New<TDynamicKeyVisitorStreamSpec>();
        dynSpec->Period = period;
        dynSpec->BufferRowLimit = NYTree::TSize(bufferRowLimit);
        dynSpec->MaxScanRowsPerIteration = NYTree::TSize(maxScanRowsPerIteration);
        dynSpec->BackgroundFillPeriod = backgroundFillPeriod;

        auto ctx = New<TDynamicKeyVisitorContext>();
        ctx->DynamicSpec = std::move(dynSpec);
        ctx->Draining = false;
        return ctx;
    }

    //! Pulls visits until |stopCount| keys are collected (or the timeout fires),
    //! appending them — without deduplication — to |out|. Returns the number of
    //! keys appended by this call.
    i64 DrainKeys(
        const TKeyVisitorPtr& visitor,
        std::vector<TKey>* out,
        i64 stopCount,
        i64 batchSize = 100,
        TDuration timeout = TDuration::Seconds(5))
    {
        i64 added = 0;
        const auto deadline = TInstant::Now() + timeout;
        while (added < stopCount && TInstant::Now() < deadline) {
            // GetNextBatch must run on the visitor's serialized invoker — that
            // is the same thread the background fill writes Buffer_ from.
            auto visits = WaitFor(BIND([visitor, batchSize] {
                return visitor->GetNextBatch(batchSize);
            })
                    .AsyncVia(Queue_->GetInvoker())
                    .Run())
                .ValueOrThrow();
            for (auto& visit : visits) {
                out->push_back(visit.Key);
                ++added;
            }
            if (visits.empty()) {
                TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(20));
            }
        }
        return added;
    }

    void SyncOnQueue(const TKeyVisitorPtr& visitor)
    {
        WaitFor(BIND([visitor] {
            // The in-memory backend ignores the transaction.
            visitor->Sync(/*transaction*/ nullptr);
        })
                .AsyncVia(Queue_->GetInvoker())
                .Run())
            .ThrowOnError();
    }

    void StopOnQueue(const TKeyVisitorPtr& visitor)
    {
        WaitFor(BIND([visitor] {
            visitor->Stop();
        })
                .AsyncVia(Queue_->GetInvoker())
                .Run())
            .ThrowOnError();
    }

    void SetUpstreamCompletedOnQueue(const TKeyVisitorPtr& visitor)
    {
        WaitFor(BIND([visitor] {
            visitor->SetUpstreamCompleted();
        })
                .AsyncVia(Queue_->GetInvoker())
                .Run())
            .ThrowOnError();
    }

    void ReconfigureOnQueue(
        const TKeyVisitorPtr& visitor,
        const TDynamicKeyVisitorContextPtr& dynamicContext)
    {
        // Reconfigure asserts the visitor's serialized-invoker affinity.
        WaitFor(BIND([visitor, dynamicContext] {
            visitor->Reconfigure(dynamicContext);
        })
                .AsyncVia(Queue_->GetInvoker())
                .Run())
            .ThrowOnError();
    }

    bool IsEmptyOnQueue(const TKeyVisitorPtr& visitor)
    {
        return WaitFor(BIND([visitor, this] {
            auto inflight = visitor->BuildInflight();
            const auto it = inflight.find(StreamId);
            return it != inflight.end() && it->second->Empty;
        })
                .AsyncVia(Queue_->GetInvoker())
                .Run())
            .ValueOrThrow();
    }

    static THashSet<TKey> ToSet(const std::vector<TKey>& keys)
    {
        return THashSet<TKey>(keys.begin(), keys.end());
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyVisitorTest, EachPassEmitsEverySeededKeyWithoutDuplicates)
{
    std::vector<TKey> seeded;
    for (ui64 hash = 1; hash <= 16; ++hash) {
        seeded.push_back(MakeUintKey(hash * 5));
    }
    SeedKeys(seeded, "/state");
    const auto expected = ToSet(seeded);

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 4);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    // Two consecutive passes: each must emit every key exactly once.
    std::vector<TKey> pass1;
    DrainKeys(visitor, &pass1, std::ssize(seeded));
    EXPECT_EQ(std::ssize(pass1), std::ssize(seeded)) << "pass 1 must emit each key once, no duplicates";
    EXPECT_EQ(ToSet(pass1), expected) << "pass 1 must emit every seeded key";

    std::vector<TKey> pass2;
    DrainKeys(visitor, &pass2, std::ssize(seeded));
    EXPECT_EQ(std::ssize(pass2), std::ssize(seeded)) << "pass 2 must emit each key once, no duplicates";
    EXPECT_EQ(ToSet(pass2), expected) << "pass 2 must restart and emit every seeded key";

    StopOnQueue(visitor);
}

TEST_F(TKeyVisitorTest, ResumesAfterRecreationWithoutDuplicates)
{
    std::vector<TKey> seeded;
    for (ui64 hash = 1; hash <= 16; ++hash) {
        seeded.push_back(MakeUintKey(hash * 5));
    }
    SeedKeys(seeded, "/state");
    const auto expected = ToSet(seeded);

    std::vector<TKey> collected;

    // First visitor: drain part of the pass, then persist the processed coverage.
    {
        auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 4);
        auto dynamicContext = MakeDynamicContext(
            /*period*/ TDuration::MilliSeconds(10),
            /*bufferRowLimit*/ 100);
        auto visitor = New<TKeyVisitor>(context, dynamicContext);
        WaitFor(visitor->Init()).ThrowOnError();

        DrainKeys(visitor, &collected, /*stopCount*/ 5);
        SyncOnQueue(visitor);
        StopOnQueue(visitor);
    }

    ASSERT_GE(std::ssize(collected), 5);
    ASSERT_LT(std::ssize(collected), std::ssize(seeded)) << "first visitor must not finish the whole pass";

    // Second visitor over the same persisted coverage: it must finish the pass
    // without re-emitting the keys the first visitor already processed.
    {
        auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 4);
        auto dynamicContext = MakeDynamicContext(
            /*period*/ TDuration::MilliSeconds(10),
            /*bufferRowLimit*/ 100);
        auto visitor = New<TKeyVisitor>(context, dynamicContext);
        WaitFor(visitor->Init()).ThrowOnError();

        DrainKeys(visitor, &collected, std::ssize(seeded) - std::ssize(collected));
        StopOnQueue(visitor);
    }

    EXPECT_EQ(ToSet(collected), expected) << "the two visitors together must cover every key";
    EXPECT_EQ(std::ssize(collected), std::ssize(seeded)) << "no key may be emitted twice across the recreation";
}

TEST_F(TKeyVisitorTest, NamesFilterIsApplied)
{
    SeedKeys({MakeUintKey(10), MakeUintKey(20)}, "/included");
    SeedKeys({MakeUintKey(30), MakeUintKey(40)}, "/excluded");

    auto context = MakeContext(
        MakeUintKeyRange(1, 100),
        /*names*/ THashSet<std::string>{"/included"},
        /*bucketCount*/ 2);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    const THashSet<TKey> expected{MakeUintKey(10), MakeUintKey(20)};
    std::vector<TKey> collected;
    DrainKeys(visitor, &collected, std::ssize(expected));
    EXPECT_EQ(ToSet(collected), expected);

    StopOnQueue(visitor);
}

TEST_F(TKeyVisitorTest, StopThenDestroyDoesNotCrash)
{
    SeedKeys({MakeUintKey(5)}, "/state");

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
    StopOnQueue(visitor);
    visitor.Reset();
}

// Regression: with several state-name rows per key, the per-iteration List
// used to leak duplicates. Each pass must emit every key exactly once even
// when KeyStates returns one row per (key, name) pair.
TEST_F(TKeyVisitorTest, MultiNameRowsEmitEveryKeyOncePerPass)
{
    std::vector<TKey> seeded;
    for (ui64 hash = 1; hash <= 8; ++hash) {
        seeded.push_back(MakeUintKey(hash * 5));
    }
    // 3 names per key — 24 rows total; dedup must collapse them to 8 keys.
    SeedKeys(seeded, "/name_a");
    SeedKeys(seeded, "/name_b");
    SeedKeys(seeded, "/name_c");
    const auto expected = ToSet(seeded);

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    std::vector<TKey> pass1;
    DrainKeys(visitor, &pass1, std::ssize(seeded));
    EXPECT_EQ(std::ssize(pass1), std::ssize(seeded)) << "pass 1: each key once, no duplicates";
    EXPECT_EQ(ToSet(pass1), expected);

    std::vector<TKey> pass2;
    DrainKeys(visitor, &pass2, std::ssize(seeded));
    EXPECT_EQ(std::ssize(pass2), std::ssize(seeded)) << "pass 2: each key once, no duplicates";
    EXPECT_EQ(ToSet(pass2), expected);

    StopOnQueue(visitor);
}

// Reconfigure must retarget the background fill period. It has to be safe both
// before Init (executor not created yet — the SetPeriod branch is skipped) and
// on a running executor (SetPeriod applied live). After a live speed-up the
// visitor must still emit every seeded key.
TEST_F(TKeyVisitorTest, ReconfigureUpdatesBackgroundFillPeriod)
{
    std::vector<TKey> seeded;
    for (ui64 hash = 1; hash <= 16; ++hash) {
        seeded.push_back(MakeUintKey(hash * 5));
    }
    SeedKeys(seeded, "/state");
    const auto expected = ToSet(seeded);

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 4);
    // Start with a large fill period so the idle cadence is effectively stalled.
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100,
        /*maxScanRowsPerIteration*/ 10'000,
        /*backgroundFillPeriod*/ TDuration::Seconds(1000));
    auto visitor = New<TKeyVisitor>(context, dynamicContext);

    // Reconfigure before Init: BackgroundFillExecutor_ is still null, so the
    // SetPeriod call must be skipped without a null deref.
    ReconfigureOnQueue(visitor, MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100,
        /*maxScanRowsPerIteration*/ 10'000,
        /*backgroundFillPeriod*/ TDuration::Seconds(1000)));

    WaitFor(visitor->Init()).ThrowOnError();

    // Live reconfigure on the running executor: speed the fill period back up.
    ReconfigureOnQueue(visitor, MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100,
        /*maxScanRowsPerIteration*/ 10'000,
        /*backgroundFillPeriod*/ TDuration::MilliSeconds(5)));

    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, std::ssize(seeded));
    EXPECT_EQ(ToSet(drained), expected)
        << "visitor must emit every key after background_fill_period is reconfigured";

    StopOnQueue(visitor);
}

// Regression: a partition range whose first column has the same uint hash on
// both bounds (only the trailing columns differ — e.g. [(50,"a"); (50,"z")))
// previously dropped its rows because GetRangeHashLength returned 0 and the
// throttler never acquired anything. GetRangeHashLength now clamps to >=1
// so the read goes through and every seeded key is visited.
TEST_F(TKeyVisitorTest, SingleHashMultiColumnRangeVisitsAllKeys)
{
    std::vector<TKey> seeded = {
        MakeKey(ui64(50), TStringBuf("alpha")),
        MakeKey(ui64(50), TStringBuf("beta")),
        MakeKey(ui64(50), TStringBuf("gamma")),
    };
    SeedKeys(seeded, "/state");
    const auto expected = ToSet(seeded);

    TKeyRange range{
        .Lower = MakeKey(ui64(50), TStringBuf("a")),
        .Upper = MakeKey(ui64(50), TStringBuf("z")),
    };
    auto context = MakeContext(range, /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, std::ssize(seeded));
    EXPECT_EQ(ToSet(drained), expected);

    StopOnQueue(visitor);
}

TEST_F(TKeyVisitorTest, NonFinalPassNeverReportsEmpty)
{
    SeedKeys({MakeUintKey(10), MakeUintKey(20)}, "/state");

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, /*stopCount*/ 4); // Two passes' worth.

    EXPECT_FALSE(IsEmptyOnQueue(visitor)) << "non-final visitor must never declare itself empty";

    StopOnQueue(visitor);
}

TEST_F(TKeyVisitorTest, EveryKeyVisitedAtLeastOnceBeforeEmpty)
{
    std::vector<TKey> seeded;
    for (ui64 hash = 1; hash <= 8; ++hash) {
        seeded.push_back(MakeUintKey(hash * 5));
    }
    SeedKeys(seeded, "/state");
    const auto expected = ToSet(seeded);

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 2);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    SetUpstreamCompletedOnQueue(visitor);

    std::vector<TKey> drained;
    const auto deadline = TInstant::Now() + TDuration::Seconds(5);
    while (TInstant::Now() < deadline) {
        DrainKeys(visitor, &drained, /*stopCount*/ 100, /*batchSize*/ 100, /*timeout*/ TDuration::MilliSeconds(100));
        SyncOnQueue(visitor);
        if (IsEmptyOnQueue(visitor)) {
            break;
        }
    }
    ASSERT_TRUE(IsEmptyOnQueue(visitor))
        << "visitor must declare itself empty after the final pass commits";

    const auto seen = ToSet(drained);
    EXPECT_TRUE(std::all_of(expected.begin(), expected.end(), [&] (const auto& key) {
        return seen.contains(key);
    })) << "every seeded key must appear at least once before the visitor reports Empty";

    StopOnQueue(visitor);
}

// A failing background-fill List must not crash the worker: the iteration sets
// the /background_fill status error and stays Idle instead of letting the
// exception escape the periodic callback. (If it threw, the test binary would
// terminate before any assertion below ran.)
TEST_F(TKeyVisitorTest, BackgroundFillListFailureSurfacesAsStatusErrorWithoutCrash)
{
    SeedKeys({MakeUintKey(10), MakeUintKey(20)}, "/state");
    KeyStates_->SetListFailure(TError("injected list failure"));

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    const auto deadline = TInstant::Now() + TDuration::Seconds(5);
    while (TInstant::Now() < deadline &&
        !context->StatusProfiler->GetStatus().Errors.contains("/background_fill"))
    {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(20));
    }
    EXPECT_TRUE(context->StatusProfiler->GetStatus().Errors.contains("/background_fill"))
        << "a failing List must surface as a /background_fill status error";

    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, /*stopCount*/ 1, /*batchSize*/ 100, /*timeout*/ TDuration::MilliSeconds(200));
    EXPECT_TRUE(drained.empty()) << "no visit may be emitted while List keeps failing";

    StopOnQueue(visitor);
}

// Once the List backend recovers, the next iteration clears /background_fill and
// the visitor resumes emitting every seeded key.
TEST_F(TKeyVisitorTest, BackgroundFillErrorClearsAfterListRecovers)
{
    const std::vector<TKey> seeded{MakeUintKey(10), MakeUintKey(20)};
    SeedKeys(seeded, "/state");
    KeyStates_->SetListFailure(TError("injected list failure"));

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    const auto failDeadline = TInstant::Now() + TDuration::Seconds(5);
    while (TInstant::Now() < failDeadline &&
        !context->StatusProfiler->GetStatus().Errors.contains("/background_fill"))
    {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(20));
    }
    ASSERT_TRUE(context->StatusProfiler->GetStatus().Errors.contains("/background_fill"))
        << "the error must be set before we clear the injected failure";

    // Clear the injected failure on the visitor's invoker — the fill loop reads
    // the flag on that same thread.
    WaitFor(BIND([this] {
        KeyStates_->SetListFailure(std::nullopt);
    })
            .AsyncVia(Queue_->GetInvoker())
            .Run())
        .ThrowOnError();

    const auto clearDeadline = TInstant::Now() + TDuration::Seconds(5);
    while (TInstant::Now() < clearDeadline &&
        context->StatusProfiler->GetStatus().Errors.contains("/background_fill"))
    {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(20));
    }
    EXPECT_FALSE(context->StatusProfiler->GetStatus().Errors.contains("/background_fill"))
        << "a succeeding List must clear the /background_fill status error";

    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, std::ssize(seeded));
    EXPECT_EQ(ToSet(drained), ToSet(seeded)) << "the visitor must resume emitting every key";

    StopOnQueue(visitor);
}

////////////////////////////////////////////////////////////////////////////////

// Regression: a near-uint64-max hash span produced a rate whose `period * rate`
// overflowed the throttler's double->i64 cast into a negative value.
TEST(TKeyVisitorSweepRateTest, LargeSpanIsClampedToI64SafeRate)
{
    for (const double period : {1e-6, 0.001, 1.0, 1.5, 1234.5}) {
        const auto span = static_cast<double>(std::numeric_limits<ui64>::max());
        const auto rate = ComputeKeyVisitorSweepRate(span, period);

        EXPECT_TRUE(std::isfinite(rate)) << "period=" << period;
        // The throttler holds up to (1s * rate) tokens as a double and casts to
        // i64 on acquire; (double)i64::max rounds up to 2^63, so the rate must
        // stay strictly below 2^63.
        EXPECT_LT(rate, std::ldexp(1.0, 63)) << "period=" << period;
        EXPECT_GE(static_cast<i64>(rate), 0)
            << "the throttler's double->i64 cast must stay non-negative; period=" << period;
    }
}

// A bounded span keeps the exact "cover the span in one period" rate.
TEST(TKeyVisitorSweepRateTest, BoundedSpanKeepsExactRate)
{
    const double period = 2.0;
    const double span = 1000.0;
    EXPECT_DOUBLE_EQ(ComputeKeyVisitorSweepRate(span, period), span / period);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
