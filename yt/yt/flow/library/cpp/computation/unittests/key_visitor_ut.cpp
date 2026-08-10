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
#include <utility>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TBlockingFirstListKeyStates
    : public NTables::TInMemoryKeyStates
{
public:
    TFuture<TListResult> List(
        TTableKeyFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) override
    {
        auto result = WaitFor(NTables::TInMemoryKeyStates::List(
            std::move(filter),
            limit,
            std::move(offsetExclusive)))
            .ValueOrThrow();
        if (!std::exchange(BlockFirstList_, false)) {
            return MakeFuture(std::move(result));
        }

        FirstListStartedPromise_.Set();
        return ReleaseFirstListPromise_.ToFuture().Apply(BIND([
            result = std::move(result)
        ] () mutable {
            return std::move(result);
        }));
    }

    TFuture<void> GetFirstListStartedFuture() const
    {
        return FirstListStartedPromise_.ToFuture();
    }

    void ReleaseFirstList()
    {
        ReleaseFirstListPromise_.Set();
    }

private:
    bool BlockFirstList_ = true;
    const TPromise<void> FirstListStartedPromise_ = NewPromise<void>();
    const TPromise<void> ReleaseFirstListPromise_ = NewPromise<void>();
};

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
        TDuration backgroundFillPeriod = TDuration::MilliSeconds(50),
        bool finite = true,
        bool fullFinalPass = true)
    {
        auto dynSpec = New<TDynamicKeyVisitorStreamSpec>();
        dynSpec->Period = period;
        dynSpec->BufferRowLimit = NYTree::TSize(bufferRowLimit);
        dynSpec->MaxScanRowsPerIteration = NYTree::TSize(maxScanRowsPerIteration);
        dynSpec->BackgroundFillPeriod = backgroundFillPeriod;
        dynSpec->Finite = finite;
        dynSpec->FullFinalPass = fullFinalPass;

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

// A non-finite visitor is a periodic scanner: the completion signal means nothing to it.
TEST_F(TKeyVisitorTest, NonFiniteVisitorIgnoresUpstreamCompletion)
{
    const std::vector<TKey> seeded{MakeUintKey(10), MakeUintKey(20)};
    SeedKeys(seeded, "/state");

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100,
        /*maxScanRowsPerIteration*/ 10'000,
        /*backgroundFillPeriod*/ TDuration::MilliSeconds(50),
        /*finite*/ false);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    SetUpstreamCompletedOnQueue(visitor);

    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, /*stopCount*/ 3 * std::ssize(seeded));
    EXPECT_EQ(std::ssize(drained), 3 * std::ssize(seeded)) << "a non-finite visitor keeps sweeping";
    EXPECT_FALSE(IsEmptyOnQueue(visitor)) << "a non-finite visitor must never declare itself empty";

    StopOnQueue(visitor);
}

// A no-upstream computation marks its initial pass Final before starting the fill, so the
// first scan is also the last one.
TEST_F(TKeyVisitorTest, CompletionAtInitSeedsFinalFirstPass)
{
    const std::vector<TKey> seeded{MakeUintKey(10), MakeUintKey(20)};
    auto keyStates = New<TBlockingFirstListKeyStates>();
    for (const auto& key : seeded) {
        keyStates->Set({ComputationId, key, "/state"});
    }

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    context->KeyStates = keyStates;
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init(/*upstreamCompleted*/ true)).ThrowOnError();
    WaitFor(keyStates->GetFirstListStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    keyStates->ReleaseFirstList();

    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, std::ssize(seeded));
    EXPECT_EQ(ToSet(drained), ToSet(seeded)) << "the seeded final pass must emit every key";
    EXPECT_TRUE(IsEmptyOnQueue(visitor)) << "the visitor must be empty after its single pass";

    std::vector<TKey> extra;
    DrainKeys(visitor, &extra, /*stopCount*/ 1, /*batchSize*/ 100, /*timeout*/ TDuration::MilliSeconds(500));
    EXPECT_TRUE(extra.empty()) << "no pass may follow the seeded final one";

    StopOnQueue(visitor);
}

// A background read can suspend after taking its snapshot while the last upstream epoch
// commits. The completion signal must not finalize that still-unrecorded scan in place: the
// following Final pass is what observes the newly committed key.
TEST_F(TKeyVisitorTest, CompletionDuringBackgroundReadStartsFreshFinalPass)
{
    auto keyStates = New<TBlockingFirstListKeyStates>();
    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    context->KeyStates = keyStates;
    auto visitor = New<TKeyVisitor>(
        context,
        MakeDynamicContext(
            /*period*/ TDuration::MilliSeconds(10),
            /*bufferRowLimit*/ 100));
    WaitFor(visitor->Init()).ThrowOnError();
    WaitFor(keyStates->GetFirstListStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();

    const auto committedAfterSnapshot = MakeUintKey(20);
    keyStates->Set({ComputationId, committedAfterSnapshot, "/state"});
    SetUpstreamCompletedOnQueue(visitor);
    keyStates->ReleaseFirstList();

    std::vector<TKey> drained;
    EXPECT_EQ(DrainKeys(visitor, &drained, /*stopCount*/ 1), 1)
        << "the Final pass must observe state committed after the stale read began";
    EXPECT_EQ(drained, std::vector<TKey>{committedAfterSnapshot});
    EXPECT_TRUE(IsEmptyOnQueue(visitor)) << "the visitor must retire after the fresh Final pass";

    StopOnQueue(visitor);
}

// A completion signal received before anything is swept finalizes the pass in hand.
TEST_F(TKeyVisitorTest, FiniteVisitorFinalizesUnsweptPassInPlace)
{
    const std::vector<TKey> seeded{MakeUintKey(10), MakeUintKey(20)};
    SeedKeys(seeded, "/state");

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    // Hold the scanner until the signal lands: the kickstarted background fill
    // may otherwise sweep a range first, and a swept pass is finalized at rotation, not
    // in place.
    dynamicContext->Draining = true;
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    SetUpstreamCompletedOnQueue(visitor);
    ReconfigureOnQueue(
        visitor,
        MakeDynamicContext(
            /*period*/ TDuration::MilliSeconds(10),
            /*bufferRowLimit*/ 100));

    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, std::ssize(seeded));
    EXPECT_EQ(ToSet(drained), ToSet(seeded)) << "the final pass must still emit every key";
    EXPECT_TRUE(IsEmptyOnQueue(visitor)) << "the visitor must be empty after its single pass";

    std::vector<TKey> extra;
    DrainKeys(visitor, &extra, /*stopCount*/ 1, /*batchSize*/ 100, /*timeout*/ TDuration::MilliSeconds(500));
    EXPECT_TRUE(extra.empty()) << "no pass may follow the final one";

    StopOnQueue(visitor);
}

// The signal arrives mid-sweep, so the pass in flight is not the one that gets finalized:
// the guarantee of a complete sweep after completion is preserved.
TEST_F(TKeyVisitorTest, FiniteVisitorFinishesSweptPassBeforeFinalizing)
{
    std::vector<TKey> seeded;
    for (ui64 hash = 1; hash <= 8; ++hash) {
        seeded.push_back(MakeUintKey(hash * 5));
    }
    SeedKeys(seeded, "/state");

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    // Consume part of the first pass, then signal: the rest of it plus one more full pass
    // must still be emitted. One key at a time, so the pass stays genuinely mid-sweep —
    // a batch large enough to drain the buffer would commit the pass and rotate it.
    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, /*stopCount*/ 1, /*batchSize*/ 1);
    ASSERT_FALSE(drained.empty());
    SetUpstreamCompletedOnQueue(visitor);
    EXPECT_FALSE(IsEmptyOnQueue(visitor)) << "a swept pass may not be finalized where it stands";

    // Count what actually comes out: a pass marked Final in place would not report Empty
    // until it commits either, so only the emitted total tells the two cases apart.
    const auto wanted = 2 * std::ssize(seeded) - std::ssize(drained);
    EXPECT_EQ(DrainKeys(visitor, &drained, wanted), wanted) << "a full further pass must be emitted";
    EXPECT_TRUE(IsEmptyOnQueue(visitor)) << "the visitor must be empty after the following pass";

    StopOnQueue(visitor);
}

// Waiving the full-sweep guarantee finalizes the pass in flight instead of the next one.
TEST_F(TKeyVisitorTest, FullFinalPassDisabledFinalizesSweptPass)
{
    std::vector<TKey> seeded;
    for (ui64 hash = 1; hash <= 8; ++hash) {
        seeded.push_back(MakeUintKey(hash * 5));
    }
    SeedKeys(seeded, "/state");

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto dynamicContext = MakeDynamicContext(
        /*period*/ TDuration::MilliSeconds(10),
        /*bufferRowLimit*/ 100,
        /*maxScanRowsPerIteration*/ 10'000,
        /*backgroundFillPeriod*/ TDuration::MilliSeconds(50),
        /*finite*/ true,
        /*fullFinalPass*/ false);
    auto visitor = New<TKeyVisitor>(context, dynamicContext);
    WaitFor(visitor->Init()).ThrowOnError();

    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, /*stopCount*/ 1, /*batchSize*/ 1);
    ASSERT_FALSE(drained.empty());
    SetUpstreamCompletedOnQueue(visitor);

    // The remainder of the pass in flight is still emitted, but nothing beyond it.
    DrainKeys(visitor, &drained, std::ssize(seeded) - std::ssize(drained));
    EXPECT_TRUE(IsEmptyOnQueue(visitor)) << "the pass in flight must be the final one";

    std::vector<TKey> extra;
    DrainKeys(visitor, &extra, /*stopCount*/ 1, /*batchSize*/ 100, /*timeout*/ TDuration::MilliSeconds(500));
    EXPECT_TRUE(extra.empty()) << "no further pass may run";

    StopOnQueue(visitor);
}

// Switching back to non-finite must keep the scanner running: the completion signal is not
// a latch that outlives the switch.
TEST_F(TKeyVisitorTest, ReconfigureBackToNonFiniteKeepsSweeping)
{
    std::vector<TKey> seeded;
    for (ui64 hash = 1; hash <= 8; ++hash) {
        seeded.push_back(MakeUintKey(hash * 5));
    }
    SeedKeys(seeded, "/state");

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto visitor = New<TKeyVisitor>(
        context,
        MakeDynamicContext(
            /*period*/ TDuration::MilliSeconds(10),
            /*bufferRowLimit*/ 100));
    WaitFor(visitor->Init()).ThrowOnError();

    // Signal mid-sweep, so the pass in flight is not finalized and the decision is deferred
    // to the rotation. One key at a time: a batch that drains the buffer would commit the
    // whole pass and rotate it into a final one before the switch lands.
    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, /*stopCount*/ 1, /*batchSize*/ 1);
    ASSERT_FALSE(drained.empty());
    SetUpstreamCompletedOnQueue(visitor);

    ReconfigureOnQueue(
        visitor,
        MakeDynamicContext(
            /*period*/ TDuration::MilliSeconds(10),
            /*bufferRowLimit*/ 100,
            /*maxScanRowsPerIteration*/ 10'000,
            /*backgroundFillPeriod*/ TDuration::MilliSeconds(50),
            /*finite*/ false));
    SetUpstreamCompletedOnQueue(visitor);

    // The rotation must not inherit the withdrawn signal.
    DrainKeys(visitor, &drained, 3 * std::ssize(seeded));
    EXPECT_FALSE(IsEmptyOnQueue(visitor)) << "a withdrawn completion must not finalize a pass";

    StopOnQueue(visitor);
}

// Switching a running scanner into finite mode is what asks it to finish; the flip arrives
// through Reconfigure, exactly as a dynamic-spec update delivers it in production.
TEST_F(TKeyVisitorTest, ReconfigureToFiniteTerminatesRunningVisitor)
{
    const std::vector<TKey> seeded{MakeUintKey(10), MakeUintKey(20)};
    SeedKeys(seeded, "/state");

    auto context = MakeContext(MakeUintKeyRange(1, 100), /*names*/ std::nullopt, /*bucketCount*/ 1);
    auto visitor = New<TKeyVisitor>(
        context,
        MakeDynamicContext(
            /*period*/ TDuration::MilliSeconds(10),
            /*bufferRowLimit*/ 100,
            /*maxScanRowsPerIteration*/ 10'000,
            /*backgroundFillPeriod*/ TDuration::MilliSeconds(50),
            /*finite*/ false));
    WaitFor(visitor->Init()).ThrowOnError();

    SetUpstreamCompletedOnQueue(visitor);
    std::vector<TKey> drained;
    DrainKeys(visitor, &drained, /*stopCount*/ 2 * std::ssize(seeded));
    ASSERT_FALSE(IsEmptyOnQueue(visitor));

    ReconfigureOnQueue(
        visitor,
        MakeDynamicContext(
            /*period*/ TDuration::MilliSeconds(10),
            /*bufferRowLimit*/ 100,
            /*maxScanRowsPerIteration*/ 10'000,
            /*backgroundFillPeriod*/ TDuration::MilliSeconds(50),
            /*finite*/ true));
    SetUpstreamCompletedOnQueue(visitor);

    std::vector<TKey> tail;
    DrainKeys(visitor, &tail, /*stopCount*/ 2 * std::ssize(seeded));
    EXPECT_TRUE(IsEmptyOnQueue(visitor)) << "the flip must let the visitor finish";

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
