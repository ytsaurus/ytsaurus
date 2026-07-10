#include "key_visitor.h"

#include "job_state/state_manager.h"

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/flow/library/cpp/tables/key_states.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <algorithm>
#include <limits>
#include <set>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration BackgroundFillIdlePeriod = TDuration::MilliSeconds(50);

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 GetRangeHashLength(const TKeyRange& range)
{
    if (range.Lower == range.Upper) {
        return 0;
    }
    const auto lower = ExtractFirstUintFromKey(range.Lower)
        .value_or(std::numeric_limits<ui64>::min());
    const auto upper = ExtractFirstUintFromKey(range.Upper)
        .value_or(std::numeric_limits<ui64>::max());
    YT_VERIFY(upper >= lower);
    // A non-empty range always spans at least one unit: keys that differ only
    // beyond the hash column (e.g. [[5u, "a"]; [5u, "z"])) have an equal hash
    // bound but still contain keys, so they must be read, not skipped.
    return std::max<ui64>(1, upper - lower);
}

TKeyRange TruncateRangeHashLength(const TKeyRange& range, ui64 length)
{
    if (length == 0) {
        return TKeyRange{.Lower = range.Lower, .Upper = range.Lower};
    }
    if (length >= GetRangeHashLength(range)) {
        return range;
    }
    const auto lower = ExtractFirstUintFromKey(range.Lower)
        .value_or(std::numeric_limits<ui64>::min());
    return TKeyRange{
        .Lower = range.Lower,
        .Upper = MakeUintKey(lower + length),
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

double ComputeKeyVisitorSweepRate(double span, double periodSeconds)
{
    YT_VERIFY(periodSeconds > 0);
    // Floor at 1/period: a sub-1 limit never lets anything through.
    const auto rate = std::max(1.0 / periodSeconds, span / periodSeconds);
    // The throttler holds up to (1s * limit) tokens as a double and casts them to
    // i64 on acquire; cap the rate at half of i64::max so that product never
    // overflows the cast (which would wrap negative and trip YT_VERIFY).
    const auto maxRate = std::numeric_limits<i64>::max() / 2.0;
    return std::min(rate, maxRate);
}

////////////////////////////////////////////////////////////////////////////////

TKeyVisitor::TKeyVisitor(
    TKeyVisitorContextPtr context,
    TDynamicKeyVisitorContextPtr dynamicContext)
    : Context_(std::move(context))
    , DynamicContext_(std::move(dynamicContext))
    , Throttler_(CreateReconfigurableThroughputThrottler(
        BuildThrottlerConfig(),
        Context_->Logger,
        Context_->Profiler.WithPrefix("/throttler")))
    , Logger(Context_->Logger)
    , Store_(New<TKeyVisitorStore>(
        Context_->ComputationId,
        Context_->StreamId,
        Context_->PartitionRange,
        Context_->Spec->BucketCount,
        Context_->KeyVisitorStates,
        Logger))
    , ScanCapStallError_(Context_->StatusProfiler->ErrorState("/scan_cap_stall"))
    , BackgroundFillError_(Context_->StatusProfiler->ErrorState("/background_fill"))
    , RegisteredCounter_(Context_->Profiler.Counter("/registered_count"))
    , PersistedCounter_(Context_->Profiler.Counter("/persisted_count"))
{
    YT_VERIFY(Context_->Spec);
}

TFuture<void> TKeyVisitor::Init()
{
    return BIND([this, strongThis = MakeStrong(this)] {
        WaitFor(Store_->Init()).ThrowOnError();
        BackgroundFillExecutor_ = New<TPeriodicExecutor>(
            Context_->SerializedInvoker,
            BIND(&TKeyVisitor::RunBackgroundFillIteration, MakeWeak(this)),
            BackgroundFillIdlePeriod);
        BackgroundFillExecutor_->Start();
    })
        .AsyncVia(Context_->SerializedInvoker)
        .Run();
}

void TKeyVisitor::Stop()
{
    if (BackgroundFillExecutor_) {
        YT_UNUSED_FUTURE(BackgroundFillExecutor_->Stop());
        BackgroundFillExecutor_.Reset();
    }
}

void TKeyVisitor::Reconfigure(TDynamicKeyVisitorContextPtr dynamicContext)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    YT_VERIFY(dynamicContext);
    DynamicContext_ = std::move(dynamicContext);
    Throttler_->Reconfigure(BuildThrottlerConfig());
}

void TKeyVisitor::SetUpstreamCompleted()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    UpstreamCompleted_ = true;
    if (BackgroundFillExecutor_) {
        BackgroundFillExecutor_->ScheduleOutOfBand();
    }
}

std::vector<TVisit> TKeyVisitor::GetNextBatch(i64 batchSize)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    if (batchSize <= 0 || DynamicContext_->Draining) {
        return {};
    }
    std::vector<TVisit> result;
    result.reserve(std::min<i64>(batchSize, BufferRowCount_));
    while (std::ssize(result) < batchSize && !Buffer_.empty()) {
        auto& front = Buffer_.front();
        const i64 want = batchSize - std::ssize(result);
        if (std::ssize(front.Visits) <= want) {
            // Consume the whole chunk and promote its full range.
            for (auto& visit : front.Visits) {
                result.push_back(std::move(visit));
            }
            BufferRowCount_ -= std::ssize(front.Visits);
            Store_->MarkCommitted(front.Range);
            Buffer_.pop_front();
        } else {
            // Take a prefix; split the chunk's range at the first remaining
            // visit's key. Visits arrive sorted by key from KeyStates->List,
            // so visits[want].Key > visits[want-1].Key and remains > Range.Lower.
            TKey splitKey = front.Visits[want].Key;
            TKeyRange takenRange{front.Range.Lower, splitKey};
            for (i64 i = 0; i < want; ++i) {
                result.push_back(std::move(front.Visits[i]));
            }
            front.Visits.erase(front.Visits.begin(), front.Visits.begin() + want);
            front.Range.Lower = std::move(splitKey);
            BufferRowCount_ -= want;
            Store_->MarkCommitted(takenRange);
            break;
        }
    }
    if (!result.empty()) {
        const auto consumed = static_cast<double>(std::ssize(result));
        ConsumedRate_.Inc(consumed);
        PersistedCounter_.Increment(static_cast<i64>(consumed));
    }
    return result;
}

THashMap<TStreamId, TInflightStreamTraverseDataPtr> TKeyVisitor::BuildInflight() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    auto inflight = New<TInflightStreamTraverseData>();
    inflight->Suspended = DynamicContext_->Draining;
    inflight->Empty =
        Store_->IsCurrentPassFinal() && Store_->IsAllCommitted() && Buffer_.empty();
    inflight->InflightMetrics->Count = inflight->Empty ? 0 : BufferRowCount_;
    inflight->InflightMetrics->NewCountPerSec = EmittedRate_.GetRate().value_or(0);
    inflight->InflightMetrics->ProcessedCountPerSec = ConsumedRate_.GetRate().value_or(0);
    if (!inflight->Empty && !Buffer_.empty() && !Buffer_.front().Visits.empty()) {
        const auto& head = Buffer_.front().Visits.front();
        inflight->MinSystemTimestamp = head.SystemTimestamp;
        inflight->MinEventTimestamp = head.EventTimestamp;
    }
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> result;
    result[Context_->StreamId] = std::move(inflight);
    return result;
}

void TKeyVisitor::Sync(NApi::IDynamicTableTransactionPtr transaction)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    Store_->Sync(std::move(transaction));
}

NConcurrency::TThroughputThrottlerConfigPtr TKeyVisitor::BuildThrottlerConfig() const
{
    auto config = New<NConcurrency::TThroughputThrottlerConfig>();
    if (DynamicContext_ && DynamicContext_->DynamicSpec) {
        const auto period = DynamicContext_->DynamicSpec->Period.SecondsFloat();
        if (period > 0) {
            // Cover the partition's hash range in one period.
            const auto lower = ExtractFirstUintFromKey(Context_->PartitionRange.Lower)
                .value_or(std::numeric_limits<ui64>::min());
            const auto upper = ExtractFirstUintFromKey(Context_->PartitionRange.Upper)
                .value_or(std::numeric_limits<ui64>::max());
            YT_VERIFY(upper >= lower);
            const auto span = static_cast<double>(upper - lower);
            config->Limit = ComputeKeyVisitorSweepRate(span, period);
        }
    }
    return config;
}

bool TKeyVisitor::IsInCatchup() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    return InCatchup_;
}

void TKeyVisitor::RunBackgroundFillIteration()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    if (DoRunBackgroundFillIteration() == EIterationOutcome::ContinueImmediately) {
        BackgroundFillExecutor_->ScheduleOutOfBand();
    }
}

TKeyVisitor::EIterationOutcome TKeyVisitor::DoRunBackgroundFillIteration()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    // This runs in a periodic-executor callback with no surrounding handler, so
    // an escaping exception terminates the worker. Catch std::exception (not
    // ...) to keep surfacing the error while letting fiber cancellation
    // propagate; YT_VERIFY in the body is a trap, not an exception, so config
    // invariants still abort.
    try {
        auto outcome = DoRunBackgroundFillIterationGuarded();
        BackgroundFillError_->ClearError();
        return outcome;
    } catch (const std::exception& ex) {
        // Retry next tick: a transient backend error clears, a stable one stays
        // visible in the status profiler.
        BackgroundFillError_->SetError(TError(ex)
            << TErrorAttribute("computation_id", Context_->ComputationId.Underlying())
            << TErrorAttribute("stream_id", Context_->StreamId.Underlying()));
        return EIterationOutcome::Idle;
    }
}

TKeyVisitor::EIterationOutcome TKeyVisitor::DoRunBackgroundFillIterationGuarded()
{
    if (DynamicContext_->Draining) {
        return EIterationOutcome::Idle;
    }
    if (BufferRowCount_ >= DynamicContext_->DynamicSpec->BufferRowLimit) {
        return EIterationOutcome::Idle;
    }

    auto range = Store_->GetNextRange();
    if (!range) {
        if (!Store_->IsAllCommitted()) {
            return EIterationOutcome::Idle;
        }
        if (Store_->IsCurrentPassFinal()) {
            return EIterationOutcome::Idle;
        }
        Store_->StartNewPass(/*finalPass*/ UpstreamCompleted_);
        return EIterationOutcome::ContinueImmediately;
    }

    // Catch-up scales the scanned slice (not the throttler config) — Reconfigure
    // would drop accumulated delays and briefly free-run.
    ui64 acquired = Throttler_->TryAcquireAvailable(static_cast<i64>(
        std::min<ui64>(GetRangeHashLength(*range), std::numeric_limits<i64>::max())));
    if (acquired == 0) {
        return EIterationOutcome::Idle;
    }
    if (InCatchup_) {
        acquired = static_cast<ui64>(acquired * DynamicContext_->DynamicSpec->CatchupSpeedupMultiplier);
    }
    auto throttledRange = TruncateRangeHashLength(*range, acquired);

    const auto& names = Context_->Spec->Names;
    const auto& externalNames = Context_->Spec->ExternalNames;
    const bool scanAll = !names && !externalNames;
    const bool scanInternal = scanAll || (names && !names->empty());
    const bool scanExternal = scanAll || (externalNames && !externalNames->empty());

    const i64 maxRows = DynamicContext_->DynamicSpec->MaxScanRowsPerIteration;
    YT_VERIFY(maxRows > 0);

    std::vector<TFuture<std::vector<TKey>>> futures;
    if (scanInternal) {
        NTables::IKeyStates::TTableKeyFilter filter;
        filter.ComputationId = Context_->ComputationId;
        if (names && !names->empty()) {
            filter.Names = std::vector<std::string>(names->begin(), names->end());
        }
        filter.LowerKey = throttledRange.Lower;
        filter.UpperKey = throttledRange.Upper;
        futures.push_back(Context_->KeyStates->List(filter, maxRows, std::nullopt)
                .AsUnique()
                .Apply(BIND([] (NTables::IKeyStates::TListResult&& result) {
                    std::vector<TKey> keys;
                    keys.reserve(result.Keys.size());
                    for (auto& tableKey : result.Keys) {
                        keys.push_back(std::move(tableKey.Key));
                    }
                    return keys;
                })));
    }
    if (scanExternal && Context_->StateManager) {
        IExternalStateManager::TFilter managerFilter;
        managerFilter.LowerKey = throttledRange.Lower;
        managerFilter.UpperKey = throttledRange.Upper;
        for (const auto& [name, manager] : Context_->StateManager->GetExternalStateManagers()) {
            if (externalNames && !externalNames->contains(name)) {
                continue;
            }
            futures.push_back(manager->List(managerFilter, maxRows, std::nullopt)
                    .AsUnique()
                    .Apply(BIND([] (IExternalStateManager::TListResult&& result) {
                        return std::move(result.Keys);
                    })));
        }
        IExternalStateJoiner::TFilter joinerFilter;
        joinerFilter.LowerKey = throttledRange.Lower;
        joinerFilter.UpperKey = throttledRange.Upper;
        for (const auto& [name, joiner] : Context_->StateManager->GetExternalStateJoiners()) {
            // Unlike a state manager, a joiner is swept only when explicitly named in
            // external_names — scan-all never sweeps joiners. A visitor-driven joiner holds a
            // single sweep cursor and is bound to at most one stream (validated at construction).
            if (!externalNames || !externalNames->contains(name)) {
                continue;
            }
            if (!joiner->IsVisitorDriven()) {
                if (NonVisitorJoinerWarned_.insert(name).second) {
                    YT_LOG_WARNING("Joiner %Qv referenced by key_visitor_stream external_names is "
                        "not visitor-driven and will not be swept",
                        name);
                }
                continue;
            }
            futures.push_back(joiner->List(joinerFilter, maxRows, std::nullopt)
                    .AsUnique()
                    .Apply(BIND([] (IExternalStateJoiner::TListResult&& result) {
                        return std::move(result.Keys);
                    })));
        }
    }

    std::vector<TFuture<void>> waits;
    waits.reserve(futures.size());
    for (const auto& future : futures) {
        waits.push_back(future.AsVoid());
    }
    WaitFor(AllSucceeded(std::move(waits))).ThrowOnError();

    // Cap-hit invariant: a source that returned exactly `maxRows` may have
    // unread rows past its last returned key. Truncating the read-range at
    // the smallest such last-key across all hit-cap sources keeps the tail
    // Pending in the store so the next iteration re-reads it from scratch —
    // no keys are lost.
    std::set<TKey> sortedKeys;
    std::optional<TKey> truncationUpper;
    for (auto& future : futures) {
        auto keys = WaitFor(future).ValueOrThrow();
        if (std::ssize(keys) >= maxRows) {
            auto& candidate = keys.back();
            if (!truncationUpper || candidate < *truncationUpper) {
                truncationUpper = candidate;
            }
        }
        for (auto& key : keys) {
            sortedKeys.insert(std::move(key));
        }
    }

    TKeyRange readRange = throttledRange;
    if (truncationUpper) {
        sortedKeys.erase(sortedKeys.lower_bound(*truncationUpper), sortedKeys.end());
        readRange.Upper = *truncationUpper;
    }

    if (truncationUpper && readRange.Upper == readRange.Lower) {
        ScanCapStallError_->SetError(TError(
            "Key visitor stalled: a single key has more than max_scan_rows_per_iteration = %v rows",
            maxRows));
        return EIterationOutcome::Idle;
    }
    ScanCapStallError_->ClearError();

    const auto [now, uniqueSeqNo] = WaitFor(Context_->TimeProvider->GenerateGlobalUniqueSeqNo())
        .ValueOrThrow();
    Store_->MarkBuffered(readRange, now);

    // Snapshot schedule lag once: anchors both the per-visit EventTimestamp
    // stamp and the catch-up toggle. Sweep anchor = oldest non-Pending
    // interval's PassStartedAt (persisted in key_visitor_states, so it
    // survives restart/rebalance); on a fresh pass we fall back to "now".
    const auto period = DynamicContext_->DynamicSpec->Period;
    const ui64 nowSeconds = now.Underlying();
    const auto minPassStartedAt = Store_->GetMinPassStartedAt();
    const ui64 startSeconds = minPassStartedAt
        ? minPassStartedAt->Underlying()
        : nowSeconds;
    const double elapsedSeconds = nowSeconds > startSeconds
        ? static_cast<double>(nowSeconds - startSeconds)
        : 0.0;
    const double scheduledSeconds = Store_->GetScannedFraction() * period.SecondsFloat();
    const double rawLagSeconds = elapsedSeconds - scheduledSeconds;
    const auto scheduleLagSeconds = static_cast<ui64>(std::max(0.0, rawLagSeconds));

    InCatchup_ = rawLagSeconds >
        DynamicContext_->DynamicSpec->CatchupLagThreshold.SecondsFloat();

    TBufferEntry entry;
    entry.Range = readRange;
    if (!sortedKeys.empty()) {
        // EventTimestamp = scheduled visit time = now − scheduleLag (clamped
        // to "now" so it never exceeds current YT time).
        const auto expectedEventTimestamp = TSystemTimestamp(nowSeconds - scheduleLagSeconds);

        i64 index = 0;
        for (const auto& key : sortedKeys) {
            TVisit visit;
            visit.MessageId = GenerateOrderedMessageId(
                uniqueSeqNo,
                Context_->StreamId,
                LexicographicallySerialize(index));
            visit.SystemTimestamp = now;
            visit.EventTimestamp = expectedEventTimestamp;
            visit.AlignmentTimestamp = now;
            visit.StreamId = Context_->StreamId;
            visit.Key = key;
            entry.Visits.push_back(std::move(visit));
            ++index;
        }
    }
    if (!entry.Visits.empty()) {
        const auto emitted = static_cast<double>(std::ssize(entry.Visits));
        EmittedRate_.Inc(emitted);
        RegisteredCounter_.Increment(static_cast<i64>(emitted));
    }
    BufferRowCount_ += std::ssize(entry.Visits);
    Buffer_.push_back(std::move(entry));
    return truncationUpper ? EIterationOutcome::ContinueImmediately : EIterationOutcome::Idle;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
