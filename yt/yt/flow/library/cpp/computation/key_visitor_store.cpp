#include "key_visitor_store.h"

#include <yt/yt/flow/library/cpp/tables/key_visitor_states.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <algorithm>
#include <limits>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TKeyVisitorInterval::Register(TRegistrar registrar)
{
    registrar.Parameter("lower", &TThis::Lower);
    registrar.Parameter("upper", &TThis::Upper);
    registrar.Parameter("pass_started_at", &TThis::PassStartedAt)
        .Default(TSystemTimestamp());
    registrar.Parameter("final_pass", &TThis::FinalPass)
        .Default(false)
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

using EIntervalState = TKeyVisitorStore::EIntervalState;
using TBucket = TKeyVisitorStore::TBucket;
using TIntervalSlot = TKeyVisitorStore::TIntervalSlot;

//! Splits the interval list so that |key| becomes a boundary between two
//! adjacent intervals. No-op if |key| already lies on a boundary.
void SplitAt(std::vector<TIntervalSlot>& intervals, const TKey& key)
{
    auto it = std::lower_bound(
        intervals.begin(),
        intervals.end(),
        key,
        [] (const auto& slot, const TKey& key) {
            return slot.Interval->Upper <= key;
        });
    if (it == intervals.end()) {
        return;
    }
    if (it->Interval->Lower == key || it->Interval->Upper == key) {
        return;
    }
    auto right = New<TKeyVisitorInterval>();
    right->Lower = key;
    right->Upper = it->Interval->Upper;
    right->PassStartedAt = it->Interval->PassStartedAt;
    right->FinalPass = it->Interval->FinalPass;
    it->Interval->Upper = key;
    intervals.insert(it + 1, TIntervalSlot{std::move(right), it->State});
}

//! Merges adjacent intervals sharing the same state in place, reusing the
//! vector's storage. The merged interval inherits the minimum PassStartedAt
//! of its inputs. Typical input has 1–2 intervals so this is O(n) with tiny
//! constants.
void CoalesceInplace(std::vector<TIntervalSlot>& intervals)
{
    size_t w = 0;
    for (size_t r = 0; r < intervals.size(); ++r) {
        if (w > 0 &&
            intervals[w - 1].State == intervals[r].State &&
            intervals[w - 1].Interval->Upper == intervals[r].Interval->Lower &&
            intervals[w - 1].Interval->FinalPass == intervals[r].Interval->FinalPass)
        {
            intervals[w - 1].Interval->Upper = intervals[r].Interval->Upper;
            intervals[w - 1].Interval->PassStartedAt = std::min(
                intervals[w - 1].Interval->PassStartedAt,
                intervals[r].Interval->PassStartedAt);
        } else {
            if (w != r) {
                intervals[w] = std::move(intervals[r]);
            }
            ++w;
        }
    }
    intervals.resize(w);
}

//! Stamps every sub-interval covered by [range.Lower; range.Upper) with
//! |state| and coalesces adjacent same-state intervals in place. When
//! |stamp| is set, PassStartedAt is overwritten on the touched slots
//! (Pending→Buffered transitions and Init seeding); when |stamp| is nullopt
//! the existing PassStartedAt is preserved (Buffered→Committed transitions).
void MarkRange(
    std::vector<TIntervalSlot>& intervals,
    const TKeyRange& range,
    EIntervalState state,
    std::optional<TSystemTimestamp> stamp)
{
    if (!(range.Lower < range.Upper)) {
        return;
    }
    SplitAt(intervals, range.Lower);
    SplitAt(intervals, range.Upper);
    for (auto& slot : intervals) {
        if (slot.Interval->Upper <= range.Lower) {
            continue;
        }
        if (slot.Interval->Lower >= range.Upper) {
            break;
        }
        slot.State = state;
        if (stamp) {
            slot.Interval->PassStartedAt = *stamp;
        }
    }
    CoalesceInplace(intervals);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TKeyVisitorStore::TBucket::Reset(bool finalPass)
{
    auto seed = New<TKeyVisitorInterval>();
    seed->Lower = Range.Lower;
    seed->Upper = Range.Upper;
    seed->FinalPass = finalPass;
    Intervals.clear();
    Intervals.push_back(TIntervalSlot{std::move(seed), EIntervalState::Pending});
}

void TKeyVisitorStore::TBucket::MarkBuffered(const TKeyRange& range, TSystemTimestamp when)
{
    MarkRange(Intervals, range, EIntervalState::Buffered, when);
}

void TKeyVisitorStore::TBucket::MarkCommitted(const TKeyRange& range)
{
    MarkRange(Intervals, range, EIntervalState::Committed, std::nullopt);
}

void TKeyVisitorStore::TBucket::SeedCommitted(const TKeyRange& range, TSystemTimestamp stampedAt, bool finalPass)
{
    // FinalPass is already carried by the Pending seed (Reset); SplitAt
    // preserves it, so we only assert consistency here.
    YT_VERIFY(Intervals.empty() || Intervals.front().Interval->FinalPass == finalPass);
    MarkRange(Intervals, range, EIntervalState::Committed, stampedAt);
}

////////////////////////////////////////////////////////////////////////////////

TKeyVisitorStore::TKeyVisitorStore(
    TComputationId computationId,
    TStreamId streamId,
    TKeyRange partitionRange,
    int bucketCount,
    NTables::IKeyVisitorStatesPtr backend,
    NLogging::TLogger logger)
    : ComputationId_(std::move(computationId))
    , StreamId_(std::move(streamId))
    , PartitionRange_(std::move(partitionRange))
    , BucketCount_(bucketCount)
    , Backend_(std::move(backend))
    , Logger(std::move(logger))
{ }

TFuture<void> TKeyVisitorStore::Init()
{
    return BIND([this, strongThis = MakeStrong(this)] {
        NTables::IKeyVisitorStates::TTableKeyFilter filter;
        filter.ComputationId = ComputationId_;
        filter.StreamId = StreamId_;
        filter.LowerKey = PartitionRange_.Lower;
        filter.UpperKey = PartitionRange_.Upper;
        auto rows = WaitFor(Backend_->ReadAll(filter)).ValueOrThrow();

        // Each closed interval is persisted twice — under {Lower, true} and
        // {Upper, false}. Open-bounded intervals lose the sentinel-side row.
        // tableKey.Key is guaranteed non-sentinel because Write skips them.
        // Rows whose parsed range falls outside the partition (pre-split
        // leftovers) are emplaced anyway: the next Sync's diff will see no
        // matching entry in newRemote and emit a delete for them.
        for (const auto& [tableKey, value] : rows) {
            TKeyVisitorIntervalPtr parsed;
            try {
                parsed = ConvertTo<TKeyVisitorIntervalPtr>(value);
            } catch (const std::exception& ex) {
                YT_TLOG_WARNING("Failed to parse persisted key_visitor interval row, skipping")
                    .With("Key", tableKey.Key)
                    .With("IsLower", tableKey.IsLower)
                    .With(ex);
                continue;
            }
            RemoteIntervals_.emplace(std::pair(tableKey.Key, tableKey.IsLower), std::move(parsed));
        }

        bool persistedPassFinal = false;
        for (const auto& [_, interval] : RemoteIntervals_) {
            if (interval->FinalPass) {
                persistedPassFinal = true;
                break;
            }
        }

        auto bucketRanges = SplitPartitionRangeIntoBuckets(PartitionRange_, BucketCount_);
        Buckets_.reserve(bucketRanges.size());
        for (auto& range : bucketRanges) {
            TBucket bucket;
            bucket.Range = range;
            bucket.Reset(persistedPassFinal);
            Buckets_.push_back(std::move(bucket));
        }

        // Seed bucket geometry from the persisted snapshot. SeedCommitted is
        // idempotent on the same range (the two entries of a closed interval
        // collapse to one call) and carries the persisted PassStartedAt so the
        // schedule-lag anchor survives across restarts.
        for (const auto& [_, interval] : RemoteIntervals_) {
            int index = FindBucketContaining(interval->Lower);
            const TKeyRange intervalRange{.Lower = interval->Lower, .Upper = interval->Upper};
            while (index < std::ssize(Buckets_) && Buckets_[index].Range.Lower < intervalRange.Upper) {
                auto& bucket = Buckets_[index];
                TKeyRange slice{
                    .Lower = std::max(bucket.Range.Lower, intervalRange.Lower),
                    .Upper = std::min(bucket.Range.Upper, intervalRange.Upper),
                };
                bucket.SeedCommitted(slice, interval->PassStartedAt, interval->FinalPass);
                ++index;
            }
        }
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

int TKeyVisitorStore::FindBucketContaining(const TKey& key) const
{
    auto it = std::upper_bound(
        Buckets_.begin(),
        Buckets_.end(),
        key,
        [] (const TKey& key, const TBucket& bucket) {
            return key < bucket.Range.Upper;
        });
    return std::distance(Buckets_.begin(), it);
}

void TKeyVisitorStore::StampAcrossBuckets(const TKeyRange& range, EIntervalState state, TSystemTimestamp when)
{
    int index = FindBucketContaining(range.Lower);
    while (index < std::ssize(Buckets_) && Buckets_[index].Range.Lower < range.Upper) {
        auto& bucket = Buckets_[index];
        TKeyRange slice{
            .Lower = std::max(bucket.Range.Lower, range.Lower),
            .Upper = std::min(bucket.Range.Upper, range.Upper),
        };
        if (state == EIntervalState::Committed) {
            bucket.MarkCommitted(slice);
        } else {
            bucket.MarkBuffered(slice, when);
        }
        ++index;
    }
}

void TKeyVisitorStore::MarkBuffered(const TKeyRange& range, TSystemTimestamp when)
{
    StampAcrossBuckets(range, EIntervalState::Buffered, when);
}

void TKeyVisitorStore::MarkCommitted(const TKeyRange& range)
{
    StampAcrossBuckets(range, EIntervalState::Committed, TSystemTimestamp());
}

std::optional<TKeyRange> TKeyVisitorStore::GetNextRange()
{
    if (Buckets_.empty()) {
        return std::nullopt;
    }
    const int total = std::ssize(Buckets_);
    for (int probed = 0; probed < total; ++probed) {
        const int candidate = (NextBucketIndex_ + probed) % total;
        const auto& bucket = Buckets_[candidate];
        for (const auto& slot : bucket.Intervals) {
            if (slot.State == EIntervalState::Pending) {
                NextBucketIndex_ = (candidate + 1) % total;
                return TKeyRange{.Lower = slot.Interval->Lower, .Upper = slot.Interval->Upper};
            }
        }
    }
    return std::nullopt;
}

bool TKeyVisitorStore::IsAllCommitted() const
{
    for (const auto& bucket : Buckets_) {
        for (const auto& slot : bucket.Intervals) {
            if (slot.State != EIntervalState::Committed) {
                return false;
            }
        }
    }
    return true;
}

void TKeyVisitorStore::Reset()
{
    for (auto& bucket : Buckets_) {
        bucket.Reset(/*finalPass*/ false);
    }
    NextBucketIndex_ = 0;
}

void TKeyVisitorStore::StartNewPass(bool finalPass)
{
    for (auto& bucket : Buckets_) {
        bucket.Reset(finalPass);
    }
    NextBucketIndex_ = 0;
}

bool TKeyVisitorStore::IsCurrentPassFinal() const
{
    if (Buckets_.empty() || Buckets_.front().Intervals.empty()) {
        return false;
    }
    return Buckets_.front().Intervals.front().Interval->FinalPass;
}

const std::vector<TKeyVisitorStore::TBucket>& TKeyVisitorStore::GetBuckets() const
{
    return Buckets_;
}

bool TKeyVisitorStore::IsRemoteStateEmpty() const
{
    return RemoteIntervals_.empty();
}

double TKeyVisitorStore::GetScannedFraction() const
{
    double total = 0;
    double scanned = 0;
    for (const auto& bucket : Buckets_) {
        for (const auto& slot : bucket.Intervals) {
            const auto lower = ExtractFirstUintFromKey(slot.Interval->Lower)
                .value_or(std::numeric_limits<ui64>::min());
            const auto upper = ExtractFirstUintFromKey(slot.Interval->Upper)
                .value_or(std::numeric_limits<ui64>::max());
            YT_VERIFY(upper >= lower);
            const double length = static_cast<double>(upper - lower);
            total += length;
            if (slot.State != EIntervalState::Pending) {
                scanned += length;
            }
        }
    }
    // Degenerate (non-uint / empty) range: nothing to scan, treat as done.
    return total > 0 ? scanned / total : 1.0;
}

std::optional<TSystemTimestamp> TKeyVisitorStore::GetMinPassStartedAt() const
{
    std::optional<TSystemTimestamp> result;
    for (const auto& bucket : Buckets_) {
        for (const auto& slot : bucket.Intervals) {
            if (slot.State == EIntervalState::Pending) {
                continue;
            }
            const auto stamp = slot.Interval->PassStartedAt;
            if (!result || stamp < *result) {
                result = stamp;
            }
        }
    }
    return result;
}

void TKeyVisitorStore::Sync(NApi::IDynamicTableTransactionPtr transaction)
{
    // Build the desired remote snapshot: each Processed interval contributes
    // two rows — {Lower, true} and {Upper, false}. Sentinel boundaries are
    // skipped (they are not storable as `any` sort-keys). No cross-bucket
    // merging: each bucket's intervals are persisted independently.
    THashMap<std::pair<TKey, bool>, TKeyVisitorIntervalPtr> newRemote;
    for (const auto& bucket : Buckets_) {
        for (const auto& slot : bucket.Intervals) {
            if (slot.State != EIntervalState::Committed) {
                continue;
            }
            const auto& interval = slot.Interval;
            if (interval->Lower != MinKey()) {
                newRemote.emplace(std::pair(interval->Lower, true), interval);
            }
            if (interval->Upper != MaxKey()) {
                newRemote.emplace(std::pair(interval->Upper, false), interval);
            }
        }
    }

    std::vector<std::pair<NTables::IKeyVisitorStates::TTableKey, std::optional<NTables::IKeyVisitorStates::TValue>>> mutations;

    auto makeTableKey = [&] (const std::pair<TKey, bool>& slot) {
        return NTables::IKeyVisitorStates::TTableKey{
            .ComputationId = ComputationId_,
            .StreamId = StreamId_,
            .Key = slot.first,
            .IsLower = slot.second,
        };
    };

    for (const auto& [slot, old] : RemoteIntervals_) {
        auto it = newRemote.find(slot);
        if (it == newRemote.end() || !(*it->second == *old)) {
            mutations.push_back({makeTableKey(slot), std::nullopt});
        }
    }
    for (const auto& [slot, fresh] : newRemote) {
        auto it = RemoteIntervals_.find(slot);
        if (it == RemoteIntervals_.end() || !(*it->second == *fresh)) {
            mutations.push_back({makeTableKey(slot), ConvertToYsonString(fresh)});
        }
    }

    RemoteIntervals_ = std::move(newRemote);

    if (mutations.empty()) {
        return;
    }
    Backend_->Write(transaction, mutations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
