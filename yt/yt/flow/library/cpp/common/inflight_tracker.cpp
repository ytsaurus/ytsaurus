#include "inflight_tracker.h"

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/heap.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// A lightweight non-owning view over a vector that adds heap operations.
// TCompare(left, right)       — returns true if left should be closer to the top.
// TOnAssign(element, index)   — called whenever an element is placed at a new index.
template <class T, class TCompare, class TOnAssign>
class TTrackerHeapView
{
public:
    TTrackerHeapView(std::vector<T>& data, TCompare compare, TOnAssign onAssign)
        : Data_(data)
        , Compare_(std::move(compare))
        , OnAssign_(std::move(onAssign))
    { }

    void Push(T value)
    {
        Data_.push_back(std::move(value));
        // Set the initial index before sifting, because AdjustHeapBack -> SiftUp
        // may not call onAssign for the final position in optimized builds when
        // no movement occurs (e.g. 1-element heap or element already in place).
        IndexOnAssign()(Data_.size() - 1);
        AdjustHeapBack(Data_.begin(), Data_.end(), Compare_, IndexOnAssign());
    }

    void ExtractTop()
    {
        ExtractHeap(Data_.begin(), Data_.end(), Compare_, IndexOnAssign());
        Data_.pop_back();
    }

    void ExtractAt(size_t index)
    {
        ExtractHeap(Data_.begin(), Data_.end(), Data_.begin() + index, Compare_, IndexOnAssign());
        Data_.pop_back();
    }

    T& Top()
    {
        return Data_[0];
    }

    size_t Size() const
    {
        return Data_.size();
    }

    bool Empty() const
    {
        return Data_.empty();
    }

private:
    std::vector<T>& Data_;
    TCompare Compare_;
    TOnAssign OnAssign_;

    auto IndexOnAssign()
    {
        return [this] (size_t i) {
            OnAssign_(Data_[i], i);
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TInflightTracker::TInflightTracker(
    const NProfiling::TProfiler& profiler,
    TWatermarkPercentileSpecPtr percentile,
    TStreamLimitUsageStatePtr limitUsageState)
    : Percentile_(std::move(percentile))
    , LimitUsageState_(std::move(limitUsageState))
    , SkipPercentileHeaps_(Percentile_->Value.Underlying() == PreciseWatermarkPercentile.Underlying())
    , Profiler_(profiler)
    , RegisteredCountCounter_(Profiler_.Counter("/registered_count"))
    , RegisteredBytesCounter_(Profiler_.Counter("/registered_bytes"))
    , UnregisteredCountCounter_(Profiler_.Counter("/unregistered_count"))
    , UnregisteredBytesCounter_(Profiler_.Counter("/unregistered_bytes"))
{ }

////////////////////////////////////////////////////////////////////////////////

auto TInflightTracker::GetSystemTimestampHeap()
{
    return TTrackerHeapView(SystemTimestampHeap_, [] (const TMapIterator& l, const TMapIterator& r) {
        return l->second.SystemTimestamp < r->second.SystemTimestamp;
    },
        [] (const TMapIterator& x, size_t i) {
            x->second.SystemHeapIndex = i;
        });
}

auto TInflightTracker::GetMinEventTimestampHeap()
{
    return TTrackerHeapView(MinEventTimestampHeap_, [] (const TMapIterator& l, const TMapIterator& r) {
        return l->second.EventTimestamp < r->second.EventTimestamp;
    },
        [] (const TMapIterator& x, size_t i) {
            x->second.MinEventHeapIndex = i;
        });
}

template <TInflightTracker::EPercentileHeap Heap>
auto TInflightTracker::GetEventTimestampPercentileHeap()
{
    return TTrackerHeapView(EventTimestampPercetilesHeaps_[Heap], [] (const TMapIterator& l, const TMapIterator& r) {
        if constexpr (Heap == Lower) {
            return l->second.EventTimestamp > r->second.EventTimestamp; // max-heap
        } else {
            return l->second.EventTimestamp < r->second.EventTimestamp; // min-heap
        }
    },
        [] (const TMapIterator& x, size_t i) {
            x->second.EventHeapIndex = i;
        });
}

////////////////////////////////////////////////////////////////////////////////

template <TInflightTracker::EPercentileHeap From>
void TInflightTracker::MoveAcross()
{
    constexpr TInflightTracker::EPercentileHeap To = From == Lower ? Upper : Lower;
    auto it = GetEventTimestampPercentileHeap<From>().Top();
    GetEventTimestampPercentileHeap<From>().ExtractTop();
    it->second.PercentileHeap = To;
    GetEventTimestampPercentileHeap<To>().Push(it);
}

void TInflightTracker::RebalanceEventHeaps(size_t lowerSize)
{
    while (GetEventTimestampPercentileHeap<Lower>().Size() > lowerSize) {
        MoveAcross</*From*/ Lower>();
    }
    while (GetEventTimestampPercentileHeap<Lower>().Size() < lowerSize && !GetEventTimestampPercentileHeap<Upper>().Empty()) {
        MoveAcross</*From*/ Upper>();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TInflightTracker::FinalizeRegister(
    TMapIterator it,
    i64 byteSize,
    TSystemTimestamp systemTimestamp,
    TSystemTimestamp eventTimestamp)
{
    auto& state = it->second;
    state.Size = byteSize;
    state.SystemTimestamp = systemTimestamp;
    state.EventTimestamp = eventTimestamp;

    GetSystemTimestampHeap().Push(it);
    GetMinEventTimestampHeap().Push(it);

    if (!SkipPercentileHeaps_) {
        // If Lower heap is empty or the new element is >= Lower top (max of lower group),
        // it belongs in Upper. Otherwise it belongs in Lower.
        if (EventTimestampPercetilesHeaps_[Lower].empty() || eventTimestamp >= EventTimestampPercetilesHeaps_[Lower][0]->second.EventTimestamp) {
            state.PercentileHeap = Upper;
            GetEventTimestampPercentileHeap<Upper>().Push(it);
        } else {
            state.PercentileHeap = Lower;
            GetEventTimestampPercentileHeap<Lower>().Push(it);
        }
    }

    ByteSize_ += byteSize;
    RegisteredCountTotal_ += 1;
    RegisteredByteSizeTotal_ += byteSize;
    PendingCounters_.RegisteredCount += 1;
    PendingCounters_.RegisteredBytes += byteSize;
}

void TInflightTracker::FinalizeUnregister(TMapIterator it)
{
    const auto& state = it->second;
    const auto byteSize = state.Size;

    GetSystemTimestampHeap().ExtractAt(state.SystemHeapIndex);
    GetMinEventTimestampHeap().ExtractAt(state.MinEventHeapIndex);
    if (!SkipPercentileHeaps_) {
        if (state.PercentileHeap == Lower) {
            GetEventTimestampPercentileHeap<Lower>().ExtractAt(state.EventHeapIndex);
        } else {
            GetEventTimestampPercentileHeap<Upper>().ExtractAt(state.EventHeapIndex);
        }
    }

    InflightMessages_.erase(it);
    ByteSize_ -= byteSize;
    PendingCounters_.UnregisteredCount += 1;
    PendingCounters_.UnregisteredBytes += byteSize;
}

void TInflightTracker::SyncCounters()
{
    RegisteredCount_.Inc(PendingCounters_.RegisteredCount);
    RegisteredBytes_.Inc(PendingCounters_.RegisteredBytes);
    RegisteredCountCounter_.Increment(PendingCounters_.RegisteredCount);
    RegisteredBytesCounter_.Increment(PendingCounters_.RegisteredBytes);
    UnregisteredCount_.Inc(PendingCounters_.UnregisteredCount);
    UnregisteredBytes_.Inc(PendingCounters_.UnregisteredBytes);
    UnregisteredCountCounter_.Increment(PendingCounters_.UnregisteredCount);
    UnregisteredBytesCounter_.Increment(PendingCounters_.UnregisteredBytes);
    PendingCounters_ = {};
    if (LimitUsageState_) {
        LimitUsageState_->Update(TStreamUsage{
            .CumulativeByteIn = RegisteredByteSizeTotal_,
            .CumulativeByteOut = RegisteredByteSizeTotal_ - ByteSize_,
            .CumulativeCountIn = RegisteredCountTotal_,
            .CumulativeCountOut = RegisteredCountTotal_ - std::ssize(InflightMessages_),
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TSystemTimestamp> TInflightTracker::GetMinSystemTimestamp()
{
    auto heap = GetSystemTimestampHeap();
    if (heap.Empty()) {
        return std::nullopt;
    }
    return heap.Top()->second.SystemTimestamp;
}

std::optional<TSystemTimestamp> TInflightTracker::GetMinEventTimestamp()
{
    size_t n = InflightMessages_.size();
    if (n == 0) {
        return std::nullopt;
    }

    if (SkipPercentileHeaps_) {
        // Percentile == 100: the lower-group is always empty, the upper-group always holds
        // every entry, and its top is the min event timestamp — same as MinEventTimestampHeap.
        // Delay is still applied via max(min - delay, 0) but for percentile==100 the result
        // simplifies to the raw min (since min - delay ≤ min).
        return GetMinEventTimestampHeap().Top()->second.EventTimestamp;
    }

    // lowerSize = floor(N * (100 - percentile) / 100).
    // When percentile == 0 (IgnoreInflight), lowerSize == N → return nullopt.
    size_t lowerSize = n * (PreciseWatermarkPercentile.Underlying() - Percentile_->Value.Underlying()) /
        PreciseWatermarkPercentile.Underlying();

    if (lowerSize == n) {
        return std::nullopt;
    }

    RebalanceEventHeaps(lowerSize);

    auto upperHeap = GetEventTimestampPercentileHeap<Upper>();
    YT_VERIFY(!upperHeap.Empty());
    const auto rawPercentileTimestamp = upperHeap.Top()->second.EventTimestamp;

    const auto percentileTimestamp = TSystemTimestamp(
        std::max<ui64>(rawPercentileTimestamp.Underlying(), Percentile_->Delay.Seconds()) - Percentile_->Delay.Seconds());

    const auto minEventTimestamp = GetMinEventTimestampHeap().Top()->second.EventTimestamp;

    return std::max(minEventTimestamp, percentileTimestamp);
}

i64 TInflightTracker::GetCount() const
{
    return std::ssize(InflightMessages_);
}

i64 TInflightTracker::GetByteSize() const
{
    return ByteSize_;
}

bool TInflightTracker::Contains(const TMessageId& messageId) const
{
    return InflightMessages_.contains(messageId);
}

void TInflightTracker::Register(const TOutputMessageConstPtr& message)
{
    auto [it, success] = InflightMessages_.try_emplace(message->MessageId);
    YT_VERIFY(success);
    FinalizeRegister(it, message->ByteSize, message->SystemTimestamp, message->EventTimestamp);
}

bool TInflightTracker::TryRegister(const TOutputMessageConstPtr& message)
{
    auto [it, success] = InflightMessages_.try_emplace(message->MessageId);
    if (!success) {
        return false;
    }
    FinalizeRegister(it, message->ByteSize, message->SystemTimestamp, message->EventTimestamp);
    return true;
}

void TInflightTracker::Register(const TInputTimerConstPtr& timer)
{
    auto [it, success] = InflightMessages_.try_emplace(timer->MessageId);
    YT_VERIFY(success);
    FinalizeRegister(it, timer->ByteSize, timer->SystemTimestamp, timer->EventTimestamp);
}

bool TInflightTracker::TryRegister(const TInputTimerConstPtr& timer)
{
    auto [it, success] = InflightMessages_.try_emplace(timer->MessageId);
    if (!success) {
        return false;
    }
    FinalizeRegister(it, timer->ByteSize, timer->SystemTimestamp, timer->EventTimestamp);
    return true;
}

void TInflightTracker::Unregister(const TMessageId& messageId)
{
    FinalizeUnregister(GetIteratorOrCrash(InflightMessages_, messageId));
}

bool TInflightTracker::TryUnregister(const TMessageId& messageId)
{
    auto it = InflightMessages_.find(messageId);
    if (it == InflightMessages_.end()) {
        return false;
    }
    FinalizeUnregister(it);
    return true;
}

TInflightStreamTraverseDataPtr TInflightTracker::BuildInflight()
{
    auto inflight = New<TInflightStreamTraverseData>();

    inflight->MinSystemTimestamp = GetMinSystemTimestamp();
    inflight->MinEventTimestamp = GetMinEventTimestamp();

    inflight->InflightMetrics->Count = GetCount();
    inflight->InflightMetrics->ByteSize = GetByteSize();
    inflight->InflightMetrics->NewCountPerSec = RegisteredCount_.GetRate().value_or(0);
    inflight->InflightMetrics->NewBytesPerSec = RegisteredBytes_.GetRate().value_or(0);
    inflight->InflightMetrics->ProcessedCountPerSec = UnregisteredCount_.GetRate().value_or(0);
    inflight->InflightMetrics->ProcessedBytesPerSec = UnregisteredBytes_.GetRate().value_or(0);

    inflight->Empty = inflight->InflightMetrics->Count == 0;
    inflight->Suspended = false;

    return inflight;
}

////////////////////////////////////////////////////////////////////////////////

TMultiInflightTracker::TMultiInflightTracker(
    const NProfiling::TProfiler& profiler,
    const THashSet<TStreamId>& streams,
    TWatermarkPercentileSpecPtr percentile,
    const TStreamLimitUsageStateMap& streamLimitUsageStates)
    : InflightTrackers_(CreateInflightTrackers(profiler, streams, percentile, streamLimitUsageStates))
{
    if (InflightTrackers_.size() <= MaxSmallTrackerCount) {
        SmallTrackers_.reserve(InflightTrackers_.size());
        for (const auto& [streamId, tracker] : InflightTrackers_) {
            SmallTrackers_.emplace_back(streamId, tracker.Get());
        }
    }
}

TMultiInflightTracker::TMultiInflightTracker(
    const NProfiling::TProfiler& profiler,
    const std::vector<TStreamId>& streams,
    TWatermarkPercentileSpecPtr percentile,
    const TStreamLimitUsageStateMap& streamLimitUsageStates)
    : TMultiInflightTracker(profiler, THashSet<TStreamId>(streams.begin(), streams.end()), percentile, streamLimitUsageStates)
{ }

THashMap<TStreamId, TInflightTrackerPtr> TMultiInflightTracker::CreateInflightTrackers(
    const NProfiling::TProfiler& profiler,
    const THashSet<TStreamId>& streams,
    TWatermarkPercentileSpecPtr percentile,
    const TStreamLimitUsageStateMap& streamLimitUsageStates)
{
    THashMap<TStreamId, TInflightTrackerPtr> inflightTrackers;
    for (const auto& streamId : streams) {
        TStreamLimitUsageStatePtr state;
        if (auto it = streamLimitUsageStates.find(streamId); it != streamLimitUsageStates.end()) {
            state = it->second;
        }
        inflightTrackers[streamId] = New<TInflightTracker>(
            profiler.WithTag("stream_id", streamId.Underlying()),
            percentile,
            std::move(state));
    }
    return inflightTrackers;
}

TInflightTracker* TMultiInflightTracker::ResolveTracker(const TStreamId& streamId) const
{
    if (!SmallTrackers_.empty()) {
        for (const auto& [id, tracker] : SmallTrackers_) {
            if (id == streamId) {
                return tracker;
            }
        }
    }
    return InflightTrackers_.at(streamId).Get();
}

void TMultiInflightTracker::Register(const TOutputMessageConstPtr& message)
{
    ResolveTracker(message->StreamId)->Register(message);
}

bool TMultiInflightTracker::TryRegister(const TOutputMessageConstPtr& message)
{
    return ResolveTracker(message->StreamId)->TryRegister(message);
}

bool TMultiInflightTracker::Contains(const TMessageMeta& message) const
{
    return ResolveTracker(message.StreamId)->Contains(message.MessageId);
}

void TMultiInflightTracker::Unregister(const TMessageMeta& message)
{
    ResolveTracker(message.StreamId)->Unregister(message.MessageId);
}

bool TMultiInflightTracker::TryUnregister(const TMessageMeta& message)
{
    return ResolveTracker(message.StreamId)->TryUnregister(message.MessageId);
}

void TMultiInflightTracker::Register(const TInputTimerConstPtr& timer)
{
    ResolveTracker(timer->StreamId)->Register(timer);
}

bool TMultiInflightTracker::TryRegister(const TInputTimerConstPtr& timer)
{
    return ResolveTracker(timer->StreamId)->TryRegister(timer);
}

void TMultiInflightTracker::SyncCounters()
{
    for (const auto& [_, tracker] : InflightTrackers_) {
        tracker->SyncCounters();
    }
}

THashMap<TStreamId, TInflightStreamTraverseDataPtr> TMultiInflightTracker::BuildInflights()
{
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> inflights;
    for (const auto& [streamId, tracker] : InflightTrackers_) {
        inflights[streamId] = tracker->BuildInflight();
    }
    return inflights;
}

THashMap<TStreamId, std::pair<i64, i64>> TMultiInflightTracker::GetCountAndByteSizes() const
{
    THashMap<TStreamId, std::pair<i64, i64>> result(InflightTrackers_.size());
    for (const auto& [streamId, tracker] : InflightTrackers_) {
        result[streamId] = {tracker->GetCount(), tracker->GetByteSize()};
    }
    return result;
}

i64 TMultiInflightTracker::GetTotalCount() const
{
    i64 count = 0;
    for (const auto& [_, tracker] : InflightTrackers_) {
        count += tracker->GetCount();
    }
    return count;
}

i64 TMultiInflightTracker::GetTotalByteSize() const
{
    i64 byteSize = 0;
    for (const auto& [_, tracker] : InflightTrackers_) {
        byteSize += tracker->GetByteSize();
    }
    return byteSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
