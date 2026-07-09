#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/stream_inflight_limits.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/flow/library/cpp/misc/counter.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// Tracks inflight messages and timers for a single stream.
// Maintains min system timestamp, min event timestamp, and percentile event timestamp needed to compute watermarks.
class TInflightTracker
    : public TRefCounted
{
public:
    TInflightTracker(
        const NProfiling::TProfiler& profiler,
        TWatermarkPercentileSpecPtr percentile,
        TStreamLimitUsageStatePtr limitUsageState = nullptr);

    bool Contains(const TMessageId& messageId) const;

    void Register(const TOutputMessageConstPtr& message);
    bool TryRegister(const TOutputMessageConstPtr& message);
    void Register(const TInputTimerConstPtr& timer);
    bool TryRegister(const TInputTimerConstPtr& timer);

    void Unregister(const TMessageId& messageId);
    bool TryUnregister(const TMessageId& messageId);

    // Flushes accumulated counter deltas batched by Register/Unregister into the profiling
    // counters. If a stream-limit-usage-state was supplied to the constructor, also
    // publishes the cumulative usage to it. Caller invokes once after a batch of operations.
    void SyncCounters();

    std::optional<TSystemTimestamp> GetMinSystemTimestamp();
    std::optional<TSystemTimestamp> GetMinEventTimestamp();

    TInflightStreamTraverseDataPtr BuildInflight();

    i64 GetByteSize() const;
    i64 GetCount() const;

private:
    const TWatermarkPercentileSpecPtr Percentile_;
    const TStreamLimitUsageStatePtr LimitUsageState_;
    // When percentile == 100 (the default `PreciseWatermarkPercentile`), the percentile heaps
    // degenerate to the same min-event-timestamp as `MinEventTimestampHeap_`. We skip maintaining
    // them and read from MinEventTimestampHeap_ directly — saves 2 × N × 8 B of vector storage
    // at peak inflight (~1.6 MB at 100k entries). The runtime path inside DoRegister/DoUnregister
    // also drops one Push/Pop and one branch per call.
    const bool SkipPercentileHeaps_;
    const NProfiling::TProfiler Profiler_;

    // Lower = max-heap of bottom elements (below the percentile boundary).
    // Upper = min-heap of top elements; top of Upper is the percentile element.
    enum EPercentileHeap
    {
        Lower = 0,
        Upper = 1
    };

    struct TMessageState
    {
        i64 Size{};
        TSystemTimestamp SystemTimestamp;
        TSystemTimestamp EventTimestamp;
        size_t SystemHeapIndex{};
        size_t MinEventHeapIndex{};
        // Index within EventTimestampPercetilesHeaps_[PercentileHeap].
        // Packed with PercentileHeap into a single 64-bit word.
        ui64 EventHeapIndex : 63 {};
        ui64 PercentileHeap : 1 {};
    };

    using TInflightMap = THashMap<TMessageId, TMessageState>;
    using TMapIterator = TInflightMap::iterator;

    TInflightMap InflightMessages_;

    // Min-heap by SystemTimestamp. Top = global minimum system timestamp.
    std::vector<TMapIterator> SystemTimestampHeap_;
    // Min-heap by EventTimestamp over all elements. Top = global minimum event timestamp.
    std::vector<TMapIterator> MinEventTimestampHeap_;
    // EventTimestampPercetilesHeaps_[Lower]: max-heap by EventTimestamp — bottom elements. Top = largest of the lower group.
    // EventTimestampPercetilesHeaps_[Upper]: min-heap by EventTimestamp — top elements. Top = percentile element.
    std::vector<TMapIterator> EventTimestampPercetilesHeaps_[2];

    i64 ByteSize_ = 0;
    i64 RegisteredCountTotal_ = 0;
    i64 RegisteredByteSizeTotal_ = 0;
    TSimpleEmaCounter RegisteredCount_;
    TSimpleEmaCounter RegisteredBytes_;
    TSimpleEmaCounter UnregisteredCount_;
    TSimpleEmaCounter UnregisteredBytes_;
    NProfiling::TCounter RegisteredCountCounter_;
    NProfiling::TCounter RegisteredBytesCounter_;
    NProfiling::TCounter UnregisteredCountCounter_;
    NProfiling::TCounter UnregisteredBytesCounter_;

    struct TPendingCounters
    {
        i64 RegisteredCount = 0;
        i64 RegisteredBytes = 0;
        i64 UnregisteredCount = 0;
        i64 UnregisteredBytes = 0;
    };

    TPendingCounters PendingCounters_;

    auto GetSystemTimestampHeap();
    auto GetMinEventTimestampHeap();
    template <EPercentileHeap Heap>
    auto GetEventTimestampPercentileHeap();

    // Move the top element of EventTimestampPercetilesHeaps_[From] into the opposite heap.
    template <EPercentileHeap From>
    void MoveAcross();

    // Rebalance so that EventTimestampPercetilesHeaps_[Lower].size() == lowerSize.
    void RebalanceEventHeaps(size_t lowerSize);

    // Finishes registering an entry that try_emplace just inserted at |it|:
    // fills the state, pushes onto every heap, and updates the in/out counters.
    void FinalizeRegister(
        TMapIterator it,
        i64 byteSize,
        TSystemTimestamp systemTimestamp,
        TSystemTimestamp eventTimestamp);

    // Extracts |it| from every heap, erases it, and updates the in/out counters.
    void FinalizeUnregister(TMapIterator it);
};

DEFINE_REFCOUNTED_TYPE(TInflightTracker);

////////////////////////////////////////////////////////////////////////////////

// Tracks inflight messages and timers across multiple streams.
class TMultiInflightTracker
    : public TRefCounted
{
public:
    TMultiInflightTracker(
        const NProfiling::TProfiler& profiler,
        const THashSet<TStreamId>& streams,
        TWatermarkPercentileSpecPtr percentile,
        const TStreamLimitUsageStateMap& streamLimitUsageStates = {});
    TMultiInflightTracker(
        const NProfiling::TProfiler& profiler,
        const std::vector<TStreamId>& streams,
        TWatermarkPercentileSpecPtr percentile,
        const TStreamLimitUsageStateMap& streamLimitUsageStates = {});

    void Register(const TOutputMessageConstPtr& message);
    bool TryRegister(const TOutputMessageConstPtr& message);
    bool Contains(const TMessageMeta& message) const;
    void Unregister(const TMessageMeta& message);
    bool TryUnregister(const TMessageMeta& message);

    void Register(const TInputTimerConstPtr& timer);
    bool TryRegister(const TInputTimerConstPtr& timer);

    void SyncCounters();

    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflights();
    THashMap<TStreamId, std::pair<i64, i64>> GetCountAndByteSizes() const;

    i64 GetTotalCount() const;
    i64 GetTotalByteSize() const;

private:
    // Stream-count threshold below which ResolveTracker linearly scans SmallTrackers_
    // instead of probing InflightTrackers_ — wins for two streams; matches the
    // inline-size template parameter of the SmallTrackers_ compact vector.
    static constexpr int MaxSmallTrackerCount = 2;

    THashMap<TStreamId, TInflightTrackerPtr> InflightTrackers_;

    // Populated in the constructor when InflightTrackers_.size() <= MaxSmallTrackerCount —
    // a linear scan beats a TStreamId hash + THashMap probe for two streams. Empty otherwise,
    // in which case ResolveTracker falls through to InflightTrackers_.at(streamId).
    TCompactVector<std::pair<TStreamId, TInflightTracker*>, MaxSmallTrackerCount> SmallTrackers_;

    TInflightTracker* ResolveTracker(const TStreamId& streamId) const;

    static THashMap<TStreamId, TInflightTrackerPtr> CreateInflightTrackers(
        const NProfiling::TProfiler& profiler,
        const THashSet<TStreamId>& streams,
        TWatermarkPercentileSpecPtr percentile,
        const TStreamLimitUsageStateMap& streamLimitUsageStates);
};

DEFINE_REFCOUNTED_TYPE(TMultiInflightTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
