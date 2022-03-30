#include "profile_manager.h"

#include "snapshot.h"
#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NQueueAgent {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

//! Queue-related profiling counters.
struct TQueueProfilingCounters
{
    TGauge Partitions;
    TGauge NonVitalConsumers;
    TGauge VitalConsumers;

    TQueueProfilingCounters(const TProfiler& profiler)
        : Partitions(profiler.Gauge("/partitions"))
        , NonVitalConsumers(profiler.WithTag("vital", "false").Gauge("/consumers"))
        , VitalConsumers(profiler.WithTag("vital", "true").Gauge("/consumers"))
    { }
};

//! Queue-related per-partition profiling counters.
struct TQueuePartitionProfilingCounters
{
    TCounter RowsWritten;
    TCounter RowsTrimmed;

    TQueuePartitionProfilingCounters(const TProfiler& profiler)
        : RowsWritten(profiler.Counter("/rows_written"))
        , RowsTrimmed(profiler.Counter("/rows_trimmed"))
    { }
};

//! Consumer-related per-partition profiling counters.
struct TConsumerPartitionProfilingCounters
{
    TCounter RowsConsumed;
    TGauge LagRows;
    TTimeGauge LagTime;

    TConsumerPartitionProfilingCounters(const TProfiler& profiler)
        : RowsConsumed(profiler.Counter("/rows_consumed"))
        , LagRows(profiler.GaugeSummary("/lag_rows"))
        , LagTime(profiler.TimeGaugeSummary("/lag_time"))
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TQueueProfileManager
    : public IQueueProfileManager
{
public:
    TQueueProfileManager(IInvokerPtr invoker, TProfiler profiler)
        : Invoker_(std::move(invoker))
        , QueueProfiler_(profiler
            .WithPrefix("/queue"))
        , ConsumerProfiler_(profiler
            .WithPrefix("/consumer"))
    { }

    void Profile(
        const TQueueSnapshotPtr& previousQueueSnapshot,
        const TQueueSnapshotPtr& currentQueueSnapshot) override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto partitionCount = currentQueueSnapshot->PartitionCount;
        EnsureCounters(partitionCount, GetKeys(currentQueueSnapshot->ConsumerSnapshots));

        if (!CheckSnapshotCompatibility(previousQueueSnapshot, currentQueueSnapshot)) {
            // Simply wait for the next call when snapshots are compatible.
            // Losing an iteration of profiling is not bad for since profiling is essentially stateless.
            return;
        }

        // We are safe to assume that all consumer refs are same in previous snapshot, current snapshots and
        // consumer profiling counters, and also that all partition-indexed vectors in both snapshots and
        // all counters have the same length.

        QueueProfilingCounters_->Partitions.Update(partitionCount);
        int vitalConsumerCount = 0;
        int nonVitalConsumerCount = 0;
        for (const auto& consumer : GetValues(currentQueueSnapshot->ConsumerSnapshots)) {
            ++(consumer->Vital ? vitalConsumerCount : nonVitalConsumerCount);
        }
        QueueProfilingCounters_->VitalConsumers.Update(vitalConsumerCount);
        QueueProfilingCounters_->NonVitalConsumers.Update(nonVitalConsumerCount);

        // Mind the clamp. We do not want process to crash if some delta turns out to be negative due to some manual action.

        for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            const auto& previousQueuePartitionSnapshot = previousQueueSnapshot->PartitionSnapshots[partitionIndex];
            const auto& currentQueuePartitionSnapshot = currentQueueSnapshot->PartitionSnapshots[partitionIndex];
            auto& profilingCounters = QueuePartitionProfilingCounters_[partitionIndex];
            SafeIncrement(
                profilingCounters.RowsWritten,
                currentQueuePartitionSnapshot->UpperRowIndex - previousQueuePartitionSnapshot->UpperRowIndex);
            SafeIncrement(
                profilingCounters.RowsTrimmed,
                currentQueuePartitionSnapshot->LowerRowIndex - previousQueuePartitionSnapshot->LowerRowIndex);
        }

        for (const auto& consumerRef : GetKeys(currentQueueSnapshot->ConsumerSnapshots)) {
            const auto& previousConsumerPartitionSnapshots = previousQueueSnapshot->ConsumerSnapshots[consumerRef]->PartitionSnapshots;
            const auto& currentConsumerPartitionSnapshots = currentQueueSnapshot->ConsumerSnapshots[consumerRef]->PartitionSnapshots;
            auto& consumerPartitionProfilingCounters = ConsumerPartitionProfilingCounters_[consumerRef];
            for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
                const auto& previousConsumerPartitionSnapshot = previousConsumerPartitionSnapshots[partitionIndex];
                const auto& currentConsumerPartitionSnapshot = currentConsumerPartitionSnapshots[partitionIndex];
                auto& profilingCounters = consumerPartitionProfilingCounters[partitionIndex];

                SafeIncrement(
                    profilingCounters.RowsConsumed,
                    currentConsumerPartitionSnapshot->NextRowIndex - previousConsumerPartitionSnapshot->NextRowIndex);
                profilingCounters.LagRows.Update(currentConsumerPartitionSnapshot->UnreadRowCount);
                profilingCounters.LagTime.Update(currentConsumerPartitionSnapshot->ProcessingLag);
            }
        }
    }

private:
    IInvokerPtr Invoker_;
    TProfiler QueueProfiler_;
    TProfiler ConsumerProfiler_;
    std::optional<TQueueProfilingCounters> QueueProfilingCounters_;
    std::vector<TQueuePartitionProfilingCounters> QueuePartitionProfilingCounters_;
    THashMap<TCrossClusterReference, std::vector<TConsumerPartitionProfilingCounters>> ConsumerPartitionProfilingCounters_;

    //! Helper for incrementing a counter only if delta is non-negative.
    void SafeIncrement(TCounter& counter, i64 delta)
    {
        if (delta >= 0) {
            counter.Increment(delta);
        }
    }

    //! Check if two snapshots are structurally similar (i.e. have same number of partitions and same set of consumers).
    bool CheckSnapshotCompatibility(
        const TQueueSnapshotPtr& previousQueueSnapshot,
        const TQueueSnapshotPtr& currentQueueSnapshot)
    {
        if (previousQueueSnapshot->PartitionCount != currentQueueSnapshot->PartitionCount) {
            return false;
        }

        auto previousConsumerRefs = GetKeys(previousQueueSnapshot->ConsumerSnapshots);
        auto currentConsumerRefs = GetKeys(currentQueueSnapshot->ConsumerSnapshots);
        std::sort(previousConsumerRefs.begin(), previousConsumerRefs.end());
        std::sort(currentConsumerRefs.begin(), currentConsumerRefs.end());

        if (previousConsumerRefs != currentConsumerRefs) {
            return false;
        }

        return true;
    }

    //! Ensures the existence of all levels.
    void EnsureCounters(int partitionCount, std::vector<NYT::NQueueAgent::TCrossClusterReference> consumerRefs)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!QueueProfilingCounters_) {
            QueueProfilingCounters_.emplace(QueueProfiler_);
        }

        auto resizeCounters = [&] (auto& counters, const TProfiler& profiler) {
            if (counters.size() > static_cast<size_t>(partitionCount)) {
                counters.erase(counters.begin() + partitionCount, counters.end());
            } else {
                for (int partitionIndex = counters.size(); partitionIndex < partitionCount; ++partitionIndex) {
                    counters.emplace_back(profiler
                        .WithTag("partition_index", ToString(partitionIndex)));
                }
            }
        };

        resizeCounters(QueuePartitionProfilingCounters_, QueueProfiler_);

        // Steal old counter vectors for the current list of consumer references.
        {
            THashMap<TCrossClusterReference, std::vector<TConsumerPartitionProfilingCounters>> oldConsumerPartitionProfilingCounters;
            oldConsumerPartitionProfilingCounters.swap(ConsumerPartitionProfilingCounters_);
            for (const auto& consumerRef : consumerRefs) {
                ConsumerPartitionProfilingCounters_[consumerRef].swap(oldConsumerPartitionProfilingCounters[consumerRef]);
            }
        }

        for (auto& [consumerRef, consumerPartitionProfilingCounters] : ConsumerPartitionProfilingCounters_) {
            auto consumerProfiler = ConsumerProfiler_
                .WithTag("consumer_path", consumerRef.Path)
                .WithTag("consumer_cluster", consumerRef.Cluster);
            resizeCounters(consumerPartitionProfilingCounters, consumerProfiler);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TQueueProfileManager);

////////////////////////////////////////////////////////////////////////////////

IQueueProfileManagerPtr CreateQueueProfileManager(IInvokerPtr invoker, const TProfiler& profiler)
{
    return New<TQueueProfileManager>(std::move(invoker), profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
