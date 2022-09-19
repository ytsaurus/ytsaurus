#include "profile_manager.h"

#include "snapshot.h"

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/solomon/sensor.h>

namespace NYT::NQueueAgent {

using namespace NProfiling;
using namespace NQueueClient;

////////////////////////////////////////////////////////////////////////////////

//! Queue-related profiling counters.
struct TQueueProfilingCounters
{
    TGauge Partitions;
    TGauge NonVitalConsumers;
    TGauge VitalConsumers;

    explicit TQueueProfilingCounters(const TProfiler& profiler)
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
    TCounter DataWeightWritten;

    TQueuePartitionProfilingCounters(const TProfiler& profiler, const TProfiler& aggregationProfiler)
        : RowsWritten(profiler.Counter("/rows_written"))
        , RowsTrimmed(profiler.Counter("/rows_trimmed"))
        , DataWeightWritten(profiler.Counter("/data_weight_written"))
    {
        Y_UNUSED(aggregationProfiler);
    }
};

struct TConsumerProfilingCounters
{
    // This class may be extended in the future.

    explicit TConsumerProfilingCounters(const TProfiler& profiler)
    {
        Y_UNUSED(profiler);
    }
};

//! Consumer-related per-partition profiling counters.
struct TConsumerPartitionProfilingCounters
{
    TCounter RowsConsumed;
    TCounter DataWeightConsumed;
    TGauge LagRows;
    TGauge LagDataWeight;
    TTimeGauge LagTime;
    TGaugeHistogram LagTimeHistogram;

    TConsumerPartitionProfilingCounters(const TProfiler& profiler, const TProfiler& aggregationProfiler)
        : RowsConsumed(profiler.Counter("/rows_consumed"))
        , DataWeightConsumed(profiler.Counter("/data_weight_consumed"))
        , LagRows(profiler.GaugeSummary("/lag_rows"))
        , LagDataWeight(profiler.GaugeSummary("/lag_data_weight"))
        , LagTime(profiler.TimeGaugeSummary("/lag_time"))
        , LagTimeHistogram(aggregationProfiler.GaugeHistogram("/lag_time_histogram", GenerateGenericBucketBounds()))
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
        , QueuePartitionProfiler_(profiler
            .WithPrefix("/queue_partition"))
        , ConsumerProfiler_(profiler
            .WithPrefix("/consumer"))
        , ConsumerPartitionProfiler_(profiler
            .WithPrefix("/consumer_partition"))
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

            auto rowsWritten = currentQueuePartitionSnapshot->UpperRowIndex - previousQueuePartitionSnapshot->UpperRowIndex;
            SafeIncrement(profilingCounters.RowsWritten, rowsWritten);

            auto rowsTrimmed = currentQueuePartitionSnapshot->LowerRowIndex - previousQueuePartitionSnapshot->LowerRowIndex;
            SafeIncrement(profilingCounters.RowsTrimmed, rowsTrimmed);

            auto dataWeightWriten = currentQueuePartitionSnapshot->CumulativeDataWeight - previousQueuePartitionSnapshot->CumulativeDataWeight;
            SafeIncrement(profilingCounters.DataWeightWritten, dataWeightWriten);
        }

        for (const auto& consumerRef : GetKeys(currentQueueSnapshot->ConsumerSnapshots)) {
            const auto& previousConsumerPartitionSnapshots = previousQueueSnapshot->ConsumerSnapshots[consumerRef]->PartitionSnapshots;
            Y_UNUSED(previousConsumerPartitionSnapshots);
            const auto& currentConsumerPartitionSnapshots = currentQueueSnapshot->ConsumerSnapshots[consumerRef]->PartitionSnapshots;
            auto& consumerProfilingCounters = *ConsumerProfilingCounters_[consumerRef];
            Y_UNUSED(consumerProfilingCounters);
            auto& consumerPartitionProfilingCounters = ConsumerPartitionProfilingCounters_[consumerRef];

            for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
                const auto& previousConsumerPartitionSnapshot = previousConsumerPartitionSnapshots[partitionIndex];
                const auto& currentConsumerPartitionSnapshot = currentConsumerPartitionSnapshots[partitionIndex];
                auto& profilingCounters = consumerPartitionProfilingCounters[partitionIndex];

                auto rowsConsumed = currentConsumerPartitionSnapshot->NextRowIndex - previousConsumerPartitionSnapshot->NextRowIndex;
                SafeIncrement(profilingCounters.RowsConsumed, rowsConsumed);

                auto dataWeightConsumed = currentConsumerPartitionSnapshot->CumulativeDataWeight - previousConsumerPartitionSnapshot->CumulativeDataWeight;
                SafeIncrement(profilingCounters.DataWeightConsumed, dataWeightConsumed);

                profilingCounters.LagRows.Update(currentConsumerPartitionSnapshot->UnreadRowCount);
                profilingCounters.LagDataWeight.Update(currentConsumerPartitionSnapshot->UnreadDataWeight);
                profilingCounters.LagTime.Update(currentConsumerPartitionSnapshot->ProcessingLag);
                profilingCounters.LagTimeHistogram.Reset();
                profilingCounters.LagTimeHistogram.Add(currentConsumerPartitionSnapshot->ProcessingLag.MillisecondsFloat());
            }
        }
    }

private:
    IInvokerPtr Invoker_;
    TProfiler QueueProfiler_;
    TProfiler QueuePartitionProfiler_;
    TProfiler ConsumerProfiler_;
    TProfiler ConsumerPartitionProfiler_;
    std::unique_ptr<TQueueProfilingCounters> QueueProfilingCounters_;
    std::vector<TQueuePartitionProfilingCounters> QueuePartitionProfilingCounters_;
    THashMap<TCrossClusterReference, std::unique_ptr<TConsumerProfilingCounters>> ConsumerProfilingCounters_;
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
            QueueProfilingCounters_ = std::make_unique<TQueueProfilingCounters>(QueueProfiler_);
        }

        auto resizePartitionCounters = [&] (auto& counters, const TProfiler& profiler) {
            if (counters.size() > static_cast<size_t>(partitionCount)) {
                counters.erase(counters.begin() + partitionCount, counters.end());
            } else {
                for (int partitionIndex = counters.size(); partitionIndex < partitionCount; ++partitionIndex) {
                    const auto& partitionProfiler = profiler
                        .WithTag("partition_index", ToString(partitionIndex));
                    const auto& partitionAggregationProfiler = profiler
                        .WithExcludedTag("partition_index", ToString(partitionIndex));
                    counters.emplace_back(partitionProfiler, partitionAggregationProfiler);
                }
            }
        };

        resizePartitionCounters(QueuePartitionProfilingCounters_, QueuePartitionProfiler_);

        // Steal old counters and counter vectors for the current list of consumer references.
        {
            THashMap<TCrossClusterReference, std::unique_ptr<TConsumerProfilingCounters>> oldConsumerProfilingCounters;
            THashMap<TCrossClusterReference, std::vector<TConsumerPartitionProfilingCounters>> oldConsumerPartitionProfilingCounters;
            oldConsumerProfilingCounters.swap(ConsumerProfilingCounters_);
            oldConsumerPartitionProfilingCounters.swap(ConsumerPartitionProfilingCounters_);
            for (const auto& consumerRef : consumerRefs) {
                if (auto it = oldConsumerProfilingCounters.find(consumerRef); it != oldConsumerProfilingCounters.end()) {
                    ConsumerProfilingCounters_[consumerRef] = std::move(it->second);
                } else {
                    ConsumerProfilingCounters_[consumerRef] = std::make_unique<TConsumerProfilingCounters>(ConsumerProfiler_);
                }
                ConsumerPartitionProfilingCounters_[consumerRef].swap(oldConsumerPartitionProfilingCounters[consumerRef]);
            }
        }

        for (auto& [consumerRef, consumerPartitionProfilingCounters] : ConsumerPartitionProfilingCounters_) {
            auto consumerPartitionProfiler = ConsumerPartitionProfiler_
                .WithRequiredTag("consumer_path", consumerRef.Path)
                .WithRequiredTag("consumer_cluster", consumerRef.Cluster);
            resizePartitionCounters(consumerPartitionProfilingCounters, consumerPartitionProfiler);
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
