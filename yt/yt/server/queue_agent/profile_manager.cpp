#include "profile_manager.h"

#include "snapshot.h"
#include "helpers.h"

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/solomon/sensor.h>

namespace NYT::NQueueAgent {

using namespace NLogging;
using namespace NProfiling;
using namespace NQueueClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

//! Helper for incrementing a counter only if delta is non-negative.
void SafeIncrement(TCounter& counter, i64 delta)
{
    if (delta >= 0) {
        counter.Increment(delta);
    }
}

//! Helper for incrementing a counter only if delta is non-null and non-negative.
void SafeIncrement(TCounter& counter, std::optional<i64> delta)
{
    if (delta) {
        SafeIncrement(counter, *delta);
    }
}

//! Helper for updating a gauge only if value is non-null.
void SafeUpdate(TGauge& gauge, std::optional<i64> value)
{
    if (value) {
        gauge.Update(*value);
    }
}

auto ResizePartitionCounters(auto& counters, const TProfiler& profiler, int partitionCount, const TLogger& Logger)
{
    if (std::ssize(counters) != partitionCount) {
        YT_LOG_DEBUG("Resizing partition counters (Size: %v -> %v)", counters.size(), partitionCount);
    }

    if (std::ssize(counters) > partitionCount) {
        counters.erase(counters.begin() + partitionCount, counters.end());
    } else {
        for (int partitionIndex = counters.size(); partitionIndex < partitionCount; ++partitionIndex) {
            const auto& partitionProfiler = profiler
                .WithTag("partition_index", ToString(partitionIndex));
            const auto& aggregationPartitionProfiler = profiler
                .WithExcludedTag("partition_index", ToString(partitionIndex));
            counters.emplace_back(partitionProfiler, aggregationPartitionProfiler);
        }
    }
}

TError GetSnapshotError(const TError& previousSnapshotError, const TError& currentSnapshotError)
{
    if (!previousSnapshotError.IsOK() || !currentSnapshotError.IsOK()) {
        return TError("At least one of the snapshots contains errors")
            << TErrorAttribute("previous_snapshot_error", previousSnapshotError)
            << TErrorAttribute("current_snapshot_error", currentSnapshotError);
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

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
    TCounter DataWeightWritten;
    TGauge RowCount;
    TGauge DataWeight;

    TQueuePartitionProfilingCounters(const TProfiler& profiler, const TProfiler& /*aggregationProfiler*/)
        : RowsWritten(profiler.Counter("/rows_written"))
        , RowsTrimmed(profiler.Counter("/rows_trimmed"))
        , DataWeightWritten(profiler.Counter("/data_weight_written"))
        , RowCount(profiler.Gauge("/row_count"))
        , DataWeight(profiler.Gauge("/data_weight"))
    { }
};

class TQueueProfileManager
    : public NDetail::TProfileManagerBase<TQueueSnapshotPtr>
{
public:
    TQueueProfileManager(
        const TProfiler& profiler,
        const TLogger& logger,
        const TQueueTableRow& row,
        bool leading)
        : TProfileManagerBase(
            {
                {
                    EProfilerScope::Object,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Queue>(row))
                        .WithGlobal()
                        .WithPrefix("/queue"),
                },
                {
                    EProfilerScope::ObjectPartition,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Queue>(row))
                        .WithGlobal()
                        .WithPrefix("/queue_partition"),
                },
                {
                    EProfilerScope::ObjectPass,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Queue>(row, /*enablePathAggregation*/ true, /*addObjectType*/ true, leading))
                        .WithPrefix("/queue/controller"),
                },
                {
                    EProfilerScope::AlertManager,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Queue>(row, /*enablePathAggregation*/ true))
                        .WithGlobal()
                        .WithPrefix("/queue/controller"),
                },
            })
        , Logger(logger)
    { }

    void Profile(
        const TQueueSnapshotPtr& previousQueueSnapshot,
        const TQueueSnapshotPtr& currentQueueSnapshot) override
    {
        if (auto snapshotCompatibilityError = CheckSnapshotCompatibility(previousQueueSnapshot, currentQueueSnapshot); !snapshotCompatibilityError.IsOK()) {
            // Simply wait for the next call when snapshots are compatible.
            // Losing an iteration of profiling is not bad for since profiling is essentially stateless.
            YT_LOG_DEBUG(snapshotCompatibilityError, "Skipping profiling iteration due to snapshot incompatibility");
            return;
        }

        if (auto snapshotError = GetSnapshotError(previousQueueSnapshot->Error, currentQueueSnapshot->Error); !snapshotError.IsOK()) {
            YT_LOG_DEBUG(snapshotError, "Skipping profiling iteration due to snapshot error");
            return;
        }

        // NB: It is important to perform this call after validating that the snapshot doesn't contain errors.
        // Otherwise, we might end up using incorrect default values from the snapshot.
        EnsureCounters(currentQueueSnapshot);

        auto partitionCount = currentQueueSnapshot->PartitionCount;

        QueueProfilingCounters_->Partitions.Update(partitionCount);
        int vitalConsumerCount = 0;
        int nonVitalConsumerCount = 0;
        for (const auto& registration : currentQueueSnapshot->Registrations) {
            ++(registration.Vital ? vitalConsumerCount : nonVitalConsumerCount);
        }
        QueueProfilingCounters_->VitalConsumers.Update(vitalConsumerCount);
        QueueProfilingCounters_->NonVitalConsumers.Update(nonVitalConsumerCount);

        // Mind the clamp. We do not want process to crash if some delta turns out to be negative due to some manual action.

        for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            const auto& previousQueuePartitionSnapshot = previousQueueSnapshot->PartitionSnapshots[partitionIndex];
            const auto& currentQueuePartitionSnapshot = currentQueueSnapshot->PartitionSnapshots[partitionIndex];

            if (auto snapshotError = GetSnapshotError(previousQueuePartitionSnapshot->Error, currentQueuePartitionSnapshot->Error); !snapshotError.IsOK()) {
                YT_LOG_DEBUG(
                    "Skipping partition in profiling due to error (Partition: %v, Error: %v)",
                    partitionIndex,
                    snapshotError);
                continue;
            }

            auto& profilingCounters = QueuePartitionProfilingCounters_[partitionIndex];

            auto rowsWritten = currentQueuePartitionSnapshot->UpperRowIndex - previousQueuePartitionSnapshot->UpperRowIndex;
            SafeIncrement(profilingCounters.RowsWritten, rowsWritten);

            auto rowsTrimmed = currentQueuePartitionSnapshot->LowerRowIndex - previousQueuePartitionSnapshot->LowerRowIndex;
            SafeIncrement(profilingCounters.RowsTrimmed, rowsTrimmed);

            SafeIncrement(profilingCounters.DataWeightWritten, OptionalSub(
                currentQueuePartitionSnapshot->CumulativeDataWeight,
                previousQueuePartitionSnapshot->CumulativeDataWeight));

            profilingCounters.RowCount.Update(currentQueuePartitionSnapshot->AvailableRowCount);
            SafeUpdate(profilingCounters.DataWeight, currentQueuePartitionSnapshot->AvailableDataWeight);
        }
    }

private:
    const TLogger Logger;

    std::unique_ptr<TQueueProfilingCounters> QueueProfilingCounters_;
    std::vector<TQueuePartitionProfilingCounters> QueuePartitionProfilingCounters_;

    //! Check if two snapshots are structurally similar (i.e. have same number of partitions and same set of consumers).
    TError CheckSnapshotCompatibility(
        const TQueueSnapshotPtr& previousQueueSnapshot,
        const TQueueSnapshotPtr& currentQueueSnapshot)
    {
        if (previousQueueSnapshot->PartitionCount != currentQueueSnapshot->PartitionCount) {
            return TError(
                "Partition counts differ: %v != %v",
                previousQueueSnapshot->PartitionCount,
                currentQueueSnapshot->PartitionCount);
        }

        return {};
    }

    //! Ensures the existence of all needed counter structures.
    void EnsureCounters(const TQueueSnapshotPtr& queueSnapshot)
    {
        auto partitionCount = queueSnapshot->PartitionCount;

        if (!QueueProfilingCounters_) {
            QueueProfilingCounters_ = std::make_unique<TQueueProfilingCounters>(GetProfiler(EProfilerScope::Object));
        }

        ResizePartitionCounters(QueuePartitionProfilingCounters_, GetProfiler(EProfilerScope::ObjectPartition), partitionCount, Logger);
    }
};

DEFINE_REFCOUNTED_TYPE(TQueueProfileManager)

////////////////////////////////////////////////////////////////////////////////

//! Consumer-related profiling counters.
struct TConsumerProfilingCounters
{
    TGauge Partitions;

    explicit TConsumerProfilingCounters(const TProfiler& profiler)
        : Partitions(profiler.Gauge("/partitions"))
    { }
};

//! Consumer-related per-partition profiling counters.
struct TConsumerPartitionProfilingCounters
{
    static constexpr ESummaryPolicy LagSummaryPolicy = ESummaryPolicy::Avg | ESummaryPolicy::Max | ESummaryPolicy::Sum;

    TCounter RowsConsumed;
    TCounter DataWeightConsumed;
    TGauge Offset;
    TGauge LagRows;
    TGauge LagDataWeight;
    TTimeGauge LagTime;
    TGaugeHistogram LagTimeHistogram;

    TConsumerPartitionProfilingCounters(const TProfiler& profiler, const TProfiler& aggregationProfiler)
        : RowsConsumed(profiler.Counter("/rows_consumed"))
        , DataWeightConsumed(profiler.Counter("/data_weight_consumed"))
        , Offset(profiler.GaugeSummary("/offset"))
        , LagRows(profiler.GaugeSummary("/lag_rows", LagSummaryPolicy))
        , LagDataWeight(profiler.GaugeSummary("/lag_data_weight", LagSummaryPolicy))
        , LagTime(profiler.TimeGaugeSummary("/lag_time", LagSummaryPolicy))
        , LagTimeHistogram(aggregationProfiler.GaugeHistogram("/lag_time_histogram", GenerateGenericBucketBounds()))
    { }
};

class TConsumerProfileManager
    : public NDetail::TProfileManagerBase<TConsumerSnapshotPtr>
{
public:
    TConsumerProfileManager(
        const TProfiler& profiler,
        const TLogger& logger,
        const TConsumerTableRow& row,
        bool leading)
        : TProfileManagerBase(
            {
                {
                    EProfilerScope::Object,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Consumer>(row))
                        .WithGlobal()
                        .WithPrefix("/consumer"),
                },
                {
                    EProfilerScope::ObjectPartition,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Consumer>(row))
                        .WithGlobal()
                        .WithPrefix("/consumer_partition"),
                },
                {
                    EProfilerScope::ObjectPass,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Consumer>(row, /*enablePathAggregation*/ true, /*addObjectType*/ true, leading))
                        .WithPrefix("/consumer/controller"),
                },
                {EProfilerScope::AlertManager, profiler},
            })
        , Logger(logger)
    { }

    void Profile(
        const TConsumerSnapshotPtr& previousConsumerSnapshot,
        const TConsumerSnapshotPtr& currentConsumerSnapshot) override
    {
        if (auto snapshotCompatibilityError = CheckSnapshotCompatibility(previousConsumerSnapshot, currentConsumerSnapshot); !snapshotCompatibilityError.IsOK()) {
            // Simply wait for the next call when snapshots are compatible.
            // Losing an iteration of profiling is not bad for since profiling is essentially stateless.
            YT_LOG_DEBUG(snapshotCompatibilityError, "Skipping profiling iteration due to snapshot incompatibility");
            return;
        }

        if (auto snapshotError = GetSnapshotError(previousConsumerSnapshot->Error, currentConsumerSnapshot->Error); !snapshotError.IsOK()) {
            YT_LOG_DEBUG(snapshotError, "Skipping profiling iteration due to snapshot error");
            return;
        }

        // NB: It is important to perform this call after validating that the snapshot doesn't contain errors.
        // Otherwise, we might end up using incorrect default values from the snapshot.
        EnsureCounters(currentConsumerSnapshot);

        YT_LOG_DEBUG("Profiling consumer (SubConsumerSnapshots: %v)", currentConsumerSnapshot->SubSnapshots.size());

        for (const auto& queueRef : GetKeys(currentConsumerSnapshot->SubSnapshots)) {
            const auto& previousSubSnapshot = previousConsumerSnapshot->SubSnapshots[queueRef];
            const auto& currentSubSnapshot = currentConsumerSnapshot->SubSnapshots[queueRef];

            auto partitionCount = currentSubSnapshot->PartitionCount;

            if (auto snapshotError = GetSnapshotError(previousSubSnapshot->Error, currentSubSnapshot->Error); !snapshotError.IsOK()) {
                YT_LOG_DEBUG(
                    "Skipping sub-consumer snapshot in profiling due to error (Queue: %v, Error: %v)",
                    queueRef,
                    snapshotError);
                continue;
            }

            // NB: It is important to perform this call after validating that the snapshot doesn't contain errors.
            // Otherwise, we might end up using incorrect default values from the snapshot.
            EnsureConsumerPartitionCounters(queueRef, currentSubSnapshot);

            const auto& previousPartitionSnapshots = previousSubSnapshot->PartitionSnapshots;
            const auto& currentPartitionSnapshots = currentSubSnapshot->PartitionSnapshots;

            auto& subConsumerProfilingCounters = ConsumerPartitionProfilingCounters_[queueRef].Counters;

            YT_LOG_DEBUG(
                "Profiling partitions for sub-consumer (Queue: %v, Partitions: %v)",
                queueRef,
                partitionCount);

            for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
                const auto& previousConsumerPartitionSnapshot = previousPartitionSnapshots[partitionIndex];
                const auto& currentConsumerPartitionSnapshot = currentPartitionSnapshots[partitionIndex];

                if (auto snapshotError = GetSnapshotError(previousConsumerPartitionSnapshot->Error, currentConsumerPartitionSnapshot->Error); !snapshotError.IsOK()) {
                    YT_LOG_DEBUG(
                        "Skipping partition in profiling due to error (Queue: %v, Partition: %v, Error: %v)",
                        queueRef,
                        partitionIndex,
                        snapshotError);
                    continue;
                }

                auto& profilingCounters = subConsumerProfilingCounters[partitionIndex];

                auto rowsConsumed = currentConsumerPartitionSnapshot->NextRowIndex - previousConsumerPartitionSnapshot->NextRowIndex;
                SafeIncrement(profilingCounters.RowsConsumed, rowsConsumed);

                auto dataWeightConsumed = OptionalSub(
                    currentConsumerPartitionSnapshot->CumulativeDataWeight,
                    previousConsumerPartitionSnapshot->CumulativeDataWeight);
                SafeIncrement(profilingCounters.DataWeightConsumed, dataWeightConsumed);

                if (rowsConsumed > 0 && !dataWeightConsumed && currentSubSnapshot->HasCumulativeDataWeightColumn) {
                    YT_LOG_DEBUG(
                        "Consumer for queue with cumulative data weight support could not export data weight consumed "
                        "(Queue: %v, Partition: %v, CumulativeDataWeight: %v -> %v, NextRowIndex: %v -> %v, RowsConsumed: %v, UnreadRowCount: %v -> %v, UnreadDataWeight: %v -> %v)",
                        queueRef,
                        partitionIndex,
                        previousConsumerPartitionSnapshot->CumulativeDataWeight,
                        currentConsumerPartitionSnapshot->CumulativeDataWeight,
                        previousConsumerPartitionSnapshot->NextRowIndex,
                        currentConsumerPartitionSnapshot->NextRowIndex,
                        rowsConsumed,
                        previousConsumerPartitionSnapshot->UnreadRowCount,
                        currentConsumerPartitionSnapshot->UnreadRowCount,
                        previousConsumerPartitionSnapshot->UnreadDataWeight,
                        currentConsumerPartitionSnapshot->UnreadDataWeight);
                }

                profilingCounters.Offset.Update(currentConsumerPartitionSnapshot->NextRowIndex);
                profilingCounters.LagRows.Update(currentConsumerPartitionSnapshot->UnreadRowCount);
                SafeUpdate(profilingCounters.LagDataWeight, currentConsumerPartitionSnapshot->UnreadDataWeight);
                profilingCounters.LagTime.Update(currentConsumerPartitionSnapshot->ProcessingLag);
                profilingCounters.LagTimeHistogram.Reset();
                profilingCounters.LagTimeHistogram.Add(currentConsumerPartitionSnapshot->ProcessingLag.MillisecondsFloat());
            }
        }
    }

private:
    const TLogger Logger;

    std::unique_ptr<TConsumerProfilingCounters> ConsumerProfilingCounters_;

    struct TPartitionProfiler
    {
        std::optional<std::string> CurrentQueueTag;
        std::vector<TConsumerPartitionProfilingCounters> Counters{};
    };

    THashMap<TCrossClusterReference, TPartitionProfiler> ConsumerPartitionProfilingCounters_;

    void EnsureCounters(const TConsumerSnapshotPtr& currentConsumerSnapshot)
    {
        if (!ConsumerProfilingCounters_) {
            ConsumerProfilingCounters_ = std::make_unique<TConsumerProfilingCounters>(GetProfiler(EProfilerScope::Object));
        }

        // Remove counters for outdated registrations.
        decltype(ConsumerPartitionProfilingCounters_) newConsumerPartitionProfilingCounters;
        for (const auto& queueRef : GetKeys(currentConsumerSnapshot->SubSnapshots)) {
            if (ConsumerPartitionProfilingCounters_.contains(queueRef)) {
                newConsumerPartitionProfilingCounters[queueRef] = ConsumerPartitionProfilingCounters_[queueRef];
            }
        }
        ConsumerPartitionProfilingCounters_ = std::move(newConsumerPartitionProfilingCounters);
    }

    void EnsureConsumerPartitionCounters(const TCrossClusterReference& queueRef, const TSubConsumerSnapshotPtr& subConsumerSnapshot)
    {
        auto profiler = GetProfiler(EProfilerScope::ObjectPartition);
        TTagSet tagSet;
        tagSet.AddRequiredTag({"queue_cluster", queueRef.Cluster});
        tagSet.AddRequiredTag({"queue_path", TrimProfilingTagValue(queueRef.Path)});
        if (subConsumerSnapshot->QueueProfilingTag.has_value()) {
            tagSet.AddRequiredTag({"queue_tag", subConsumerSnapshot->QueueProfilingTag.value()});
        }
        profiler = profiler.WithTags(tagSet);

        if (!ConsumerPartitionProfilingCounters_.contains(queueRef)) {
            ConsumerPartitionProfilingCounters_[queueRef] = TPartitionProfiler{
                .CurrentQueueTag = subConsumerSnapshot->QueueProfilingTag,
            };
        }

        auto& partitionProfiler = ConsumerPartitionProfilingCounters_[queueRef];

        if (partitionProfiler.CurrentQueueTag != subConsumerSnapshot->QueueProfilingTag) {
            YT_LOG_DEBUG(
                "Updating consumer partition counters (Queue: %v, Partitions: %v, QueueTag: %v -> %v)",
                queueRef,
                subConsumerSnapshot->PartitionCount,
                partitionProfiler.CurrentQueueTag,
                subConsumerSnapshot->QueueProfilingTag);

            partitionProfiler.CurrentQueueTag = subConsumerSnapshot->QueueProfilingTag;
            partitionProfiler.Counters = {};
        }

        ResizePartitionCounters(
            partitionProfiler.Counters,
            profiler,
            subConsumerSnapshot->PartitionCount,
            Logger().WithTag("Queue: %v", queueRef));
    }

    TError CheckSnapshotCompatibility(const TConsumerSnapshotPtr& previousConsumerSnapshot, const TConsumerSnapshotPtr& currentConsumerSnapshot) const
    {
        auto getQueueRefsAndPartitionCounts = [] (const TConsumerSnapshotPtr& snapshot) {
        std::vector<std::pair<TCrossClusterReference, int>> result;
            for (const auto& [queueRef, subSnapshot] : snapshot->SubSnapshots) {
                result.emplace_back(queueRef, subSnapshot->PartitionCount);
            }
            std::sort(result.begin(), result.end());
            return result;
        };

        auto previousQueueRefsAndPartitions = getQueueRefsAndPartitionCounts(previousConsumerSnapshot);
        auto currentQueueRefsAndPartitions = getQueueRefsAndPartitionCounts(currentConsumerSnapshot);

        if (previousQueueRefsAndPartitions != currentQueueRefsAndPartitions) {
            return TError(
                "Queue refs and partitions differ: %v != %v",
                previousQueueRefsAndPartitions,
                currentQueueRefsAndPartitions);
        }

        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(TConsumerProfileManager);

////////////////////////////////////////////////////////////////////////////////

IQueueProfileManagerPtr CreateQueueProfileManager(
    const TProfiler& profiler,
    const TLogger& logger,
    const TQueueTableRow& row,
    bool leading)
{
    return New<TQueueProfileManager>(profiler, logger, row, leading);
}

IConsumerProfileManagerPtr CreateConsumerProfileManager(
    const TProfiler& profiler,
    const TLogger& logger,
    const TConsumerTableRow& row,
    bool leading)
{
    return New<TConsumerProfileManager>(profiler, logger, row, leading);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
