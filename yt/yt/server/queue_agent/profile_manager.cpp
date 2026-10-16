#include "profile_manager.h"

#include "helpers.h"
#include "snapshot.h"

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/simple_sensor_impl.h>

#include <library/cpp/yt/misc/range_helpers.h>

#include <util/generic/algorithm.h>

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

TError GetSnapshotError(const TError& previousSnapshotError, const TError& currentSnapshotError)
{
    if (!previousSnapshotError.IsOK() || !currentSnapshotError.IsOK()) {
        return TError("At least one of the snapshots contains errors")
            .With("previous_snapshot_error", previousSnapshotError)
            .With("current_snapshot_error", currentSnapshotError);
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

    explicit TQueuePartitionProfilingCounters(const TProfiler& profiler)
        : RowsWritten(profiler.Counter("/rows_written"))
        , RowsTrimmed(profiler.Counter("/rows_trimmed"))
        , DataWeightWritten(profiler.Counter("/data_weight_written"))
        , RowCount(profiler.Gauge("/row_count"))
        , DataWeight(profiler.Gauge("/data_weight"))
    { }
};

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void ResizePartitionCounters(
    std::vector<TQueuePartitionProfilingCounters>* counters,
    const TProfiler& profiler,
    int partitionCount,
    const TLogger& Logger)
{
    if (std::ssize(*counters) != partitionCount) {
        YT_TLOG_DEBUG("Resizing partition counters")
            .With("OldSize", counters->size())
            .With("NewSize", partitionCount);
    }

    if (std::ssize(*counters) > partitionCount) {
        counters->erase(counters->begin() + partitionCount, counters->end());
    } else {
        for (int partitionIndex = counters->size(); partitionIndex < partitionCount; ++partitionIndex) {
            counters->emplace_back(profiler.WithTag("partition_index", ToString(partitionIndex)));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

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
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Queue>(
                            row,
                            NDetail::TProfilingOptions{
                                .EnablePathAggregation = true,
                                .AddObjectType = true,
                                .Leading = leading,
                            }))
                        .WithPrefix("/queue/controller"),
                },
                {
                    EProfilerScope::AlertManager,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Queue>(
                            row,
                            NDetail::TProfilingOptions{
                                .EnablePathAggregation = true,
                            }))
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
            YT_TLOG_DEBUG("Skipping profiling iteration due to snapshot incompatibility")
                .With(snapshotCompatibilityError);
            return;
        }

        if (auto snapshotError = GetSnapshotError(previousQueueSnapshot->Error, currentQueueSnapshot->Error); !snapshotError.IsOK()) {
            YT_TLOG_DEBUG("Skipping profiling iteration due to snapshot error")
                .With(snapshotError);
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
                YT_TLOG_DEBUG("Skipping partition in profiling due to error")
                    .With("PartitionIndex", partitionIndex)
                    .With(snapshotError);
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

        ResizePartitionCounters(&QueuePartitionProfilingCounters_, GetProfiler(EProfilerScope::ObjectPartition), partitionCount, Logger);
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

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

//! Keeps counters only for registered partitions; null or empty list means all partitions.
void UpdateRegisteredPartitionCounters(
    THashMap<int, TConsumerPartitionProfilingCounters>* counters,
    const TProfiler& profiler,
    int partitionCount,
    const std::optional<std::vector<int>>& registeredPartitions,
    const TLogger& Logger)
{
    THashSet<int> partitionIndexes;
    if (registeredPartitions && !registeredPartitions->empty()) {
        std::vector<int> droppedPartitionIndexes;
        for (auto partitionIndex : *registeredPartitions) {
            if (partitionIndex >= 0 && partitionIndex < partitionCount) {
                partitionIndexes.insert(partitionIndex);
            } else {
                droppedPartitionIndexes.push_back(partitionIndex);
            }
        }
        YT_TLOG_DEBUG_IF(!droppedPartitionIndexes.empty(), "Ignoring registered partitions with indexes out of queue partition range")
            .With("PartitionCount", partitionCount)
            .With("PartitionIndexes", droppedPartitionIndexes);
    } else {
        partitionIndexes = std::views::iota(0, partitionCount) | RangeTo<THashSet<int>>();
    }

    auto oldSize = counters->size();
    bool updated = false;
    EraseNodesIf(*counters, [&] (const auto& pair) {
        bool erase = !partitionIndexes.contains(pair.first);
        updated |= erase;
        return erase;
    });
    for (auto partitionIndex : partitionIndexes) {
        if (counters->contains(partitionIndex)) {
            continue;
        }
        counters->emplace(
            partitionIndex,
            TConsumerPartitionProfilingCounters(
                profiler.WithTag("partition_index", ToString(partitionIndex)),
                profiler.WithExcludedTag("partition_index", ToString(partitionIndex))));
        updated = true;
    }

    YT_TLOG_DEBUG_IF(updated, "Partition counters updated")
        .With("OldSize", oldSize)
        .With("NewSize", counters->size());
}

//! Null or empty list means the consumer reads all partitions of the queue.
const std::optional<std::vector<int>>& GetRegisteredPartitions(
    const TConsumerSnapshotPtr& consumerSnapshot,
    const TTablePath& queuePath)
{
    auto registrationIt = std::ranges::find(consumerSnapshot->Registrations, queuePath, &TConsumerRegistrationTableRow::Queue);
    YT_VERIFY(registrationIt != consumerSnapshot->Registrations.end());
    return registrationIt->Partitions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TConsumerProfileManager
    : public NDetail::TProfileManagerBase<TConsumerSnapshotPtr>
{
public:
    TConsumerProfileManager(
        const TProfiler& profiler,
        const TLogger& logger,
        const std::optional<std::string>& consumerName,
        const TConsumerTableRow& row,
        bool leading)
        : TProfileManagerBase(
            {
                {
                    EProfilerScope::Object,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Consumer>(
                            row,
                            NDetail::TProfilingOptions{
                                .Name = consumerName,
                            }))
                        .WithGlobal()
                        .WithPrefix("/consumer"),
                },
                {
                    EProfilerScope::ObjectPartition,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Consumer>(
                            row,
                            NDetail::TProfilingOptions{
                                .Name = consumerName,
                            }))
                        .WithGlobal()
                        .WithPrefix("/consumer_partition"),
                },
                {
                    EProfilerScope::ObjectPass,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Consumer>(
                            row,
                            NDetail::TProfilingOptions{
                                .EnablePathAggregation = true,
                                .AddObjectType = true,
                                .Leading = leading,
                                .Name = consumerName,
                            }))
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
            YT_TLOG_DEBUG("Skipping profiling iteration due to snapshot incompatibility")
                .With(snapshotCompatibilityError);
            return;
        }

        if (auto snapshotError = GetSnapshotError(previousConsumerSnapshot->Error, currentConsumerSnapshot->Error); !snapshotError.IsOK()) {
            YT_TLOG_DEBUG("Skipping profiling iteration due to snapshot error")
                .With(snapshotError);
            return;
        }

        // NB: It is important to perform this call after validating that the snapshot doesn't contain errors.
        // Otherwise, we might end up using incorrect default values from the snapshot.
        EnsureCounters(currentConsumerSnapshot);

        YT_TLOG_DEBUG("Profiling consumer")
            .With("SubConsumerSnapshots", currentConsumerSnapshot->SubSnapshots.size());

        for (const auto& queuePath : GetKeys(currentConsumerSnapshot->SubSnapshots)) {
            const auto& previousSubSnapshot = previousConsumerSnapshot->SubSnapshots[queuePath];
            const auto& currentSubSnapshot = currentConsumerSnapshot->SubSnapshots[queuePath];

            auto partitionCount = currentSubSnapshot->PartitionCount;

            if (auto snapshotError = GetSnapshotError(previousSubSnapshot->Error, currentSubSnapshot->Error); !snapshotError.IsOK()) {
                YT_TLOG_DEBUG("Skipping sub-consumer snapshot in profiling due to error")
                    .With("Queue", queuePath)
                    .With(snapshotError);
                continue;
            }

            // NB: It is important to perform this call after validating that the snapshot doesn't contain errors.
            // Otherwise, we might end up using incorrect default values from the snapshot.
            EnsureConsumerPartitionCounters(queuePath, currentSubSnapshot, GetRegisteredPartitions(currentConsumerSnapshot, queuePath));

            const auto& previousPartitionSnapshots = previousSubSnapshot->PartitionSnapshots;
            const auto& currentPartitionSnapshots = currentSubSnapshot->PartitionSnapshots;

            auto& subConsumerProfilingCounters = ConsumerPartitionProfilingCounters_[queuePath].Counters;

            YT_TLOG_DEBUG("Profiling partitions for sub-consumer")
                .With("Queue", queuePath)
                .With("Partitions", partitionCount)
                .With("RegisteredPartitions", subConsumerProfilingCounters.size());

            for (auto& [partitionIndex, profilingCounters] : subConsumerProfilingCounters) {
                const auto& previousConsumerPartitionSnapshot = previousPartitionSnapshots[partitionIndex];
                const auto& currentConsumerPartitionSnapshot = currentPartitionSnapshots[partitionIndex];

                if (auto snapshotError = GetSnapshotError(previousConsumerPartitionSnapshot->Error, currentConsumerPartitionSnapshot->Error); !snapshotError.IsOK()) {
                    YT_TLOG_DEBUG("Skipping partition in profiling due to error")
                        .With("Queue", queuePath)
                        .With("PartitionIndex", partitionIndex)
                        .With(snapshotError);
                    continue;
                }

                auto rowsConsumed = currentConsumerPartitionSnapshot->NextRowIndex - previousConsumerPartitionSnapshot->NextRowIndex;
                SafeIncrement(profilingCounters.RowsConsumed, rowsConsumed);

                auto dataWeightConsumed = OptionalSub(
                    currentConsumerPartitionSnapshot->CumulativeDataWeight,
                    previousConsumerPartitionSnapshot->CumulativeDataWeight);
                SafeIncrement(profilingCounters.DataWeightConsumed, dataWeightConsumed);

                if (rowsConsumed > 0 && !dataWeightConsumed && currentSubSnapshot->HasCumulativeDataWeightColumn) {
                    YT_TLOG_DEBUG("Consumer for queue with cumulative data weight support could not export data weight consumed")
                        .With("Queue", queuePath)
                        .With("PartitionIndex", partitionIndex)
                        .With("OldCumulativeDataWeight", previousConsumerPartitionSnapshot->CumulativeDataWeight)
                        .With("NewCumulativeDataWeight", currentConsumerPartitionSnapshot->CumulativeDataWeight)
                        .With("OldNextRowIndex", previousConsumerPartitionSnapshot->NextRowIndex)
                        .With("NewNextRowIndex", currentConsumerPartitionSnapshot->NextRowIndex)
                        .With("RowsConsumed", rowsConsumed)
                        .With("OldUnreadRowCount", previousConsumerPartitionSnapshot->UnreadRowCount)
                        .With("NewUnreadRowCount", currentConsumerPartitionSnapshot->UnreadRowCount)
                        .With("OldUnreadDataWeight", previousConsumerPartitionSnapshot->UnreadDataWeight)
                        .With("NewUnreadDataWeight", currentConsumerPartitionSnapshot->UnreadDataWeight);
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
        THashMap<int, TConsumerPartitionProfilingCounters> Counters;
    };

    THashMap<TTablePath, TPartitionProfiler> ConsumerPartitionProfilingCounters_;

    void EnsureCounters(const TConsumerSnapshotPtr& currentConsumerSnapshot)
    {
        if (!ConsumerProfilingCounters_) {
            ConsumerProfilingCounters_ = std::make_unique<TConsumerProfilingCounters>(GetProfiler(EProfilerScope::Object));
        }

        // Remove counters for outdated registrations.
        decltype(ConsumerPartitionProfilingCounters_) newConsumerPartitionProfilingCounters;
        for (const auto& queuePath : GetKeys(currentConsumerSnapshot->SubSnapshots)) {
            if (ConsumerPartitionProfilingCounters_.contains(queuePath)) {
                newConsumerPartitionProfilingCounters[queuePath] = ConsumerPartitionProfilingCounters_[queuePath];
            }
        }
        ConsumerPartitionProfilingCounters_ = std::move(newConsumerPartitionProfilingCounters);
    }

    void EnsureConsumerPartitionCounters(
        const TTablePath& queuePath,
        const TSubConsumerSnapshotPtr& subConsumerSnapshot,
        const std::optional<std::vector<int>>& registeredPartitions)
    {
        auto profiler = GetProfiler(EProfilerScope::ObjectPartition);
        TTagSet tagSet;
        tagSet.AddRequiredTag({"queue_cluster", queuePath.GetCluster().value()});
        tagSet.AddRequiredTag({"queue_path", TrimProfilingTagValue(queuePath.GetPath())});
        tagSet.AddRequiredTag({"queue_tag", subConsumerSnapshot->QueueProfilingTag.value_or(NoneProfilingTag)});
        profiler = profiler.WithTags(tagSet);

        if (!ConsumerPartitionProfilingCounters_.contains(queuePath)) {
            ConsumerPartitionProfilingCounters_[queuePath] = TPartitionProfiler{
                .CurrentQueueTag = subConsumerSnapshot->QueueProfilingTag,
            };
        }

        auto& partitionProfiler = ConsumerPartitionProfilingCounters_[queuePath];

        if (partitionProfiler.CurrentQueueTag != subConsumerSnapshot->QueueProfilingTag) {
            YT_TLOG_DEBUG("Updating consumer partition counters")
                .With("Queue", queuePath)
                .With("Partitions", subConsumerSnapshot->PartitionCount)
                .With("OldQueueTag", partitionProfiler.CurrentQueueTag)
                .With("NewQueueTag", subConsumerSnapshot->QueueProfilingTag);

            partitionProfiler.CurrentQueueTag = subConsumerSnapshot->QueueProfilingTag;
            partitionProfiler.Counters = {};
        }

        UpdateRegisteredPartitionCounters(
            &partitionProfiler.Counters,
            profiler,
            subConsumerSnapshot->PartitionCount,
            registeredPartitions,
            Logger().WithTag("Queue", queuePath));
    }

    TError CheckSnapshotCompatibility(const TConsumerSnapshotPtr& previousConsumerSnapshot, const TConsumerSnapshotPtr& currentConsumerSnapshot) const
    {
        auto getQueuePathsAndPartitionCounts = [] (const TConsumerSnapshotPtr& snapshot) {
            std::vector<std::pair<TTablePath, int>> result;
            for (const auto& [queuePath, subSnapshot] : snapshot->SubSnapshots) {
                result.emplace_back(queuePath, subSnapshot->PartitionCount);
            }
            std::ranges::sort(result);
            return result;
        };

        auto previousQueuePathsAndPartitions = getQueuePathsAndPartitionCounts(previousConsumerSnapshot);
        auto currentQueuePathsAndPartitions = getQueuePathsAndPartitionCounts(currentConsumerSnapshot);

        if (previousQueuePathsAndPartitions != currentQueuePathsAndPartitions) {
            return TError(
                "Queue refs and partitions differ: %v != %v",
                previousQueuePathsAndPartitions,
                currentQueuePathsAndPartitions);
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
    const std::optional<std::string>& consumerName,
    const TConsumerTableRow& row,
    bool leading)
{
    return New<TConsumerProfileManager>(profiler, logger, consumerName, row, leading);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
