#include "snapshot_representation.h"
#include "snapshot.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueAgent {

using namespace NYson;
using namespace NYTree;

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////

void BuildEmaCounterYson(const TEmaCounter& counter, TFluentAny fluent)
{
    fluent.BeginMap()
        .Item("current").Value(counter.ImmediateRate)
        .Item("1m").Value(counter.WindowRates[0])
        .Item("1h").Value(counter.WindowRates[1])
        .Item("1d").Value(counter.WindowRates[2])
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void BuildQueueStatusYson(const TQueueSnapshotPtr& snapshot, TFluentAny fluent)
{
    if (!snapshot->Error.IsOK()) {
        fluent
            .BeginMap()
                .Item("error").Value(snapshot->Error)
            .EndMap();
        return;
    }

    fluent
        .BeginMap()
            .Item("family").Value(snapshot->Family)
            .Item("partition_count").Value(snapshot->PartitionCount)
            .Item("consumers").BeginMap()
                .DoFor(snapshot->ConsumerSnapshots, [&] (TFluentMap fluent, const auto& pair) {
                    const auto& consumerRef = pair.first;
                    const auto& consumerSnapshot = pair.second;
                    Y_UNUSED(consumerSnapshot);

                    if (consumerSnapshot->Error.IsOK()) {
                        fluent
                            .Item(ToString(consumerRef)).BeginMap()
                                .Item("vital").Value(consumerSnapshot->Vital)
                                .Item("owner").Value(consumerSnapshot->Owner)
                            .EndMap();
                    } else {
                        fluent
                            .Item(ToString(consumerRef)).BeginMap()
                                .Item("error").Value(consumerSnapshot->Error)
                            .EndMap();
                    }
                })
            .EndMap()
            .Item("has_timestamp_column").Value(snapshot->HasTimestampColumn)
            .Item("has_cumulative_data_weight_column").Value(snapshot->HasCumulativeDataWeightColumn)
            .Item("write_row_count_rate").Do(std::bind(BuildEmaCounterYson, snapshot->WriteRate.RowCount, _1))
            .Item("write_data_weight_rate").Do(std::bind(BuildEmaCounterYson, snapshot->WriteRate.DataWeight, _1))
        .EndMap();
}

void BuildQueuePartitionYson(const TQueuePartitionSnapshotPtr& snapshot, TFluentAny fluent)
{
    if (!snapshot->Error.IsOK()) {
        fluent
            .BeginMap()
                .Item("error").Value(snapshot->Error)
            .EndMap();
        return;
    }

    fluent
        .BeginMap()
            .Item("lower_row_index").Value(snapshot->LowerRowIndex)
            .Item("upper_row_index").Value(snapshot->UpperRowIndex)
            .Item("available_row_count").Value(snapshot->AvailableRowCount)
            .Item("last_row_commit_time").Value(snapshot->LastRowCommitTime)
            .Item("commit_idle_time").Value(snapshot->CommitIdleTime)
            .Item("write_row_count_rate").Do(std::bind(BuildEmaCounterYson, snapshot->WriteRate.RowCount, _1))
            .Item("write_data_weight_rate").Do(std::bind(BuildEmaCounterYson, snapshot->WriteRate.DataWeight, _1))
            .Item("meta").Value(snapshot->Meta)
        .EndMap();
}

void BuildQueuePartitionListYson(const TQueueSnapshotPtr& snapshot, TFluentAny fluent)
{
    if (!snapshot->Error.IsOK()) {
        fluent
            .BeginList()
            .EndList();
        return;
    }

    fluent
        .BeginList()
            .DoFor(snapshot->PartitionSnapshots, [&] (TFluentList fluent, const TQueuePartitionSnapshotPtr& snapshot) {
                fluent
                    .Item().Do(std::bind(BuildQueuePartitionYson, snapshot, _1));
            })
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////

void BuildConsumerStatusYson(const TConsumerSnapshotPtr& snapshot, TFluentAny fluent)
{
    if (!snapshot->Error.IsOK()) {
        fluent
            .BeginMap()
                .Item("error").Value(snapshot->Error)
            .EndMap();
        return;
    }

    fluent
        .BeginMap()
            .Item("target_queue").Value(snapshot->TargetQueue)
            .Item("vital").Value(snapshot->Vital)
            .Item("owner").Value(snapshot->Owner)
            .Item("partition_count").Value(snapshot->PartitionCount)
            .Item("read_row_count_rate").Do(std::bind(BuildEmaCounterYson, snapshot->ReadRate.RowCount, _1))
            .Item("read_data_weight_rate").Do(std::bind(BuildEmaCounterYson, snapshot->ReadRate.DataWeight, _1))
        .EndMap();
}

void BuildConsumerPartitionYson(const TConsumerPartitionSnapshotPtr& snapshot, TFluentAny fluent)
{
    if (!snapshot->Error.IsOK()) {
        fluent
            .BeginMap()
                .Item("error").Value(snapshot->Error)
                .Item("next_row_index").Value(snapshot->NextRowIndex)
                .Item("last_consume_time").Value(snapshot->LastConsumeTime)
            .EndMap();
        return;
    }

    fluent
        .BeginMap()
            .Item("next_row_index").Value(snapshot->NextRowIndex)
            .Item("last_consume_time").Value(snapshot->LastConsumeTime)
            .Item("disposition").Value(snapshot->Disposition)
            .Item("unread_row_count").Value(snapshot->UnreadRowCount)
            .Item("next_row_commit_time").Value(snapshot->NextRowCommitTime)
            .Item("processing_lag").Value(snapshot->ProcessingLag)
            .Item("consume_idle_time").Value(snapshot->ConsumeIdleTime)
            .Item("cumulative_data_weight").Value(snapshot->CumulativeDataWeight)
            .Item("read_row_count_rate").Do(std::bind(BuildEmaCounterYson, snapshot->ReadRate.RowCount, _1))
            .Item("read_data_weight_rate").Do(std::bind(BuildEmaCounterYson, snapshot->ReadRate.DataWeight, _1))
        .EndMap();
}

void BuildConsumerPartitionListYson(const TConsumerSnapshotPtr& snapshot, TFluentAny fluent)
{
    if (!snapshot->Error.IsOK()) {
        fluent
            .BeginList()
            .EndList();
        return;
    }

    fluent
        .BeginList()
            .DoFor(snapshot->PartitionSnapshots, [&] (TFluentList fluent, const TConsumerPartitionSnapshotPtr& snapshot) {
                fluent
                    .Item().Do(std::bind(BuildConsumerPartitionYson, snapshot, _1));
            })
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
