#include "snapshot_representation.h"
#include "snapshot.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueAgent {

using namespace NYson;
using namespace NYTree;

using namespace std::placeholders;

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
