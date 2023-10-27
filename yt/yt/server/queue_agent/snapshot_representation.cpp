#include "snapshot_representation.h"
#include "snapshot.h"

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/net/local_address.h>

namespace NYT::NQueueAgent {

using namespace NNet;
using namespace NQueueClient;
using namespace NYson;
using namespace NYTree;

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////

void BuildEmaCounterYson(const TEmaCounter& counter, TFluentAny fluent)
{
    auto immediateRate = counter.ImmediateRate;
    auto oneMinuteRateRaw = counter.WindowRates[0];
    auto oneMinuteRate = counter.GetRate(0).value_or(immediateRate);
    auto oneHourRate = counter.GetRate(1).value_or(oneMinuteRate);
    auto oneDayRate = counter.GetRate(2).value_or(oneHourRate);

    fluent.BeginMap()
        .Item("current").Value(immediateRate)
        .Item("1m_raw").Value(oneMinuteRateRaw)
        .Item("1m").Value(oneMinuteRate)
        .Item("1h").Value(oneHourRate)
        .Item("1d").Value(oneDayRate)
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void BuildRegistrationYson(TFluentList fluent, TConsumerRegistrationTableRow registration)
{
    fluent
        .Item()
            .BeginMap()
                .Item("queue").Value(registration.Queue)
                .Item("consumer").Value(registration.Consumer)
                .Item("vital").Value(registration.Vital)
            .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void BuildQueueStatusYson(const TQueueSnapshotPtr& snapshot, TFluentAny fluent)
{
    if (!snapshot->Error.IsOK()) {
        fluent
            .BeginMap()
                .Item("queue_agent_host").Value(GetLocalHostName())
                .Item("error").Value(snapshot->Error)
            .EndMap();
        return;
    }

    fluent
        .BeginMap()
            .Item("queue_agent_host").Value(GetLocalHostName())
            .Item("family").Value(snapshot->Family)
            .Item("partition_count").Value(snapshot->PartitionCount)
            .Item("registrations").DoListFor(snapshot->Registrations, BuildRegistrationYson)
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
            .Item("cumulative_data_weight").Value(snapshot->CumulativeDataWeight)
            .Item("trimmed_data_weight").Value(snapshot->TrimmedDataWeight)
            .Item("available_data_weight").Value(snapshot->AvailableDataWeight)
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

void BuildSubConsumerStatusYson(const TSubConsumerSnapshotPtr& subSnapshot, TFluentAny fluent)
{
    if (!subSnapshot->Error.IsOK()) {
        fluent
            .BeginMap()
                .Item("error").Value(subSnapshot->Error)
            .EndMap();
        return;
    }

    fluent
        .BeginMap()
            .Item("partition_count").Value(subSnapshot->PartitionCount)
            .Item("read_row_count_rate").Do(std::bind(BuildEmaCounterYson, subSnapshot->ReadRate.RowCount, _1))
            .Item("read_data_weight_rate").Do(std::bind(BuildEmaCounterYson, subSnapshot->ReadRate.DataWeight, _1))
        .EndMap();
}

void BuildConsumerStatusYson(const TConsumerSnapshotPtr& snapshot, TFluentAny fluent)
{
    if (!snapshot->Error.IsOK()) {
        fluent
            .BeginMap()
                .Item("queue_agent_host").Value(GetLocalHostName())
                .Item("error").Value(snapshot->Error)
            .EndMap();
        return;
    }

    fluent
        .BeginMap()
            .Item("queue_agent_host").Value(GetLocalHostName())
            .Item("registrations").DoListFor(snapshot->Registrations, BuildRegistrationYson)
            .Item("queues").DoMapFor(snapshot->SubSnapshots, [] (TFluentMap fluent, auto pair) {
                const auto& queueRef = pair.first;
                const auto& subSnapshot = pair.second;
                fluent
                    .Item(ToString(queueRef)).Do(std::bind(BuildSubConsumerStatusYson, subSnapshot, _1));
            })

        .EndMap();
}

void BuildSubConsumerPartitionYson(const TConsumerPartitionSnapshotPtr& snapshot, TFluentAny fluent)
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
            .Item("unread_data_weight").Value(snapshot->UnreadDataWeight)
            .Item("next_row_commit_time").Value(snapshot->NextRowCommitTime)
            .Item("processing_lag").Value(snapshot->ProcessingLag)
            .Item("consume_idle_time").Value(snapshot->ConsumeIdleTime)
            .Item("cumulative_data_weight").Value(snapshot->CumulativeDataWeight)
            .Item("read_row_count_rate").Do(std::bind(BuildEmaCounterYson, snapshot->ReadRate.RowCount, _1))
            .Item("read_data_weight_rate").Do(std::bind(BuildEmaCounterYson, snapshot->ReadRate.DataWeight, _1))
        .EndMap();
}

void BuildSubConsumerPartitionListYson(const TSubConsumerSnapshotPtr& subSnapshot, TFluentAny fluent)
{
    if (!subSnapshot->Error.IsOK()) {
        fluent
            .BeginList()
            .EndList();
        return;
    }

    fluent
        .BeginList()
            .DoFor(subSnapshot->PartitionSnapshots, [&] (TFluentList fluent, const TConsumerPartitionSnapshotPtr& partitionSnapshot) {
                fluent
                    .Item().Do(std::bind(BuildSubConsumerPartitionYson, partitionSnapshot, _1));
            })
        .EndList();
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
        .DoMapFor(snapshot->SubSnapshots, [&] (TFluentMap fluent, auto pair) {
            const auto& queueRef = pair.first;
            const auto& subSnapshot = pair.second;
            fluent
                .Item(ToString(queueRef)).Do(std::bind(BuildSubConsumerPartitionListYson, subSnapshot, _1));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
