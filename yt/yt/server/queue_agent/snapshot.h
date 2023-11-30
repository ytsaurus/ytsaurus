#pragma once

#include "performance_counters.h"
#include "private.h"

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Snapshot of a queue.
struct TQueueSnapshot
    : public TRefCounted
{
    NQueueClient::TQueueTableRow Row;
    std::optional<NQueueClient::TReplicatedTableMappingTableRow> ReplicatedTableMappingRow;

    TError Error;

    EQueueFamily Family;
    int PartitionCount = 0;

    //! Total write counters over all partitions.
    TPerformanceCounters WriteRate;

    bool HasTimestampColumn = false;
    bool HasCumulativeDataWeightColumn = false;

    i64 PassIndex = 0;
    TInstant PassInstant;

    std::vector<TQueuePartitionSnapshotPtr> PartitionSnapshots;
    std::vector<NQueueClient::TConsumerRegistrationTableRow> Registrations;
};

DEFINE_REFCOUNTED_TYPE(TQueueSnapshot)

////////////////////////////////////////////////////////////////////////////////

//! Snapshot of a partition within queue.
struct TQueuePartitionSnapshot
    : public TRefCounted
{
    TError Error;

    // Fields below are not set if error is set.
    i64 LowerRowIndex = -1;
    i64 UpperRowIndex = -1;
    i64 AvailableRowCount = -1;
    TInstant LastRowCommitTime;
    TDuration CommitIdleTime;

    std::optional<i64> CumulativeDataWeight;
    // Currently, this value is an approximation and includes the weight of the first row of the snapshot.
    std::optional<i64> TrimmedDataWeight;
    // Currently, this value does not include the first available row.
    std::optional<i64> AvailableDataWeight;

    //! Write counters for the given partition.
    TPerformanceCounters WriteRate;

    //! Meta-information specific to given queue family.
    NYson::TYsonString Meta;

    NTabletClient::ETabletState TabletState;
};

DEFINE_REFCOUNTED_TYPE(TQueuePartitionSnapshot)

////////////////////////////////////////////////////////////////////////////////

//! Snapshot of a consumer.
struct TConsumerSnapshot
    : public TRefCounted
{
    // This field is always set.
    NQueueClient::TConsumerTableRow Row;
    std::optional<NQueueClient::TReplicatedTableMappingTableRow> ReplicatedTableMappingRow;

    TError Error;

    i64 PassIndex = 0;
    TInstant PassInstant;

    std::vector<NQueueClient::TConsumerRegistrationTableRow> Registrations;
    THashMap<NQueueClient::TCrossClusterReference, TSubConsumerSnapshotPtr> SubSnapshots;
};

DEFINE_REFCOUNTED_TYPE(TConsumerSnapshot)

////////////////////////////////////////////////////////////////////////////////

// Snapshot of a subconsumer corresponding to a particular queue, for which consumer is registered.
struct TSubConsumerSnapshot
    : public TRefCounted
{
    int PartitionCount = 0;
    TPerformanceCounters ReadRate;

    TError Error;

    //! Temporary information for debugging. Equal to the same value of the corresponding queue.
    //! COMPAT(achulkov2): Remove this.
    bool HasCumulativeDataWeightColumn = false;

    std::vector<TConsumerPartitionSnapshotPtr> PartitionSnapshots;
};

DEFINE_REFCOUNTED_TYPE(TSubConsumerSnapshot)

////////////////////////////////////////////////////////////////////////////////

//! Snapshot of a partition within consumer.
struct TConsumerPartitionSnapshot
    : public TRefCounted
{
    // The field below is effectively the error of the corresponding queue partition.
    TError Error;

    // Fields below are always set.
    i64 NextRowIndex = -1;
    TInstant LastConsumeTime;
    TDuration ConsumeIdleTime;

    // Fields below are not set if error is set (as they depend on the unavailable information on the queue partition).

    EConsumerPartitionDisposition Disposition = EConsumerPartitionDisposition::None;
    //! Offset of the next row with respect to the upper row index in the partition.
    //! May be negative if the consumer is ahead of the partition.
    i64 UnreadRowCount = -1;
    //! Amount of data unread by the consumer. std::nullopt if the consumer is aheaed of the partition, expired or "almost expired".
    std::optional<i64> UnreadDataWeight;
    //! If #Disposition == PendingConsumption and commit timestamp is set up, the commit timestamp of the next row to be read by the consumer;
    //! std::nullopt otherwise.
    std::optional<TInstant> NextRowCommitTime;
    //! If #NextRowCommitTime is set, difference between Now() and *NextRowCommitTime; zero otherwise.
    TDuration ProcessingLag;

    std::optional<i64> CumulativeDataWeight;

    //! Read counters of the given consumer for the partition.
    TPerformanceCounters ReadRate;
};

DEFINE_REFCOUNTED_TYPE(TConsumerPartitionSnapshot)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
