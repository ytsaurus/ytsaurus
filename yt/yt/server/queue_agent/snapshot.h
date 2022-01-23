#pragma once

#include "dynamic_state.h"

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Snapshot of a queue.
struct TQueueSnapshot
    : public TRefCounted
{
    TQueueTableRow Row;

    TError Error;

    EQueueFamily Family;
    int PartitionCount = 0;

    std::vector<TQueuePartitionSnapshotPtr> PartitionSnapshots;
    THashMap<TCrossClusterReference, TConsumerSnapshotPtr> ConsumerSnapshots;
};

DEFINE_REFCOUNTED_TYPE(TQueueSnapshot);

////////////////////////////////////////////////////////////////////////////////

//! Snapshot of a partition within queue.
struct TQueuePartitionSnapshot
    : public TRefCounted
{
    TError Error;

    i64 LowerRowIndex = -1;
    i64 UpperRowIndex = -1;
    i64 AvailableRowCount = -1;
    std::optional<TInstant> LastRowCommitTime;
    std::optional<TDuration> CommitIdleTime;
};

DEFINE_REFCOUNTED_TYPE(TQueuePartitionSnapshot);

////////////////////////////////////////////////////////////////////////////////

//! Snapshot of a consumer.
struct TConsumerSnapshot
    : public TRefCounted
{
    TConsumerTableRow Row;

    TError Error;

    TCrossClusterReference TargetQueue;
    bool Vital = false;

    TString Owner;
    i64 PartitionCount = 0;

    std::vector<TConsumerPartitionSnapshotPtr> PartitionSnapshots;
};

DEFINE_REFCOUNTED_TYPE(TConsumerSnapshot);

////////////////////////////////////////////////////////////////////////////////

//! Snapshot of a partition within consumer.
struct TConsumerPartitionSnapshot
    : public TRefCounted
{
    TError Error;

    i64 NextRowIndex = -1;
    i64 UnreadRowCount = -1;
    std::optional<TInstant> NextRowCommitTime;
    std::optional<TDuration> ProcessingLag;
    std::optional<TInstant> LastConsumeTime;
    std::optional<TDuration> ConsumeIdleTime;
};

DEFINE_REFCOUNTED_TYPE(TConsumerPartitionSnapshot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
