#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TJobId;
typedef TGuid TOperationId;

DECLARE_ENUM(EOperationType,
    ((Map)(0))
    ((Merge)(1))
    ((Erase)(2))
    ((Sort)(3))
    ((Reduce)(4))
    ((MapReduce)(5))
);

DECLARE_ENUM(EJobType,
    ((Map)(0))
    ((PartitionMap)(1))

    ((SortedMerge)(2))
    ((OrderedMerge)(3))
    ((UnorderedMerge)(4))

    ((Partition)(5))

    ((SimpleSort)(6))
    ((PartitionSort)(7))

    ((SortedReduce)(8))
    ((PartitionReduce)(9))
);

DECLARE_ENUM(EOperationState,
    (Initializing)
    (Preparing)
    (Reviving)
    (Running)
    (Completing)
    (Completed)
    (Aborting)
    (Aborted)
    (Failing)
    (Failed)
);

DECLARE_ENUM(EJobState,
    ((Running)(0))
    ((Aborting)(1))
    ((Completed)(2))
    ((Failed)(3))
    ((Aborted)(4))
    ((Waiting)(5))
);

DECLARE_ENUM(EJobPhase,
    ((Created)(0))
    ((PreparingConfig)(1))
    ((PreparingProxy)(2))

    ((PreparingSandbox)(10))

    ((StartedProxy)(50))
    ((StartedJob)(51))
    ((FinishedJob)(52))

    ((Cleanup)(80))

    ((Completed)(101))
    ((Failed)(102))
);

class TSchedulerServiceProxy;

struct TJobIOConfig;
typedef TIntrusivePtr<TJobIOConfig> TJobIOConfigPtr;

struct TOperationSpecBase;
typedef TIntrusivePtr<TOperationSpecBase> TOperationSpecBasePtr;

struct TUserJobSpec;
typedef TIntrusivePtr<TUserJobSpec> TUserJobSpecPtr;

struct  TMapOperationSpec;
typedef TIntrusivePtr<TMapOperationSpec> TMapOperationSpecPtr;

struct TMergeOperationSpecBase;
typedef TIntrusivePtr<TMergeOperationSpecBase> TMergeOperationSpecBasePtr;

struct TMergeOperationSpec;
typedef TIntrusivePtr<TMergeOperationSpec> TMergeOperationSpecPtr;

struct TUnorderedMergeOperationSpec;
typedef TIntrusivePtr<TUnorderedMergeOperationSpec> TUnorderedMergeOperationSpecPtr;

struct TOrderedMergeOperationSpec;
typedef TIntrusivePtr<TOrderedMergeOperationSpec> TOrderedMergeOperationSpecPtr;

struct TSortedMergeOperationSpec;
typedef TIntrusivePtr<TSortedMergeOperationSpec> TSortedMergeOperationSpecPtr;

struct TEraseOperationSpec;
typedef TIntrusivePtr<TEraseOperationSpec> TEraseOperationSpecPtr;

struct TReduceOperationSpec;
typedef TIntrusivePtr<TReduceOperationSpec> TReduceOperationSpecPtr;

struct TSortOperationSpecBase;
typedef TIntrusivePtr<TSortOperationSpecBase> TSortOperationSpecBasePtr;

struct TSortOperationSpec;
typedef TIntrusivePtr<TSortOperationSpec> TSortOperationSpecPtr;

struct TMapReduceOperationSpec;
typedef TIntrusivePtr<TMapReduceOperationSpec> TMapReduceOperationSpecPtr;

struct TPoolConfig;
typedef TIntrusivePtr<TPoolConfig> TPoolConfigPtr;

struct TPooledOperationSpec;
typedef TIntrusivePtr<TPooledOperationSpec> TPooledOperationSpecPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
