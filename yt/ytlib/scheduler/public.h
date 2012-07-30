#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>
#include <ytlib/transaction_server/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NTransactionServer::TTransactionId;

typedef TGuid TJobId;
typedef TGuid TOperationId;

DECLARE_ENUM(EOperationType,
    ((Map)(0))
    ((Merge)(1))
    ((Erase)(2))
    ((Sort)(3))
    ((Reduce)(4))
);

DECLARE_ENUM(EJobType,
    ((Map)(0))
    ((SortedMerge)(1))
    ((OrderedMerge)(2))
    ((UnorderedMerge)(7))
    ((Partition)(3))
    ((PartitionSort)(4))
    ((SimpleSort)(5))
    ((Reduce)(6))
);

DECLARE_ENUM(EOperationState,
    (Initializing)
    (Preparing)
    (Reviving)
    (Running)
    (Completed)
    (Aborted)
    (Failed)
);

DECLARE_ENUM(EJobState,
    ((Running)(0))
    ((Aborting)(1))
    ((Completed)(2))
    ((Failed)(3))
    ((Aborted)(4))
);

DECLARE_ENUM(EJobProgress,
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

DECLARE_ENUM(ESchedulerStrategy,
    (Null)
    (Fifo)
);

class TSchedulerService;
typedef TIntrusivePtr<TSchedulerService> TSchedulerServicePtr;

class TSchedulerServiceProxy;

class TOperation;
typedef TIntrusivePtr<TOperation> TOperationPtr;

class TJob;
typedef TIntrusivePtr<TJob> TJobPtr;

class TExecNode;
typedef TIntrusivePtr<TExecNode> TExecNodePtr;

class TSchedulerConfig;
typedef TIntrusivePtr<TSchedulerConfig> TSchedulerConfigPtr;

class TScheduler;
typedef TIntrusivePtr<TScheduler> TSchedulerPtr;

struct ISchedulerStrategy;

struct IOperationHost;

struct IOperationController;
typedef TIntrusivePtr<IOperationController> IOperationControllerPtr;

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

struct TEraseOperationSpec;
typedef TIntrusivePtr<TEraseOperationSpec> TEraseOperationSpecPtr;

struct TSortOperationSpec;
typedef TIntrusivePtr<TSortOperationSpec> TSortOperationSpecPtr;

struct TReduceOperationSpec;
typedef TIntrusivePtr<TReduceOperationSpec> TReduceOperationSpecPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
