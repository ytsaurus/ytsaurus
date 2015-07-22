#pragma once

#include <core/misc/enum.h>
#include <core/misc/error.h>
#include <core/misc/protobuf_helpers.h>

#include <ytlib/scheduler/public.h>

#include <ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

////////////////////////////////////////////////////////////////////////////////

class TSchedulerService;
typedef TIntrusivePtr<TSchedulerService> TSchedulerServicePtr;

class TSchedulerServiceProxy;

class TOperation;
typedef TIntrusivePtr<TOperation> TOperationPtr;

class TJob;
typedef TIntrusivePtr<TJob> TJobPtr;

typedef std::list<TJobPtr> TJobList;

class TExecNode;
typedef TIntrusivePtr<TExecNode> TExecNodePtr;

class TFairShareStrategyConfig;
typedef TIntrusivePtr<TFairShareStrategyConfig> TFairShareStrategyConfigPtr;

class TEventLogConfig;
typedef TIntrusivePtr<TEventLogConfig> TEventLogConfigPtr;

class TSimpleOperationOptions;
typedef TIntrusivePtr<TSimpleOperationOptions> TSimpleOperationOptionsPtr;

class TMapOperationOptions;
typedef TIntrusivePtr<TMapOperationOptions> TMapOperationOptionsPtr;

class TUnorderedMergeOperationOptions;
typedef TIntrusivePtr<TUnorderedMergeOperationOptions> TUnorderedMergeOperationOptionsPtr;

class TReduceOperationOptions;
typedef TIntrusivePtr<TReduceOperationOptions> TReduceOperationOptionsPtr;

class TEraseOperationOptions;
typedef TIntrusivePtr<TEraseOperationOptions> TEraseOperationOptionsPtr;

class TOrderedMergeOperationOptions;
typedef TIntrusivePtr<TOrderedMergeOperationOptions> TOrderedMergeOperationOptionsPtr;

class TSortedMergeOperationOptions;
typedef TIntrusivePtr<TSortedMergeOperationOptions> TSortedMergeOperationOptionsPtr;

class TSortOperationOptionsBase;
typedef TIntrusivePtr<TSortOperationOptionsBase> TSortOperationOptionsBasePtr;

class TMapReduceOperationOptions;
typedef TIntrusivePtr<TMapReduceOperationOptions> TMapReduceOperationOptionsPtr;

class TSortOperationOptions;
typedef TIntrusivePtr<TSortOperationOptions> TSortOperationOptionsPtr;

class TRemoteCopyOperationOptions;
typedef TIntrusivePtr<TRemoteCopyOperationOptions> TRemoteCopyOperationOptionsPtr;

class TSchedulerConfig;
typedef TIntrusivePtr<TSchedulerConfig> TSchedulerConfigPtr;

class TScheduler;
typedef TIntrusivePtr<TScheduler> TSchedulerPtr;

struct IEventLogHost;

struct ISchedulerStrategy;
struct ISchedulerStrategyHost;

struct IOperationHost;
struct ISchedulingContext;

struct IOperationController;
typedef TIntrusivePtr<IOperationController> IOperationControllerPtr;

class TMasterConnector;

using TRefCountedJobResult = TRefCountedProto<NJobTrackerClient::NProto::TJobResult>;
typedef TIntrusivePtr<TRefCountedJobResult> TRefCountedJobResultPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
