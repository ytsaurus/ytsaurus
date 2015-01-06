#pragma once

#include <core/misc/enum.h>
#include <core/misc/error.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
