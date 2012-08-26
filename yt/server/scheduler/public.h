#pragma once

#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

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
struct ISchedulingContext;

struct IOperationController;
typedef TIntrusivePtr<IOperationController> IOperationControllerPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
