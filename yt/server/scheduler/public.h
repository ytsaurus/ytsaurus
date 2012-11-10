#pragma once

#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ESchedulerStrategy,
    (Null)
    (FairShare)
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

struct TFairShareStrategyConfig;
typedef TIntrusivePtr<TFairShareStrategyConfig> TFairShareStrategyConfigPtr;

struct TSchedulerConfig;
typedef TIntrusivePtr<TSchedulerConfig> TSchedulerConfigPtr;

class TScheduler;
typedef TIntrusivePtr<TScheduler> TSchedulerPtr;

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
