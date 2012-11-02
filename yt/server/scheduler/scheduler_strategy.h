#pragma once

#include "public.h"

#include <ytlib/actions/signal.h>

#include <ytlib/scheduler/job.pb.h>

#include <ytlib/yson/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulerStrategyHost
{
    virtual ~ISchedulerStrategyHost()
    { }

    DECLARE_INTERFACE_SIGNAL(void(TOperationPtr), OperationStarted);
    DECLARE_INTERFACE_SIGNAL(void(TOperationPtr), OperationFinished);

    DECLARE_INTERFACE_SIGNAL(void(TJobPtr), JobStarted);
    DECLARE_INTERFACE_SIGNAL(void(TJobPtr), JobFinished);

    virtual TMasterConnector* GetMasterConnector() = 0;

    virtual NProto::TNodeResources GetTotalResourceLimits() = 0;
    virtual std::vector<TExecNodePtr> GetExecNodes() = 0;

};

struct ISchedulerStrategy
{
    virtual ~ISchedulerStrategy()
    { }

    virtual void ScheduleJobs(ISchedulingContext* context) = 0;
    virtual void BuildOperationProgressYson(TOperationPtr operation, NYson::IYsonConsumer* consumer) = 0;
    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
