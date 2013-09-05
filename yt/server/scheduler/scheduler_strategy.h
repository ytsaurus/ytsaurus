#pragma once

#include "public.h"

#include <ytlib/actions/signal.h>

#include <ytlib/yson/public.h>

#include <ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulerStrategyHost
{
    virtual ~ISchedulerStrategyHost()
    { }

    DECLARE_INTERFACE_SIGNAL(void(TOperationPtr), OperationRegistered);
    DECLARE_INTERFACE_SIGNAL(void(TOperationPtr), OperationUnregistered);

    DECLARE_INTERFACE_SIGNAL(void(TJobPtr job), JobStarted);
    DECLARE_INTERFACE_SIGNAL(void(TJobPtr job), JobFinished);
    DECLARE_INTERFACE_SIGNAL(void(TJobPtr job, const NNodeTrackerClient::NProto::TNodeResources& resourcesDelta), JobUpdated);

    virtual TMasterConnector* GetMasterConnector() = 0;

    virtual NNodeTrackerClient::NProto::TNodeResources GetTotalResourceLimits() = 0;
    virtual std::vector<TExecNodePtr> GetExecNodes() = 0;
    virtual int GetExecNodeCount() = 0;

};

struct ISchedulerStrategy
{
    virtual ~ISchedulerStrategy()
    { }

    virtual void ScheduleJobs(ISchedulingContext* context) = 0;

    //! Builds a YSON structure reflecting the state of the operation to be put into Cypress.
    virtual void BuildOperationProgressYson(
        TOperationPtr operation,
        NYson::IYsonConsumer* consumer) = 0;

    //! Builds a YSON structure reflecting the state of the scheduler to be displayed in Orchid.
    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) = 0;

    //! Provides a string describing operation status and statistics.
    virtual Stroka GetOperationLoggingProgress(TOperationPtr operation) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
