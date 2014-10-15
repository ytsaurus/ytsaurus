#pragma once

#include "public.h"
#include "event_log.h"

#include <core/actions/signal.h>

#include <core/yson/public.h>

#include <core/ytree/public.h>

#include <ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulerStrategyHost
    : public virtual TEventLogHostBase
{
    virtual ~ISchedulerStrategyHost()
    { }

    DECLARE_INTERFACE_SIGNAL(void(TOperationPtr), OperationRegistered);
    DECLARE_INTERFACE_SIGNAL(void(TOperationPtr), OperationUnregistered);
    DECLARE_INTERFACE_SIGNAL(void(TOperationPtr, NYTree::INodePtr update), OperationRuntimeParamsUpdated);

    DECLARE_INTERFACE_SIGNAL(void(TJobPtr job), JobStarted);
    DECLARE_INTERFACE_SIGNAL(void(TJobPtr job), JobFinished);
    DECLARE_INTERFACE_SIGNAL(void(TJobPtr job, const NNodeTrackerClient::NProto::TNodeResources& resourcesDelta), JobUpdated);
    
    DECLARE_INTERFACE_SIGNAL(void(NYTree::INodePtr pools), PoolsUpdated);

    virtual NNodeTrackerClient::NProto::TNodeResources GetTotalResourceLimits() = 0;
    virtual NNodeTrackerClient::NProto::TNodeResources
        GetResourceLimits(const TNullable<Stroka>& schedulingTag) = 0;
    virtual std::vector<TExecNodePtr> GetExecNodes() const = 0;
    virtual int GetExecNodeCount() const = 0;
};

struct ISchedulerStrategy
{
    virtual ~ISchedulerStrategy()
    { }

    virtual void ScheduleJobs(ISchedulingContext* context) = 0;

    //! Builds a YSON structure containing a set of attributes to be assigned to operation's node
    //! in Cypress during creation.
    virtual void BuildOperationAttributes(
        TOperationPtr operation,
        NYson::IYsonConsumer* consumer) = 0;

    //! Builds a YSON structure reflecting operation's progress.
    //! This progress is periodically pushed into Cypress and is also displayed via Orchid.
    virtual void BuildOperationProgress(
        TOperationPtr operation,
        NYson::IYsonConsumer* consumer) = 0;

    //! Similar to #BuildOperationProgress but constructs a reduced version to used by UI.
    virtual void BuildBriefOperationProgress(
        TOperationPtr operation,
        NYson::IYsonConsumer* consumer) = 0;

    //! Builds a YSON structure reflecting the state of the scheduler to be displayed in Orchid.
    virtual void BuildOrchid(NYson::IYsonConsumer* consumer) = 0;

    //! Provides a string describing operation status and statistics.
    virtual Stroka GetOperationLoggingProgress(TOperationPtr operation) = 0;

    //! Called for a just initialized operation to construct its brief spec
    //! to be used by UI.
    virtual void BuildBriefSpec(
        TOperationPtr operation,
        NYson::IYsonConsumer* consumer) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
