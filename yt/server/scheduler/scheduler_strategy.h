#pragma once

#include "public.h"
#include "event_log.h"

#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/core/actions/signal.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/permission.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulerStrategyHost
    : public virtual IEventLogHost
{
    virtual ~ISchedulerStrategyHost() = default;

    DECLARE_INTERFACE_SIGNAL(void(const TOperationPtr& operation), OperationRegistered);
    DECLARE_INTERFACE_SIGNAL(void(const TOperationPtr& operation), OperationUnregistered);
    DECLARE_INTERFACE_SIGNAL(void(const TOperationPtr& operation, const NYTree::INodePtr& update), OperationRuntimeParamsUpdated);

    DECLARE_INTERFACE_SIGNAL(void(const TJobPtr& job), JobFinished);
    DECLARE_INTERFACE_SIGNAL(void(const TJobPtr& job, const TJobResources& resourcesDelta), JobUpdated);

    DECLARE_INTERFACE_SIGNAL(void(const NYTree::INodePtr& pools), PoolsUpdated);

    virtual TJobResources GetTotalResourceLimits() = 0;
    virtual TJobResources GetResourceLimits(const TNullable<Stroka>& tag) = 0;

    virtual void ActivateOperation(const TOperationId& operationId) = 0;

    virtual std::vector<TExecNodeDescriptor> GetExecNodeDescriptors(const TNullable<Stroka>& tag) const = 0;
    virtual int GetExecNodeCount() const = 0;
    virtual int GetTotalNodeCount() const = 0;

    virtual TFuture<void> CheckPoolPermission(
        const NYPath::TYPath& path,
        const Stroka& user,
        NYTree::EPermission permission) = 0;
};

struct ISchedulerStrategy
    : public virtual TRefCounted
{
    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) = 0;

    //! Starts periodic updates and logging.
    virtual void StartPeriodicActivity() = 0;

    //! Called periodically to build new tree snapshot.
    virtual void OnFairShareUpdateAt(TInstant now) = 0;

    //! Called periodically to log scheduling tree state.
    virtual void OnFairShareLoggingAt(TInstant now) = 0;

    //! Resets memoized state.
    virtual void ResetState() = 0;

    //! Validates that operation can be started.
    /*!
     *  In particular, the following checks are performed:
     *  1) Limits for the number of concurrent operations are validated.
     *  2) Pool permissions are validated.
     */
    virtual TFuture<void> ValidateOperationStart(const TOperationPtr& operation) = 0;

    //! Retrieves operation job scheduling timings statistics.
    virtual NJobTrackerClient::TStatistics GetOperationTimeStatistics(const TOperationId& operationId) = 0;

    //! Builds a YSON structure containing a set of attributes to be assigned to operation's node
    //! in Cypress during creation.
    virtual void BuildOperationAttributes(
        const TOperationId& operationId,
        NYson::IYsonConsumer* consumer) = 0;

    //! Builds a YSON structure reflecting operation's progress.
    //! This progress is periodically pushed into Cypress and is also displayed via Orchid.
    virtual void BuildOperationProgress(
        const TOperationId& operationId,
        NYson::IYsonConsumer* consumer) = 0;

    //! Similar to #BuildOperationProgress but constructs a reduced version to used by UI.
    virtual void BuildBriefOperationProgress(
        const TOperationId& operationId,
        NYson::IYsonConsumer* consumer) = 0;

    //! Builds a YSON structure reflecting the state of the scheduler to be displayed in Orchid.
    virtual void BuildOrchid(NYson::IYsonConsumer* consumer) = 0;

    //! Provides a string describing operation status and statistics.
    virtual Stroka GetOperationLoggingProgress(const TOperationId& operationId) = 0;

    //! Called for a just initialized operation to construct its brief spec
    //! to be used by UI.
    virtual void BuildBriefSpec(
        const TOperationId& operationId,
        NYson::IYsonConsumer* consumer) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulerStrategy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
