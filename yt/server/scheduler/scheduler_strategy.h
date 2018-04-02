#pragma once

#include "public.h"
#include "event_log.h"
#include "job_metrics.h"

#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/ytlib/scheduler/job_resources.h>

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

    virtual TJobResources GetTotalResourceLimits() = 0;
    virtual TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) = 0;
    virtual std::vector<NNodeTrackerClient::TNodeId> GetExecNodeIds(const TSchedulingTagFilter& filter) const = 0;
    virtual TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(const TSchedulingTagFilter& filter) const = 0;

    virtual TInstant GetConnectionTime() const = 0;

    virtual void ActivateOperation(const TOperationId& operationId) = 0;
    virtual void AbortOperation(const TOperationId& operationId, const TError& error) = 0;

    virtual TMemoryDistribution GetExecNodeMemoryDistribution(const TSchedulingTagFilter& filter) const = 0;

    virtual void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const = 0;

    virtual void SetSchedulerAlert(
        ESchedulerAlertType alertType,
        const TError& alert) = 0;

    virtual TFuture<void> SetOperationAlert(
        const TOperationId& operationId,
        EOperationAlertType alertType,
        const TError& alert,
        TNullable<TDuration> timeout = Null) = 0;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobUpdateStatus,
    (Running)
    (Finished)
);

struct TJobUpdate
{
    EJobUpdateStatus Status;
    TOperationId OperationId;
    TJobId JobId;
    TString TreeId;
    TJobResources Delta;
};

////////////////////////////////////////////////////////////////////////////////

struct ISchedulerStrategy
    : public virtual TRefCounted
{
    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) = 0;

    //! Starts periodic updates and logging.
    virtual void OnMasterConnected() = 0;

    //! Stops all activities, resets all state.
    virtual void OnMasterDisconnected() = 0;

    //! Called periodically to build new tree snapshot.
    virtual void OnFairShareUpdateAt(TInstant now) = 0;

    //! Called periodically to log scheduling tree state.
    virtual void OnFairShareLoggingAt(TInstant now) = 0;

    //! Called periodically to log essential for simulator tree state.
    virtual void OnFairShareEssentialLoggingAt(TInstant now) = 0;

    //! Called periodically to update min needed job resources for operation.
    virtual void OnMinNeededJobResourcesUpdate() = 0;

    //! Validates that operation can be started.
    /*!
     *  In particular, the following checks are performed:
     *  1) Limits for the number of concurrent operations are validated.
     *  2) Pool permissions are validated.
     */
    virtual TFuture<void> ValidateOperationStart(const IOperationStrategyHost* operation) = 0;

    //! Validates that operation can be registered without errors.
    /*!
     *  Checks limits for the number of concurrent operations.
     *
     *  The implementation must be synchronous.
     */
    virtual void ValidateOperationCanBeRegistered(const IOperationStrategyHost* operation) = 0;

    //! Register operation in strategy.
    /*!
     *  The implementation must throw no exceptions.
     */
    virtual void RegisterOperation(IOperationStrategyHost* operation) = 0;

    //! Disable operation. Remove all operation jobs from tree.
    /*!
     *  The implementation must throw no exceptions.
     */
    virtual void DisableOperation(IOperationStrategyHost* operation) = 0;

    //! Must be called for a registered operation after it is materialized.
    virtual void EnableOperation(IOperationStrategyHost* operation) = 0;

    //! Unregister operation in strategy.
    /*!
     *  The implementation must throw no exceptions.
     */
    virtual void UnregisterOperation(IOperationStrategyHost* operation) = 0;

    //! Register jobs that are already created somewhere outside strategy.
    virtual void RegisterJobs(const TOperationId& operationId, const std::vector<TJobPtr>& job) = 0;

    virtual void ProcessJobUpdates(
        const std::vector<TJobUpdate>& jobUpdates,
        std::vector<TJobId>* successfullyUpdatedJobs,
        std::vector<TJobId>* jobsToAbort) = 0;

    virtual void ApplyJobMetricsDelta(const TOperationIdToOperationJobMetrics& operationIdToOperationJobMetrics) = 0;

    virtual void UpdatePoolTrees(const NYTree::INodePtr& poolTreesNode) = 0;

    virtual void ValidateNodeTags(const THashSet<TString>& tags) = 0;

    virtual void UpdateOperationRuntimeParameters(IOperationStrategyHost* operation) = 0;

    virtual void UpdateOperationRuntimeParameters(
        IOperationStrategyHost* operation,
        const TOperationFairShareStrategyTreeOptionsPtr& runtimeParams) = 0;

    //! Updates current config used by strategy.
    virtual void UpdateConfig(const TFairShareStrategyConfigPtr& config) = 0;

    //! Builds a YSON structure containing a set of attributes to be assigned to operation's node
    //! in Cypress during creation.
    virtual void BuildOperationAttributes(
        const TOperationId& operationId,
        NYTree::TFluentMap fluent) = 0;

    //! Builds a YSON structure containing a set of runtime operation params.
    virtual void BuildOperationRuntimeParams(
        const TOperationId& operationId,
        const TOperationStrategyRuntimeParametersPtr& runtimeParams,
        NYTree::TFluentMap fluent) = 0;

    //! Builds a YSON map fragment with strategy specific information about operation
    //! that used for event log.
    virtual void BuildOperationInfoForEventLog(
        const IOperationStrategyHost* operation,
        NYTree::TFluentMap fluent) = 0;

    //! Builds a YSON structure reflecting operation's progress.
    //! This progress is periodically pushed into Cypress and is also displayed via Orchid.
    virtual void BuildOperationProgress(
        const TOperationId& operationId,
        NYTree::TFluentMap fluent) = 0;

    //! Similar to #BuildOperationProgress but constructs a reduced version to be used by UI.
    virtual void BuildBriefOperationProgress(
        const TOperationId& operationId,
        NYTree::TFluentMap fluent) = 0;

    //! Builds a YSON structure reflecting the state of the scheduler to be displayed in Orchid.
    virtual void BuildOrchid(NYTree::TFluentMap fluent) = 0;

    //! Provides a string describing operation status and statistics.
    virtual TString GetOperationLoggingProgress(const TOperationId& operationId) = 0;

    //! Called for a just initialized operation to construct its brief spec
    //! to be used by UI.
    virtual void BuildBriefSpec(
        const TOperationId& operationId,
        NYTree::TFluentMap fluent) = 0;

    virtual std::vector<TSchedulingTagFilter> GetOperationPoolTreeSchedulingTagFilters(const TOperationId& operationId) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulerStrategy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
