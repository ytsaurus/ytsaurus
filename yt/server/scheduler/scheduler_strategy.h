#pragma once

#include "public.h"

#include <yt/server/lib/scheduler/event_log.h>
#include <yt/server/lib/scheduler/job_metrics.h>
#include <yt/server/lib/scheduler/structs.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/actions/signal.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/permission.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulerStrategyHost
    : public virtual IEventLogHost
{
    virtual ~ISchedulerStrategyHost() = default;

    virtual IInvokerPtr GetControlInvoker(EControlQueue queue) const = 0;
    virtual IInvokerPtr GetFairShareLoggingInvoker() const = 0;
    virtual IInvokerPtr GetFairShareProfilingInvoker() const = 0;
    virtual IInvokerPtr GetFairShareUpdateInvoker() const = 0;

    virtual NEventLog::TFluentLogEvent LogFairShareEventFluently(TInstant now) = 0;

    virtual void Disconnect(const TError& error) = 0;

    virtual TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) = 0;
    virtual std::vector<NNodeTrackerClient::TNodeId> GetExecNodeIds(const TSchedulingTagFilter& filter) const = 0;
    virtual TString GetExecNodeAddress(NNodeTrackerClient::TNodeId nodeId) const = 0;
    virtual TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(const TSchedulingTagFilter& filter) const = 0;
    
    virtual void UpdateNodesOnChangedTrees(const THashMap<TString, TSchedulingTagFilter>& treeIdToFilter) = 0;

    virtual TString FormatResources(const TJobResourcesWithQuota& resources) const = 0;
    virtual TString FormatResourceUsage(
        const TJobResources& usage,
        const TJobResources& limits,
        const NNodeTrackerClient::NProto::TDiskResources& diskResources) const = 0;

    virtual TInstant GetConnectionTime() const = 0;

    virtual void ActivateOperation(TOperationId operationId) = 0;
    virtual void AbortOperation(TOperationId operationId, const TError& error) = 0;
    virtual void FlushOperationNode(TOperationId operationId) = 0;

    virtual TMemoryDistribution GetExecNodeMemoryDistribution(const TSchedulingTagFilter& filter) const = 0;

    virtual void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const = 0;

    virtual void SetSchedulerAlert(
        ESchedulerAlertType alertType,
        const TError& alert) = 0;

    virtual TFuture<void> SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout = std::nullopt) = 0;
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

    virtual void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext) = 0;

    //! Starts periodic updates and logging.
    virtual void OnMasterConnected() = 0;

    //! Stops all activities, resets all state.
    virtual void OnMasterDisconnected() = 0;

    //! Called periodically to collect the metrics of tree elements.
    virtual void OnFairShareProfilingAt(TInstant now) = 0;

    //! Called periodically to build new tree snapshots.
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

    //! Returns limit validation errors for each pool specified in #runtimeParameters
    /*!
     *  Checks limits for the number of concurrent operations.
     *  For each tree with limit violations returns corresponding error.
     *
     *  The implementation must be synchronous.
     */
    virtual THashMap<TString, TError> GetPoolLimitViolations(
        const IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters) = 0;

    //! Register operation in strategy.
    /*!
     *  The implementation must throw no exceptions.
     */
    virtual void RegisterOperation(IOperationStrategyHost* operation, std::vector<TString>* unknownTreeIds) = 0;

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

    virtual void UnregisterOperationFromTree(TOperationId operationId, const TString& treeId) = 0;

    //! Register jobs that are already created somewhere outside strategy.
    virtual void RegisterJobsFromRevivedOperation(TOperationId operationId, const std::vector<TJobPtr>& job) = 0;

    //! Out of the pool trees specified for the operation, choose one most suitable tree
    //! depending on the operation's demand and current resource usage in each tree.
    virtual TString ChooseBestSingleTreeForOperation(TOperationId operationId, TJobResources newDemand) = 0;

    virtual void ProcessJobUpdates(
        const std::vector<TJobUpdate>& jobUpdates,
        std::vector<std::pair<TOperationId, TJobId>>* successfullyUpdatedJobs,
        std::vector<TJobId>* jobsToAbort) = 0;

    virtual void ApplyJobMetricsDelta(const TOperationIdToOperationJobMetrics& operationIdToOperationJobMetrics) = 0;

    virtual void UpdatePoolTrees(const NYTree::INodePtr& poolTreesNode) = 0;

    virtual std::vector<TString> GetNodeTreeIds(const THashSet<TString>& tags) = 0;

    virtual void ApplyOperationRuntimeParameters(IOperationStrategyHost* operation) = 0;

    virtual TFuture<void> ValidateOperationRuntimeParameters(
        IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters,
        bool validatePools) = 0;

    virtual void InitOperationRuntimeParameters(
        const TOperationRuntimeParametersPtr& runtimeParameters,
        const TOperationSpecBasePtr& spec,
        const NSecurityClient::TSerializableAccessControlList& baseAcl,
        const TString& user,
        EOperationType operationType) = 0;

    //! Updates current config used by strategy.
    virtual void UpdateConfig(const TFairShareStrategyConfigPtr& config) = 0;

    //! Builds a YSON structure containing a set of attributes to be assigned to operation's node
    //! in Cypress during creation.
    virtual void BuildOperationAttributes(
        TOperationId operationId,
        NYTree::TFluentMap fluent) = 0;

    //! Builds a YSON map fragment with strategy specific information about operation
    //! that used for event log.
    virtual void BuildOperationInfoForEventLog(
        const IOperationStrategyHost* operation,
        NYTree::TFluentMap fluent) = 0;

    //! Builds a YSON structure reflecting operation's progress.
    //! This progress is periodically pushed into Cypress and is also displayed via Orchid.
    virtual void BuildOperationProgress(
        TOperationId operationId,
        NYTree::TFluentMap fluent) = 0;

    //! Similar to #BuildOperationProgress but constructs a reduced version to be used by UI.
    virtual void BuildBriefOperationProgress(
        TOperationId operationId,
        NYTree::TFluentMap fluent) = 0;

    //! Builds a YSON structure reflecting the state of the scheduler to be displayed in Orchid.
    virtual void BuildOrchid(NYTree::TFluentMap fluent) = 0;

    virtual TPoolTreeControllerSettingsMap GetOperationPoolTreeControllerSettingsMap(TOperationId operationId) = 0;

    virtual std::vector<std::pair<TOperationId, TError>> GetUnschedulableOperations() = 0;

    virtual void ScanWaitingForPoolOperations() = 0;

    virtual TFuture<void> GetFullFairShareUpdateFinished() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulerStrategy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
