#pragma once

#include "public.h"

#include "job_resources_helpers.h"
#include "master_connector.h"
#include "job.h"

#include <yt/yt/server/lib/scheduler/event_log.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/library/vector_hdrf/resource_helpers.h>

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
    virtual IInvokerPtr GetBackgroundInvoker() const = 0;
    virtual IInvokerPtr GetOrchidWorkerInvoker() const = 0;

    virtual NEventLog::TFluentLogEvent LogFairShareEventFluently(TInstant now) = 0;
    virtual NEventLog::TFluentLogEvent LogAccumulatedUsageEventFluently(TInstant now) = 0;

    virtual void Disconnect(const TError& error) = 0;

    virtual TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) const = 0;
    virtual TJobResources GetResourceUsage(const TSchedulingTagFilter& filter) const = 0;
    virtual TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(const TSchedulingTagFilter& filter = {}) const = 0;

    // TODO(eshcherbin): Add interface for node manager to be used by strategy.
    virtual const std::vector<IInvokerPtr>& GetNodeShardInvokers() const = 0;
    virtual int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId) const = 0;
    virtual void AbortJobsAtNode(NNodeTrackerClient::TNodeId nodeId, EAbortReason reason) = 0;

    virtual TString FormatResources(const TJobResourcesWithQuota& resources) const = 0;
    virtual TString FormatResourceUsage(
        const TJobResources& usage,
        const TJobResources& limits,
        const NNodeTrackerClient::NProto::TDiskResources& diskResources) const = 0;
    virtual void SerializeResources(const TJobResourcesWithQuota& resources, NYson::IYsonConsumer* consumer) const = 0;
    virtual void SerializeDiskQuota(const TDiskQuota& diskQuota, NYson::IYsonConsumer* consumer) const = 0;

    virtual std::optional<int> FindMediumIndexByName(const TString& mediumName) const = 0;
    virtual const TString& GetMediumNameByIndex(int mediumIndex) const = 0;

    virtual TInstant GetConnectionTime() const = 0;

    virtual void MarkOperationAsRunningInStrategy(TOperationId operationId) = 0;
    virtual void AbortOperation(TOperationId operationId, const TError& error) = 0;
    virtual void FlushOperationNode(TOperationId operationId) = 0;

    virtual TMemoryDistribution GetExecNodeMemoryDistribution(const TSchedulingTagFilter& filter) const = 0;

    virtual void ValidatePoolPermission(
        TGuid poolObjectId,
        const TString& poolName,
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

    virtual void LogResourceMetering(
        const TMeteringKey& key,
        const TMeteringStatistics& statistics,
        const THashMap<TString, TString>& otherTags,
        TInstant connectionTime,
        TInstant previousLogTime,
        TInstant currentTime) = 0;
    virtual int GetDefaultAbcId() const = 0;

    virtual void InvokeStoringStrategyState(TPersistentStrategyStatePtr strategyState) = 0;

    virtual TFuture<void> UpdateLastMeteringLogTime(TInstant time) = 0;

    virtual const THashMap<TString, TString>& GetUserDefaultParentPoolMap() const = 0;
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
    // It is used to update job resources in case of EJobUpdateStatus::Running status.
    TJobResources JobResources;
    // It is used to determine whether the job should be aborted if the operation is running in a module-aware scheduling segment.
    std::optional<TString> JobDataCenter;
    std::optional<TString> JobInfinibandCluster;
};

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedJobPreemptionStatusMapPerOperation
    : public TRefCounted
    , public TJobPreemptionStatusMapPerOperation
{ };

DEFINE_REFCOUNTED_TYPE(TRefCountedJobPreemptionStatusMapPerOperation)

struct TCachedJobPreemptionStatuses
{
    TRefCountedJobPreemptionStatusMapPerOperationPtr Value;
    TInstant UpdateTime;
};

////////////////////////////////////////////////////////////////////////////////

struct ISchedulerStrategy
    : public virtual TRefCounted
{
    //! Processes running jobs and schedules new jobs.
    virtual TFuture<void> ProcessSchedulingHeartbeat(const ISchedulingContextPtr& schedulingContext, bool skipScheduleJobs) = 0;

    //! Notify strategy about job updates.
    virtual void ProcessJobUpdates(
        const std::vector<TJobUpdate>& jobUpdates,
        THashSet<TJobId>* jobsToPostpone,
        std::vector<TJobId>* jobsToAbort) = 0;

    //! Save some strategy-specific attributes from handshake result.
    virtual void OnMasterHandshake(const TMasterHandshakeResult& result) = 0;

    //! Starts periodic updates and logging.
    virtual void OnMasterConnected() = 0;

    //! Stops all activities, resets all state.
    virtual void OnMasterDisconnected() = 0;

    //! Registers or updates a node.
    virtual TFuture<void> RegisterOrUpdateNode(
        NNodeTrackerClient::TNodeId nodeId,
        const TString& nodeAddress,
        const TBooleanFormulaTags& tags) = 0;

    //! Unregisters node.
    virtual void UnregisterNode(
        NNodeTrackerClient::TNodeId nodeId,
        const TString& nodeAddress) = 0;

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
    virtual void RegisterOperation(
        IOperationStrategyHost* operation,
        std::vector<TString>* unknownTreeIds,
        TPoolTreeControllerSettingsMap* poolTreeControllerSettingsMap) = 0;

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

    //! Error results in operation's failure.
    virtual TError OnOperationMaterialized(TOperationId operationId) = 0;

    virtual void ApplyJobMetricsDelta(TOperationIdToOperationJobMetrics operationIdToOperationJobMetrics) = 0;

    virtual void UpdatePoolTrees(const NYson::TYsonString& poolTreesYson) = 0;

    virtual TError UpdateUserToDefaultPoolMap(const THashMap<TString, TString>& userToDefaultPoolMap) = 0;

    //! Initializes persistent strategy state.
    virtual void InitPersistentState(const TPersistentStrategyStatePtr& persistentStrategyState) = 0;

    virtual void ApplyOperationRuntimeParameters(IOperationStrategyHost* operation) = 0;

    virtual TFuture<void> ValidateOperationRuntimeParameters(
        IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters,
        bool validatePools) = 0;

    virtual void ValidatePoolLimits(
        IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters) = 0;

    virtual void InitOperationRuntimeParameters(
        const TOperationRuntimeParametersPtr& runtimeParameters,
        const TOperationSpecBasePtr& spec,
        const TString& user,
        EOperationType operationType,
        TOperationId operationId) = 0;

    //! Apply |update| to |origin|, changing |origin|.
    //! NOTE: |origin| can not be |nullptr|.
    virtual void UpdateRuntimeParameters(
        const TOperationRuntimeParametersPtr& origin,
        const TOperationRuntimeParametersUpdatePtr& update,
        const TString& user) = 0;

    //! Updates current config used by strategy.
    virtual void UpdateConfig(const TFairShareStrategyConfigPtr& config) = 0;

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

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    virtual std::vector<std::pair<TOperationId, TError>> GetHungOperations() = 0;

    virtual void ScanPendingOperations() = 0;

    virtual TFuture<void> GetFullFairShareUpdateFinished() = 0;

    //! These methods are used for diagnostics.
    virtual void BuildSchedulingAttributesStringForNode(
        NNodeTrackerClient::TNodeId nodeId,
        const TString& nodeAddress,
        const TBooleanFormulaTags& nodeTags,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;
    virtual void BuildSchedulingAttributesForNode(
        NNodeTrackerClient::TNodeId nodeId,
        const TString& nodeAddress,
        const TBooleanFormulaTags& nodeTags,
        NYTree::TFluentMap fluent) const = 0;
    virtual void BuildSchedulingAttributesStringForOngoingJobs(
        const TString& nodeAddress,
        const TBooleanFormulaTags& nodeTags,
        const std::vector<TJobPtr>& jobs,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;

    //! These OnFairShare*At methods used for testing purposes in simulator.
    //! Called periodically to collect the metrics of tree elements.
    virtual void OnFairShareProfilingAt(TInstant now) = 0;

    //! Called periodically to build new tree snapshots.
    virtual void OnFairShareUpdateAt(TInstant now) = 0;

    //! Called periodically to log scheduling tree state.
    virtual void OnFairShareLoggingAt(TInstant now) = 0;

    //! Called periodically to log essential for simulator tree state.
    virtual void OnFairShareEssentialLoggingAt(TInstant now) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulerStrategy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
