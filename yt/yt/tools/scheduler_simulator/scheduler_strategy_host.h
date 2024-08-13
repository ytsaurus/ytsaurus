#pragma once

#include "private.h"
#include "config.h"

#include <yt/yt/server/scheduler/helpers.h>
#include <yt/yt/server/scheduler/allocation.h>
#include <yt/yt/server/scheduler/exec_node.h>
#include <yt/yt/server/scheduler/operation.h>
#include <yt/yt/server/scheduler/persistent_scheduler_state.h>
#include <yt/yt/server/scheduler/scheduler_strategy.h>

#include <yt/yt/server/lib/scheduler/event_log.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <util/generic/size_literals.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerStrategyHost
    : public NScheduler::ISchedulerStrategyHost
    , public NScheduler::TEventLogHostBase
{
public:
    TSchedulerStrategyHost(
        const std::vector<NScheduler::TExecNodePtr>* execNodes,
        IOutputStream* eventLogOutputStream,
        const TRemoteEventLogConfigPtr& remoteEventLogConfig,
        const std::vector<IInvokerPtr>& nodeShardInvoker);

    IInvokerPtr GetControlInvoker(NScheduler::EControlQueue queue) const override;
    IInvokerPtr GetFairShareLoggingInvoker() const override;
    IInvokerPtr GetFairShareProfilingInvoker() const override;
    IInvokerPtr GetFairShareUpdateInvoker() const override;
    IInvokerPtr GetBackgroundInvoker() const override;
    IInvokerPtr GetOrchidWorkerInvoker() const override;

    NEventLog::TFluentLogEvent LogFairShareEventFluently(TInstant now) override;
    NEventLog::TFluentLogEvent LogAccumulatedUsageEventFluently(TInstant now) override;

    NScheduler::TJobResources GetResourceLimits(const NScheduler::TSchedulingTagFilter& filter) const override;
    NScheduler::TJobResources GetResourceUsage(const NScheduler::TSchedulingTagFilter& filter) const override;

    void Disconnect(const TError& error) override;
    TInstant GetConnectionTime() const override;

    NScheduler::TMemoryDistribution GetExecNodeMemoryDistribution(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    NScheduler::TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    const std::vector<IInvokerPtr>& GetNodeShardInvokers() const override;
    int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId) const override;
    void AbortAllocationsAtNode(NNodeTrackerClient::TNodeId nodeId, NScheduler::EAbortReason reason) override;

    std::optional<int> FindMediumIndexByName(const TString& mediumName) const override;
    const TString& GetMediumNameByIndex(int mediumIndex) const override;

    TString FormatResources(const NScheduler::TJobResourcesWithQuota& resources) const override;
    void SerializeResources(const NScheduler::TJobResourcesWithQuota& resources, NYson::IYsonConsumer* consumer) const override;
    void SerializeDiskQuota(const NScheduler::TDiskQuota& diskQuota, NYson::IYsonConsumer* consumer) const override;

    void ValidatePoolPermission(
        NObjectClient::TObjectId poolObjectId,
        const TString& poolName,
        const TString& user,
        NYTree::EPermission permission) const override;

    void MarkOperationAsRunningInStrategy(NScheduler::TOperationId operationId) override;

    void AbortOperation(NScheduler::TOperationId operationId, const TError& error) override;
    void FlushOperationNode(NScheduler::TOperationId operationId) override;

    void PreemptAllocation(const NScheduler::TAllocationPtr& allocation, TDuration interruptTimeout);

    NYson::IYsonConsumer* GetEventLogConsumer() override;

    const NLogging::TLogger* GetEventLogger() override;

    void SetSchedulerAlert(NScheduler::ESchedulerAlertType alertType, const TError& alert) override;

    TFuture<void> SetOperationAlert(
        NScheduler::TOperationId operationId,
        NScheduler::EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout) override;

    void LogResourceMetering(
        const NScheduler::TMeteringKey& key,
        const NScheduler::TMeteringStatistics& statistics,
        const THashMap<TString, TString>& otherTags,
        TInstant connectionTime,
        TInstant previousLogTime,
        TInstant currentTime) override;

    int GetDefaultAbcId() const override;

    void InvokeStoringStrategyState(NScheduler::TPersistentStrategyStatePtr persistentStrategyState) override;

    TFuture<void> UpdateLastMeteringLogTime(TInstant time) override;

    void CloseEventLogger();

    const THashMap<TString, TString>& GetUserDefaultParentPoolMap() const override;

private:
    const std::vector<NScheduler::TExecNodePtr>* ExecNodes_;
    NScheduler::TJobResources TotalResourceLimits_;
    mutable THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TJobResources> FilterToAllocationResources_;
    mutable THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TMemoryDistribution> FilterToMemoryDistribution_;
    std::optional<NYson::TYsonWriter> LocalEventLogWriter_;

    NEventLog::IEventLogWriterPtr RemoteEventLogWriter_;
    std::unique_ptr<NYson::IYsonConsumer> RemoteEventLogConsumer_;

    NChunkClient::TMediumDirectoryPtr MediumDirectory_;

    const std::vector<IInvokerPtr>& NodeShardInvokers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
