#pragma once

#include "private.h"
#include "config.h"

#include <yt/yt/server/scheduler/helpers.h>
#include <yt/yt/server/scheduler/job.h>
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
        const IInvokerPtr& nodeShardsInvoker);

    IInvokerPtr GetControlInvoker(NScheduler::EControlQueue queue) const override;
    IInvokerPtr GetFairShareLoggingInvoker() const override;
    IInvokerPtr GetFairShareProfilingInvoker() const override;
    IInvokerPtr GetFairShareUpdateInvoker() const override;
    IInvokerPtr GetBackgroundInvoker() const override;
    IInvokerPtr GetOrchidWorkerInvoker() const override;
    const std::vector<IInvokerPtr>& GetNodeShardInvokers() const override;

    NEventLog::TFluentLogEvent LogFairShareEventFluently(TInstant now) override;

    NScheduler::TJobResources GetResourceLimits(const NScheduler::TSchedulingTagFilter& filter) const override;
    NScheduler::TJobResources GetResourceUsage(const NScheduler::TSchedulingTagFilter& filter) const override;

    void Disconnect(const TError& error) override;
    TInstant GetConnectionTime() const override;

    NScheduler::TMemoryDistribution GetExecNodeMemoryDistribution(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    std::vector<NNodeTrackerClient::TNodeId> GetExecNodeIds(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    TString GetExecNodeAddress(NNodeTrackerClient::TNodeId nodeId) const override;

    NScheduler::TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    void UpdateNodesOnChangedTrees(
        const THashMap<TString, NScheduler::TSchedulingTagFilter>& treeIdToFilter) override;

    TString FormatResources(const NScheduler::TJobResourcesWithQuota& resources) const override;
    TString FormatResourceUsage(
        const NScheduler::TJobResources& usage,
        const NScheduler::TJobResources& limits,
        const NNodeTrackerClient::NProto::TDiskResources& diskResources) const override;
    void SerializeResources(const NScheduler::TJobResourcesWithQuota& resources, NYson::IYsonConsumer* consumer) const override;

    void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const override;

    void MarkOperationAsRunningInStrategy(NScheduler::TOperationId operationId) override;

    void AbortOperation(NScheduler::TOperationId operationId, const TError& error) override;
    void FlushOperationNode(NScheduler::TOperationId operationId) override;

    void PreemptJob(const NScheduler::TJobPtr& job, TDuration interruptTimeout);

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
        TInstant lastUpdateTime,
        TInstant now) override;

    int GetDefaultAbcId() const override;

    void InvokeStoringStrategyState(NScheduler::TPersistentStrategyStatePtr persistentStrategyState) override;

    TFuture<void> UpdateLastMeteringLogTime(TInstant time) override;

    void CloseEventLogger();

    const THashMap<TString, TString>& GetUserDefaultParentPoolMap() const override;

private:
    const std::vector<NScheduler::TExecNodePtr>* ExecNodes_;
    NScheduler::TJobResources TotalResourceLimits_;
    mutable THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TJobResources> FilterToJobResources_;
    mutable THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TMemoryDistribution> FilterToMemoryDistribution_;
    std::optional<NYson::TYsonWriter> LocalEventLogWriter_;

    NEventLog::IEventLogWriterPtr RemoteEventLogWriter_;
    std::unique_ptr<NYson::IYsonConsumer> RemoteEventLogConsumer_;

    NChunkClient::TMediumDirectoryPtr MediumDirectory_;
    std::vector<IInvokerPtr> NodeShardsInvokers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
