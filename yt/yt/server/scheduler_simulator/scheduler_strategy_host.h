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

#include <yt/yt/ytlib/scheduler/job_resources.h>

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

    virtual IInvokerPtr GetControlInvoker(NScheduler::EControlQueue queue) const override;
    virtual IInvokerPtr GetFairShareLoggingInvoker() const override;
    virtual IInvokerPtr GetFairShareProfilingInvoker() const override;
    virtual IInvokerPtr GetFairShareUpdateInvoker() const override;
    virtual IInvokerPtr GetOrchidWorkerInvoker() const override;
    virtual const std::vector<IInvokerPtr>& GetNodeShardInvokers() const override;

    virtual NEventLog::TFluentLogEvent LogFairShareEventFluently(TInstant now) override;

    virtual NScheduler::TJobResources GetResourceLimits(const NScheduler::TSchedulingTagFilter& filter) const override;
    virtual NScheduler::TJobResources GetResourceUsage(const NScheduler::TSchedulingTagFilter& filter) const override;

    virtual void Disconnect(const TError& error) override;
    virtual TInstant GetConnectionTime() const override;

    virtual NScheduler::TMemoryDistribution GetExecNodeMemoryDistribution(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    virtual std::vector<NNodeTrackerClient::TNodeId> GetExecNodeIds(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    virtual TString GetExecNodeAddress(NNodeTrackerClient::TNodeId nodeId) const override;

    virtual NScheduler::TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    virtual void UpdateNodesOnChangedTrees(
        const THashMap<TString, NScheduler::TSchedulingTagFilter>& treeIdToFilter) override;

    virtual TString FormatResources(const NScheduler::TJobResourcesWithQuota& resources) const override;
    virtual TString FormatResourceUsage(
        const NScheduler::TJobResources& usage,
        const NScheduler::TJobResources& limits,
        const NNodeTrackerClient::NProto::TDiskResources& diskResources) const override;
    virtual void SerializeResources(const NScheduler::TJobResourcesWithQuota& resources, NYson::IYsonConsumer* consumer) const override;

    virtual void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const override;

    virtual void MarkOperationAsRunningInStrategy(NScheduler::TOperationId operationId) override;

    virtual void AbortOperation(NScheduler::TOperationId operationId, const TError& error) override;
    virtual void FlushOperationNode(NScheduler::TOperationId operationId) override;

    void PreemptJob(const NScheduler::TJobPtr& job, TDuration interruptTimeout);

    virtual NYson::IYsonConsumer* GetEventLogConsumer() override;

    virtual const NLogging::TLogger* GetEventLogger() override;

    virtual void SetSchedulerAlert(NScheduler::ESchedulerAlertType alertType, const TError& alert) override;

    virtual TFuture<void> SetOperationAlert(
        NScheduler::TOperationId operationId,
        NScheduler::EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout) override;

    virtual void LogResourceMetering(
        const NScheduler::TMeteringKey& key,
        const NScheduler::TMeteringStatistics& statistics,
        const THashMap<TString, TString>& otherTags,
        TInstant lastUpdateTime,
        TInstant now) override;

    virtual int GetDefaultAbcId() const override;

    virtual void InvokeStoringStrategyState(NScheduler::TPersistentStrategyStatePtr persistentStrategyState) override;

    virtual const THashMap<TString, TString>& GetUserDefaultParentPoolMap() const override;

    void CloseEventLogger();

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
