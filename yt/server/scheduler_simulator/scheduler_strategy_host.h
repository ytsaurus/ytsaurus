#pragma once

#include "private.h"
#include "config.h"

#include <yt/server/scheduler/helpers.h>
#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/exec_node.h>
#include <yt/server/scheduler/operation.h>
#include <yt/server/scheduler/scheduler_strategy.h>

#include <yt/server/lib/scheduler/event_log.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/misc/numeric_helpers.h>

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
        const TRemoteEventLogConfigPtr& remoteEventLogConfig);

    virtual IInvokerPtr GetControlInvoker(NScheduler::EControlQueue queue) const override;
    virtual IInvokerPtr GetFairShareProfilingInvoker() const override;
    virtual IInvokerPtr GetFairShareUpdateInvoker() const override;

    virtual NScheduler::TJobResources GetResourceLimits(const NScheduler::TSchedulingTagFilter& filter) override;

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

    virtual void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const override;

    virtual void ActivateOperation(NScheduler::TOperationId operationId) override;

    virtual void AbortOperation(NScheduler::TOperationId operationId, const TError& error) override;

    void PreemptJob(const NScheduler::TJobPtr& job);

    virtual NYson::IYsonConsumer* GetEventLogConsumer() override;

    virtual const NLogging::TLogger* GetEventLogger() override;

    virtual void SetSchedulerAlert(NScheduler::ESchedulerAlertType alertType, const TError& alert) override;

    virtual TFuture<void> SetOperationAlert(
        NScheduler::TOperationId operationId,
        NScheduler::EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout) override;

    void CloseEventLogger();

private:
    const std::vector<NScheduler::TExecNodePtr>* ExecNodes_;
    NScheduler::TJobResources TotalResourceLimits_;
    THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TJobResources> FilterToJobResources_;
    mutable THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TMemoryDistribution> FilterToMemoryDistribution_;
    std::optional<NYson::TYsonWriter> LocalEventLogWriter_;

    NEventLog::IEventLogWriterPtr RemoteEventLogWriter_;
    std::unique_ptr<NYson::IYsonConsumer> RemoteEventLogConsumer_;

    NChunkClient::TMediumDirectoryPtr MediumDirectory_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
