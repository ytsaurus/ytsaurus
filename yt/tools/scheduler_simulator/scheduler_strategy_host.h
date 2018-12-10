#pragma once

#include "private.h"

#include <yt/server/scheduler/helpers.h>
#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/event_log.h>
#include <yt/server/scheduler/exec_node.h>
#include <yt/server/scheduler/operation.h>
#include <yt/server/scheduler/scheduler_strategy.h>
#include <yt/server/controller_agent/operation_controller.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/size_literals.h>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerStrategyHost
    : public NScheduler::ISchedulerStrategyHost
    , public NScheduler::TEventLogHostBase
{
public:
    TSchedulerStrategyHost(
        const std::vector<NScheduler::TExecNodePtr>* execNodes,
        IOutputStream* eventLogOutputStream);

    virtual NScheduler::TJobResources GetResourceLimits(const NScheduler::TSchedulingTagFilter& filter) override;

    TInstant GetConnectionTime() const override;

    virtual NScheduler::TMemoryDistribution GetExecNodeMemoryDistribution(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    virtual std::vector<NNodeTrackerClient::TNodeId> GetExecNodeIds(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    virtual TString GetExecNodeAddress(NNodeTrackerClient::TNodeId nodeId) const override;

    virtual NScheduler::TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(
        const NScheduler::TSchedulingTagFilter& filter) const override;

    virtual void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const override;

    virtual void ActivateOperation(const NScheduler::TOperationId& operationId) override;

    virtual void AbortOperation(const NScheduler::TOperationId& operationId, const TError& error) override;

    void PreemptJob(const NScheduler::TJobPtr& job, bool shouldLogEvent);

    virtual NYson::IYsonConsumer* GetEventLogConsumer() override;

    void LogFinishedJobFluently(NScheduler::ELogEventType eventType, NScheduler::TJobPtr job);

    virtual void SetSchedulerAlert(NScheduler::ESchedulerAlertType alertType, const TError& alert) override;

    virtual TFuture<void> SetOperationAlert(
        const NScheduler::TOperationId& operationId,
        NScheduler::EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout) override;

private:
    const std::vector<NScheduler::TExecNodePtr>* ExecNodes_;
    NScheduler::TJobResources TotalResourceLimits_;
    THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TJobResources> FilterToJobResources_;
    mutable THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TMemoryDistribution> FilterToMemoryDistribution_;
    NYson::TYsonWriter Writer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
