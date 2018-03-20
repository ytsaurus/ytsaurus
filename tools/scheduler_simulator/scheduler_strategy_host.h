#pragma once

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
        const std::vector<NScheduler::TExecNodePtr>& execNodes,
        IOutputStream* eventLogOutputStream)
        : ExecNodes_(execNodes)
        , TotalResourceLimits_(NScheduler::ZeroJobResources())
        , Writer_(eventLogOutputStream, NYson::EYsonFormat::Pretty, NYson::EYsonType::ListFragment)
    {
        for (const auto& execNode : ExecNodes_) {
            TotalResourceLimits_ += execNode->GetResourceLimits();
        }
    }

    virtual NScheduler::TJobResources GetTotalResourceLimits() override
    {
        return TotalResourceLimits_;
    }

    virtual NScheduler::TJobResources GetResourceLimits(
        const NScheduler::TSchedulingTagFilter& filter) override
    {
        auto it = FilterToJobResources_.find(filter);
        if (it != FilterToJobResources_.end()) {
            return it->second;
        }

        NScheduler::TJobResources result;
        for (const auto& execNode : ExecNodes_) {
            if (execNode->CanSchedule(filter)) {
                result += execNode->GetResourceLimits();
            }
        }

        FilterToJobResources_.insert({filter, result});

        return result;
    }

    TInstant GetConnectionTime() const override
    {
        return TInstant();
    }

    virtual NScheduler::TMemoryDistribution GetExecNodeMemoryDistribution(
        const NScheduler::TSchedulingTagFilter& filter) const override
    {
        auto it = FilterToMemoryDistribution_.find(filter);
        if (it != FilterToMemoryDistribution_.end()) {
            return it->second;
        }

        NScheduler::TMemoryDistribution distribution;
        for (const auto& execNode : ExecNodes_) {
            if (execNode->CanSchedule(filter)) {
                auto resourceLimits = execNode->GetResourceLimits();
                ++distribution[RoundUp(resourceLimits.GetMemory(), 1_GB)];
            }
        }

        FilterToMemoryDistribution_.insert({filter, distribution});

        return distribution;
    }

    virtual std::vector<NNodeTrackerClient::TNodeId> GetExecNodeIds(
        const NScheduler::TSchedulingTagFilter& filter) const override
    {
        std::vector<NNodeTrackerClient::TNodeId> result;
        for (const auto& execNode : ExecNodes_) {
            if (execNode->CanSchedule(filter)) {
                result.push_back(execNode->GetId());
            }
        }
        return result;
    }

    virtual NScheduler::TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(
        const NScheduler::TSchedulingTagFilter& filter) const override
    {
        auto result = New<NScheduler::TRefCountedExecNodeDescriptorMap>();

        for (const auto& execNode : ExecNodes_) {
            if (execNode->CanSchedule(filter)) {
                YCHECK(result->emplace(execNode->GetId(), execNode->BuildExecDescriptor()).second);
            }
        }

        return result;
    }

    virtual void ValidatePoolPermission(
        const NYPath::TYPath& path,
        const TString& user,
        NYTree::EPermission permission) const override
    { }

    virtual void ActivateOperation(const NScheduler::TOperationId& operationId) override
    { }

    virtual void AbortOperation(const NScheduler::TOperationId& operationId, const TError& error) override
    { }

    void PreemptJob(const NScheduler::TJobPtr& job, bool shouldLogEvent)
    {
        YCHECK(job->GetNode()->Jobs().erase(job) == 1);

        auto status = NScheduler::JobStatusFromError(TError());
        job->SetState(NJobTrackerClient::EJobState::Aborted);

        if (shouldLogEvent) {
            LogFinishedJobFluently(NScheduler::ELogEventType::JobAborted, job);
        }
    }

    virtual NYson::IYsonConsumer* GetEventLogConsumer() override
    {
        return &Writer_;
    }

    void LogFinishedJobFluently(NScheduler::ELogEventType eventType, NScheduler::TJobPtr job)
    {
        LogEventFluently(eventType)
            .Item("job_id").Value(job->GetId())
            .Item("operation_id").Value(job->GetOperationId())
            .Item("start_time").Value(job->GetStartTime())
            .Item("finish_time").Value(job->GetFinishTime())
            .Item("resource_limits").Value(job->ResourceLimits())
            .Item("job_type").Value(job->GetType());
    }

    virtual void SetSchedulerAlert(NScheduler::ESchedulerAlertType alertType, const TError& alert) override
    { }

    virtual TFuture<void> SetOperationAlert(
        const NScheduler::TOperationId& operationId,
        NScheduler::EOperationAlertType alertType,
        const TError& alert) override
    {
        return VoidFuture;
    }

private:
    const std::vector<NScheduler::TExecNodePtr>& ExecNodes_;
    NScheduler::TJobResources TotalResourceLimits_;
    THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TJobResources> FilterToJobResources_;
    mutable THashMap<NScheduler::TSchedulingTagFilter, NScheduler::TMemoryDistribution> FilterToMemoryDistribution_;
    NYson::TYsonWriter Writer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namesapce NSchedulerSimulator
} // namesapce NYT
