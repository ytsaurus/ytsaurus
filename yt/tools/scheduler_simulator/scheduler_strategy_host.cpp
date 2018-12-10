#include "scheduler_strategy_host.h"

namespace NYT {
namespace NSchedulerSimulator {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerSimulatorLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerStrategyHost::TSchedulerStrategyHost(
    const std::vector<NScheduler::TExecNodePtr>* execNodes,
    IOutputStream* eventLogOutputStream)
    : ExecNodes_(execNodes)
    , TotalResourceLimits_(ZeroJobResources())
    , Writer_(eventLogOutputStream, NYson::EYsonFormat::Pretty, NYson::EYsonType::ListFragment)
{
    for (const auto& execNode : *ExecNodes_) {
        TotalResourceLimits_ += execNode->GetResourceLimits();
    }
}

TJobResources TSchedulerStrategyHost::GetResourceLimits(const TSchedulingTagFilter& filter)
{
    auto it = FilterToJobResources_.find(filter);
    if (it != FilterToJobResources_.end()) {
        return it->second;
    }

    TJobResources result;
    for (const auto& execNode : *ExecNodes_) {
        if (execNode->CanSchedule(filter)) {
            result += execNode->GetResourceLimits();
        }
    }

    FilterToJobResources_.insert({filter, result});

    return result;
}

TInstant TSchedulerStrategyHost::GetConnectionTime() const
{
    return TInstant();
}

TMemoryDistribution TSchedulerStrategyHost::GetExecNodeMemoryDistribution(
    const TSchedulingTagFilter& filter) const
{
    auto it = FilterToMemoryDistribution_.find(filter);
    if (it != FilterToMemoryDistribution_.end()) {
        return it->second;
    }

    TMemoryDistribution distribution;
    for (const auto& execNode : *ExecNodes_) {
        if (execNode->CanSchedule(filter)) {
            auto resourceLimits = execNode->GetResourceLimits();
            ++distribution[RoundUp(resourceLimits.GetMemory(), 1_GB)];
        }
    }

    FilterToMemoryDistribution_.insert({filter, distribution});

    return distribution;
}

std::vector<NNodeTrackerClient::TNodeId> TSchedulerStrategyHost::GetExecNodeIds(
    const TSchedulingTagFilter& filter) const
{
    std::vector<NNodeTrackerClient::TNodeId> result;
    for (const auto& execNode : *ExecNodes_) {
        if (execNode->CanSchedule(filter)) {
            result.push_back(execNode->GetId());
        }
    }
    return result;
}  

TString TSchedulerStrategyHost::GetExecNodeAddress(NNodeTrackerClient::TNodeId nodeId) const
{
    for (const auto& execNode : *ExecNodes_) {
        if (execNode->GetId() == nodeId) {
            return execNode->GetDefaultAddress();
        }
    }

    Y_UNREACHABLE();
}

TRefCountedExecNodeDescriptorMapPtr TSchedulerStrategyHost::CalculateExecNodeDescriptors(
    const TSchedulingTagFilter& filter) const
{
    auto result = New<TRefCountedExecNodeDescriptorMap>();

    for (const auto& execNode : *ExecNodes_) {
        if (execNode->CanSchedule(filter)) {
            YCHECK(result->emplace(execNode->GetId(), execNode->BuildExecDescriptor()).second);
        }
    }

    return result;
}

void TSchedulerStrategyHost::ValidatePoolPermission(
    const NYPath::TYPath& path,
    const TString& user,
    NYTree::EPermission permission) const
{ }

void TSchedulerStrategyHost::ActivateOperation(const TOperationId& operationId)
{
    // Nothing to do.
}

void TSchedulerStrategyHost::AbortOperation(const TOperationId& operationId, const TError& error)
{
    YCHECK(false);
}

void TSchedulerStrategyHost::PreemptJob(const TJobPtr& job, bool shouldLogEvent)
{
    YCHECK(job->GetNode()->Jobs().erase(job) == 1);
    job->SetState(NJobTrackerClient::EJobState::Aborted);

    if (shouldLogEvent) {
        LogFinishedJobFluently(ELogEventType::JobAborted, job);
    }
}

NYson::IYsonConsumer* TSchedulerStrategyHost::GetEventLogConsumer()
{
    return &Writer_;
}

void TSchedulerStrategyHost::LogFinishedJobFluently(ELogEventType eventType, TJobPtr job)
{
    LogEventFluently(eventType)
        .Item("job_id").Value(job->GetId())
        .Item("operation_id").Value(job->GetOperationId())
        .Item("start_time").Value(job->GetStartTime())
        .Item("finish_time").Value(job->GetFinishTime())
        .Item("resource_limits").Value(job->ResourceLimits())
        .Item("job_type").Value(job->GetType());
}

void TSchedulerStrategyHost::SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
{
    if (!alert.IsOK()) {
        LOG_WARNING(alert, "Setting scheduler alert (AlertType: %lv)", alertType);
    }
}

TFuture<void> TSchedulerStrategyHost::SetOperationAlert(
    const TOperationId& operationId,
    EOperationAlertType alertType,
    const TError& alert,
    std::optional<TDuration> timeout)
{
    return VoidFuture;
}

} // namespace NSchedulerSimulator
} // namespace NYT
