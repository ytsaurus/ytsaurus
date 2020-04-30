#include "scheduler_strategy_host.h"

#include "event_log.h"

namespace NYT::NSchedulerSimulator {

using namespace NScheduler;
using namespace NConcurrency;
using namespace NEventLog;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerSimulatorLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerStrategyHost::TSchedulerStrategyHost(
    const std::vector<NScheduler::TExecNodePtr>* execNodes,
    IOutputStream* eventLogOutputStream,
    const TRemoteEventLogConfigPtr& remoteEventLogConfig)
    : ExecNodes_(execNodes)
    , MediumDirectory_(CreateDefaultMediumDirectory())
{
    YT_VERIFY(eventLogOutputStream || remoteEventLogConfig);

    if (remoteEventLogConfig) {
        RemoteEventLogWriter_ = CreateRemoteEventLogWriter(remoteEventLogConfig, GetCurrentInvoker());
        RemoteEventLogConsumer_ = RemoteEventLogWriter_->CreateConsumer();
    } else {
        LocalEventLogWriter_.emplace(eventLogOutputStream, NYson::EYsonFormat::Binary, NYson::EYsonType::ListFragment);
    }

    for (const auto& execNode : *ExecNodes_) {
        TotalResourceLimits_ += execNode->GetResourceLimits();
    }
}

IInvokerPtr TSchedulerStrategyHost::GetControlInvoker(NYT::NScheduler::EControlQueue queue) const
{
    return GetCurrentInvoker();
}

IInvokerPtr TSchedulerStrategyHost::GetFairShareLoggingInvoker() const
{
    return GetCurrentInvoker();
}

IInvokerPtr TSchedulerStrategyHost::GetFairShareProfilingInvoker() const
{
    return GetCurrentInvoker();
}

IInvokerPtr TSchedulerStrategyHost::GetFairShareUpdateInvoker() const
{
    return GetCurrentInvoker();
}

TFluentLogEvent TSchedulerStrategyHost::LogFairShareEventFluently(TInstant now)
{
    return LogEventFluently(ELogEventType::FairShareInfo, now);
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

void TSchedulerStrategyHost::Disconnect(const TError& error)
{
    YT_VERIFY(false);
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
            ++distribution[RoundUp(resourceLimits.GetMemory(), 1_GBs)];
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

    YT_ABORT();
}

TRefCountedExecNodeDescriptorMapPtr TSchedulerStrategyHost::CalculateExecNodeDescriptors(
    const TSchedulingTagFilter& filter) const
{
    auto result = New<TRefCountedExecNodeDescriptorMap>();

    for (const auto& execNode : *ExecNodes_) {
        if (execNode->CanSchedule(filter)) {
            YT_VERIFY(result->emplace(execNode->GetId(), execNode->BuildExecDescriptor()).second);
        }
    }

    return result;
}

void TSchedulerStrategyHost::UpdateNodesOnChangedTrees(
    const THashMap<TString, NScheduler::TSchedulingTagFilter>& treeIdToFilter)
{
    // Nothing to do.
}

TString TSchedulerStrategyHost::FormatResources(const TJobResourcesWithQuota& resources) const
{
    return NScheduler::FormatResources(resources, MediumDirectory_);
}

TString TSchedulerStrategyHost::FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits,
    const NNodeTrackerClient::NProto::TDiskResources& diskResources) const
{
    return NScheduler::FormatResourceUsage(usage, limits, diskResources, MediumDirectory_);
}

void TSchedulerStrategyHost::ValidatePoolPermission(
    const NYPath::TYPath& path,
    const TString& user,
    NYTree::EPermission permission) const
{ }

void TSchedulerStrategyHost::ActivateOperation(TOperationId operationId)
{
    // Nothing to do.
}

void TSchedulerStrategyHost::AbortOperation(TOperationId operationId, const TError& error)
{
    YT_VERIFY(false);
}

void TSchedulerStrategyHost::PreemptJob(const TJobPtr& job)
{
    YT_VERIFY(job->GetNode()->Jobs().erase(job) == 1);
    job->SetState(NJobTrackerClient::EJobState::Aborted);
}

NYson::IYsonConsumer* TSchedulerStrategyHost::GetEventLogConsumer()
{
    YT_VERIFY(RemoteEventLogWriter_ || LocalEventLogWriter_);
    if (RemoteEventLogConsumer_) {
        return RemoteEventLogConsumer_.get();
    } else {
        return &LocalEventLogWriter_.value();
    }
}

const NLogging::TLogger* TSchedulerStrategyHost::GetEventLogger()
{
    return nullptr;
}

void TSchedulerStrategyHost::SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
{
    if (!alert.IsOK()) {
        YT_LOG_WARNING(alert, "Setting scheduler alert (AlertType: %lv)", alertType);
    }
}

TFuture<void> TSchedulerStrategyHost::SetOperationAlert(
    TOperationId operationId,
    EOperationAlertType alertType,
    const TError& alert,
    std::optional<TDuration> timeout)
{
    return VoidFuture;
}

void TSchedulerStrategyHost::CloseEventLogger() {
    if (RemoteEventLogWriter_) {
        WaitFor(RemoteEventLogWriter_->Close())
            .ThrowOnError();
    }
}

} // namespace NYT::NSchedulerSimulator
