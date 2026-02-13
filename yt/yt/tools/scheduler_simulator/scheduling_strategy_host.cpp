#include "scheduling_strategy_host.h"

#include "node_shard.h"

#include "event_log.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

namespace NYT::NSchedulerSimulator {

using namespace NScheduler;
using namespace NConcurrency;
using namespace NEventLog;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = SchedulerSimulatorLogger;

////////////////////////////////////////////////////////////////////////////////

TStrategyHost::TStrategyHost(
    const std::vector<NScheduler::TExecNodePtr>* execNodes,
    IOutputStream* eventLogOutputStream,
    const TRemoteEventLogConfigPtr& remoteEventLogConfig,
    const std::vector<IInvokerPtr>& nodeShardInvokers)
    : ExecNodes_(execNodes)
    , MediumDirectory_(CreateDefaultMediumDirectory())
    , NodeShardInvokers_(nodeShardInvokers)
{
    YT_VERIFY(eventLogOutputStream || remoteEventLogConfig);

    if (remoteEventLogConfig) {
        RemoteEventLogWriter_ = CreateRemoteEventLogWriter(remoteEventLogConfig, GetCurrentInvoker());
        RemoteEventLogConsumer_ = RemoteEventLogWriter_->CreateConsumer();
    } else {
        LocalEventLogWriter_.emplace(eventLogOutputStream, NYson::EYsonFormat::Binary, NYson::EYsonType::ListFragment);
    }

    for (const auto& execNode : *ExecNodes_) {
        TotalResourceLimits_ += execNode->ResourceLimits();
    }
}

IInvokerPtr TStrategyHost::GetControlInvoker(NYT::NScheduler::EControlQueue /*queue*/) const
{
    return GetCurrentInvoker();
}

IInvokerPtr TStrategyHost::GetFairShareLoggingInvoker() const
{
    return GetCurrentInvoker();
}

IInvokerPtr TStrategyHost::GetFairShareProfilingInvoker() const
{
    return GetCurrentInvoker();
}

IInvokerPtr TStrategyHost::GetFairShareUpdateInvoker() const
{
    return GetCurrentInvoker();
}

IInvokerPtr TStrategyHost::GetBackgroundInvoker() const
{
    return GetCurrentInvoker();
}

IInvokerPtr TStrategyHost::GetOrchidWorkerInvoker() const
{
    return GetCurrentInvoker();
}

int TStrategyHost::GetNodeShardId(TNodeId nodeId) const
{
    return TSimulatorNodeShard::GetNodeShardId(nodeId, std::ssize(NodeShardInvokers_));
}

const std::vector<IInvokerPtr>& TStrategyHost::GetNodeShardInvokers() const
{
    return NodeShardInvokers_;
}

TFluentLogEvent TStrategyHost::LogFairShareEventFluently(TInstant now)
{
    return LogEventFluently(GetEventLogger(), ELogEventType::FairShareInfo, now);
}

TFluentLogEvent TStrategyHost::LogAccumulatedUsageEventFluently(TInstant /*now*/)
{
    YT_UNIMPLEMENTED();
}

TJobResources TStrategyHost::GetResourceLimits(const TSchedulingTagFilter& filter) const
{
    auto it = FilterToAllocationResources_.find(filter);
    if (it != FilterToAllocationResources_.end()) {
        return it->second;
    }

    TJobResources result;
    for (const auto& execNode : *ExecNodes_) {
        if (execNode->CanSchedule(filter)) {
            result += execNode->ResourceLimits();
        }
    }

    FilterToAllocationResources_.insert({filter, result});

    return result;
}

TJobResources TStrategyHost::GetResourceUsage(const TSchedulingTagFilter& /*filter*/) const
{
    return {};
}

void TStrategyHost::Disconnect(const TError& /*error*/)
{
    YT_ABORT();
}

TInstant TStrategyHost::GetConnectionTime() const
{
    return TInstant();
}

TMemoryDistribution TStrategyHost::GetExecNodeMemoryDistribution(
    const TSchedulingTagFilter& filter) const
{
    auto it = FilterToMemoryDistribution_.find(filter);
    if (it != FilterToMemoryDistribution_.end()) {
        return it->second;
    }

    TMemoryDistribution distribution;
    for (const auto& execNode : *ExecNodes_) {
        if (execNode->CanSchedule(filter)) {
            auto resourceLimits = execNode->ResourceLimits();
            ++distribution[RoundUp(resourceLimits.GetMemory(), 1_GBs)];
        }
    }

    FilterToMemoryDistribution_.insert({filter, distribution});

    return distribution;
}

void TStrategyHost::AbortAllocationsAtNode(TNodeId /*nodeId*/, NScheduler::EAbortReason /*reason*/)
{
    // Nothing to do.
}

std::optional<int> TStrategyHost::FindMediumIndexByName(const std::string& /*mediumName*/) const
{
    return {};
}

const std::string& TStrategyHost::GetMediumNameByIndex(int /*mediumIndex*/) const
{
    static const std::string defaultMediumName = "default";
    return defaultMediumName;
}

TString TStrategyHost::FormatResources(const TJobResourcesWithQuota& resources) const
{
    return NScheduler::FormatResources(resources);
}

void TStrategyHost::SerializeResources(const TJobResourcesWithQuota& resources, NYson::IYsonConsumer* consumer) const
{
    return NScheduler::SerializeJobResourcesWithQuota(resources, MediumDirectory_, consumer);
}

void TStrategyHost::SerializeDiskQuota(const TDiskQuota& diskQuota, NYson::IYsonConsumer* consumer) const
{
    return NScheduler::SerializeDiskQuota(diskQuota, MediumDirectory_, consumer);
}

void TStrategyHost::ValidatePoolPermission(
    const std::string& /*treeId*/,
    TGuid /*poolObjectId*/,
    const TString& /*poolName*/,
    const std::string& /*user*/,
    NYTree::EPermission /*permission*/) const
{ }

void TStrategyHost::MarkOperationAsRunningInStrategy(TOperationId /*operationId*/)
{
    // Nothing to do.
}

void TStrategyHost::AbortOperation(TOperationId /*operationId*/, const TError& /*error*/)
{
    YT_ABORT();
}

void TStrategyHost::FlushOperationNode(TOperationId /*operationId*/)
{
    YT_ABORT();
}

void TStrategyHost::PreemptAllocation(const TAllocationPtr& allocation, TDuration /*interruptTimeout*/)
{
    EraseOrCrash(allocation->GetNode()->Allocations(), allocation);
    allocation->SetState(EAllocationState::Finished);
}

NYson::IYsonConsumer* TStrategyHost::GetEventLogConsumer()
{
    YT_VERIFY(RemoteEventLogWriter_ || LocalEventLogWriter_);
    if (RemoteEventLogConsumer_) {
        return RemoteEventLogConsumer_.get();
    } else {
        return &LocalEventLogWriter_.value();
    }
}

const NLogging::TLogger* TStrategyHost::GetEventLogger()
{
    return nullptr;
}

void TStrategyHost::SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
{
    if (!alert.IsOK()) {
        YT_LOG_WARNING(alert, "Setting scheduler alert (AlertType: %v)", alertType);
    }
}

TFuture<void> TStrategyHost::SetOperationAlert(
    TOperationId /*operationId*/,
    EOperationAlertType /*alertType*/,
    const TError& /*alert*/,
    std::optional<TDuration> /*timeout*/)
{
    return OKFuture;
}

void TStrategyHost::LogResourceMetering(
    const TMeteringKey& /*key*/,
    const TMeteringStatistics& /*statistics*/,
    const THashMap<TString, TString>& /*otherTags*/,
    TInstant /*connectionTime*/,
    TInstant /*previousLogTime*/,
    TInstant /*currentTime*/)
{
    // Skip!
}

int TStrategyHost::GetDefaultAbcId() const
{
    return -1;
}

void TStrategyHost::InvokeStoringStrategyState(NStrategy::TPersistentStrategyStatePtr /*persistentStrategyState*/)
{ }

TFuture<void> TStrategyHost::UpdateLastMeteringLogTime(TInstant /*time*/)
{
    return OKFuture;
}

void TStrategyHost::CloseEventLogger()
{
    if (RemoteEventLogWriter_) {
        WaitFor(RemoteEventLogWriter_->Close())
            .ThrowOnError();
    }
}

const THashMap<std::string, TString>& TStrategyHost::GetUserDefaultParentPoolMap() const
{
    static const THashMap<std::string, TString> stub;
    return stub;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
