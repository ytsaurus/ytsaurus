#include "controller_agent_connector.h"
#include "bootstrap.h"
#include "job_detail.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/controller_agent/public.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;

using NControllerAgent::TJobTrackerServiceProxy;
using NJobAgent::FillJobStatus;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentConnectorPool::TControllerAgentConnector
    : public TControllerAgentConnectorPool::TControllerAgentConnectorBase
{
public:
    TControllerAgentConnector(
        TControllerAgentConnectorPool* controllerAgentConnectorPool,
        TControllerAgentDescriptor controllerAgentDescriptor);
    
    NRpc::IChannelPtr GetChannel() const noexcept;
    void SendOutOfBandHeartbeatIfNeeded();
    void EnqueueFinishedJob(const TJobPtr& job);

    void OnConfigUpdated();

    ~TControllerAgentConnector() override;

private:
    struct THeartbeatInfo
    {
        TInstant LastSentHeartbeatTime;
        TInstant LastFailedHeartbeatTime;
        TDuration FailedHeartbeatBackoffTime;
    };
    THeartbeatInfo HeartbeatInfo_;

    TControllerAgentConnectorPoolPtr ControllerAgentConnectorPool_;
    TControllerAgentDescriptor ControllerAgentDescriptor_;

    NRpc::IChannelPtr Channel_;

    const NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;

    IReconfigurableThroughputThrottlerPtr StatisticsThrottler_;

    TDuration RunningJobInfoSendingBackoff_;

    THashSet<TJobPtr> EnqueuedFinishedJobs_;
    bool ShouldSendOutOfBand_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void SendHeartbeat();
    void OnAgentIncarnationOutdated() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

TControllerAgentConnectorPool::TControllerAgentConnector::TControllerAgentConnector(
    TControllerAgentConnectorPool* controllerAgentConnectorPool,
    TControllerAgentDescriptor controllerAgentDescriptor)
    : ControllerAgentConnectorPool_(MakeStrong(controllerAgentConnectorPool))
    , ControllerAgentDescriptor_(std::move(controllerAgentDescriptor))
    , Channel_(ControllerAgentConnectorPool_->CreateChannel(ControllerAgentDescriptor_))
    , HeartbeatExecutor_(New<TPeriodicExecutor>(
        ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker(),
        BIND(&TControllerAgentConnector::SendHeartbeat, MakeWeak(this)),
        ControllerAgentConnectorPool_->CurrentConfig_->HeartbeatPeriod,
        ControllerAgentConnectorPool_->CurrentConfig_->HeartbeatSplay))
    , StatisticsThrottler_(CreateReconfigurableThroughputThrottler(
        ControllerAgentConnectorPool_->CurrentConfig_->StatisticsThrottler))
    , RunningJobInfoSendingBackoff_(
        ControllerAgentConnectorPool_->CurrentConfig_->RunningJobInfoSendingBackoff)
{
    YT_LOG_DEBUG("Controller agent connector created (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);
    HeartbeatExecutor_->Start();
}

NRpc::IChannelPtr TControllerAgentConnectorPool::TControllerAgentConnector::GetChannel() const noexcept
{
    return Channel_;
}

void TControllerAgentConnectorPool::TControllerAgentConnector::SendOutOfBandHeartbeatIfNeeded()
{
    VERIFY_INVOKER_THREAD_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker(), JobThread);

    if (ShouldSendOutOfBand_) {
        HeartbeatExecutor_->ScheduleOutOfBand();
        ShouldSendOutOfBand_ = false;
    }
}

void TControllerAgentConnectorPool::TControllerAgentConnector::EnqueueFinishedJob(const TJobPtr& job)
{
    VERIFY_INVOKER_THREAD_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker(), JobThread);

    EnqueuedFinishedJobs_.insert(job);
    ShouldSendOutOfBand_ = true;
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnConfigUpdated()
{
    VERIFY_INVOKER_THREAD_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker(), JobThread);

    const auto& currentConfig = *ControllerAgentConnectorPool_->CurrentConfig_;

    HeartbeatExecutor_->SetPeriod(currentConfig.HeartbeatPeriod);
    RunningJobInfoSendingBackoff_ = currentConfig.RunningJobInfoSendingBackoff;
    StatisticsThrottler_->Reconfigure(currentConfig.StatisticsThrottler);
}

TControllerAgentConnectorPool::TControllerAgentConnector::~TControllerAgentConnector()
{
    VERIFY_INVOKER_THREAD_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker(), JobThread);

    YT_LOG_DEBUG("Controller agent connector destroyed (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);

    ControllerAgentConnectorPool_->OnControllerAgentConnectorDestroyed(
        ControllerAgentDescriptor_);
    HeartbeatExecutor_->Stop();
}

void TControllerAgentConnectorPool::TControllerAgentConnector::SendHeartbeat()
{
    VERIFY_INVOKER_THREAD_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker(), JobThread);

    if (!ControllerAgentConnectorPool_->Bootstrap_->IsConnected())
    {
        return;
    }

    // We consider disabled heartbeats as a signal to stop sending heartbeats and we intentionally do not report jobs that remained in EnqueuedFinishedJobs_. 
    // All these jobs are supposed to be aborted by controller within timeout.
    if (!ControllerAgentConnectorPool_->AreHeartbeatsEnabled()) {
        EnqueuedFinishedJobs_.clear();
        return;
    }

    if (TInstant::Now() < HeartbeatInfo_.LastFailedHeartbeatTime + HeartbeatInfo_.FailedHeartbeatBackoffTime) {
        YT_LOG_INFO(
            "Skipping heartbeat to agent since backoff after previous heartbeat failure (AgentAddress: %v, IncarnationId: %v)",
            ControllerAgentDescriptor_.Address,
            ControllerAgentDescriptor_.IncarnationId);
        return;
    }

    std::vector<TJobPtr> jobs;
    for (auto& job : ControllerAgentConnectorPool_->Bootstrap_->GetJobController()->GetJobs()) {
        if (TypeFromId(job->GetId()) != EObjectType::SchedulerJob) {
            continue;
        }

        if (job->GetState() != EJobState::Running) {
            continue;
        }

        auto schedulerJob = StaticPointerCast<TJob>(std::move(job));

        if (!schedulerJob->ShouldSendJobInfoToAgent()) {
            continue;
        }

        const auto& controllerAgentDescriptor = schedulerJob->GetControllerAgentDescriptor();

        if (!controllerAgentDescriptor) {
            YT_LOG_DEBUG(
                "Skipping heartbeat for job since old agent incarnation is outdated and new incarnation is not received yet (JobId: %v)",
                schedulerJob->GetId());
            continue;
        }

        if (controllerAgentDescriptor != ControllerAgentDescriptor_) {
            continue;
        }

        jobs.push_back(std::move(schedulerJob));
    }

    std::vector<TJobPtr> sentEnqueuedJobs;
    sentEnqueuedJobs.reserve(std::size(EnqueuedFinishedJobs_)); 
    for (const auto job : EnqueuedFinishedJobs_) {
        sentEnqueuedJobs.push_back(job);
    }

    TJobTrackerServiceProxy proxy(Channel_);
    auto request = proxy.Heartbeat();
    request->set_node_id(ControllerAgentConnectorPool_->Bootstrap_->GetNodeId());
    ToProto(request->mutable_node_descriptor(), ControllerAgentConnectorPool_->Bootstrap_->GetLocalDescriptor());
    ToProto(request->mutable_controller_agent_incarnation_id(), ControllerAgentDescriptor_.IncarnationId);

    i64 finishedJobsStatisticsSize = 0;
    for (const auto& job : sentEnqueuedJobs) {
        auto* const jobStatus = request->add_jobs();
        FillJobStatus(jobStatus, job);
        job->ResetStatisticsLastSendTime();

        if (auto statistics = job->GetStatistics()) {
            auto statisticsString = statistics.ToString();
            finishedJobsStatisticsSize += std::ssize(statisticsString);
            jobStatus->set_statistics(std::move(statisticsString));
        }
    }

    // In case of statistics size throttling we want to report older jobs first to ensure
    // that all jobs will sent statistics eventually.
    std::sort(
        jobs.begin(),
        jobs.end(),
        [] (const auto& lhs, const auto& rhs) noexcept {
            return lhs->GetStatisticsLastSendTime() < rhs->GetStatisticsLastSendTime();
        });

    const auto now = TInstant::Now();
    int consideredRunnigJobCount = 0;
    int reportedRunningJobCount = 0;
    i64 runningJobsStatisticsSize = 0;
    for (const auto& job : jobs) {
        if (now - job->GetStatisticsLastSendTime() < RunningJobInfoSendingBackoff_) {
            break;
        }

        ++consideredRunnigJobCount;

        if (auto statistics = job->GetStatistics()) {
            auto statisticsString = statistics.ToString();
            if (StatisticsThrottler_->TryAcquire(statisticsString.size())) {
                ++reportedRunningJobCount;
                auto* const jobStatus = request->add_jobs();

                FillJobStatus(jobStatus, job);

                runningJobsStatisticsSize += statisticsString.size();
                job->ResetStatisticsLastSendTime();
                jobStatus->set_statistics(std::move(statisticsString));
            }
        }
    }

    YT_LOG_DEBUG(
        "Job statistics for agent prepared (RunningJobsStatisticsSize: %v, FinishedJobsStatisticsSize: %v, "
        "RunningJobCount: %v, SkippedJobCountDueToBackoff: %v, SkippedJobCountDueToStatisticsSizeThrottling: %v)",
        runningJobsStatisticsSize,
        finishedJobsStatisticsSize,
        std::size(jobs),
        std::ssize(jobs) - consideredRunnigJobCount,
        consideredRunnigJobCount - reportedRunningJobCount);

    HeartbeatInfo_.LastSentHeartbeatTime = TInstant::Now();

    if (ControllerAgentConnectorPool_->TestHeartbeatDelay_) {
        TDelayedExecutor::WaitForDuration(ControllerAgentConnectorPool_->TestHeartbeatDelay_);
    }

    auto requestFuture = request->Invoke();
    YT_LOG_INFO(
        "Heartbeat sent to agent (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);

    auto responseOrError = WaitFor(std::move(requestFuture));
    if (!responseOrError.IsOK()) {
        HeartbeatInfo_.LastFailedHeartbeatTime = TInstant::Now();
        if (HeartbeatInfo_.FailedHeartbeatBackoffTime == TDuration::Zero()) {
            HeartbeatInfo_.FailedHeartbeatBackoffTime = ControllerAgentConnectorPool_->CurrentConfig_->FailedHeartbeatBackoffStartTime;
        } else {
            HeartbeatInfo_.FailedHeartbeatBackoffTime = std::min(
                HeartbeatInfo_.FailedHeartbeatBackoffTime * ControllerAgentConnectorPool_->CurrentConfig_->FailedHeartbeatBackoffMultiplier,
                ControllerAgentConnectorPool_->CurrentConfig_->FailedHeartbeatBackoffMaxTime);
        }
        YT_LOG_ERROR(responseOrError, "Error reporting heartbeat to agent (AgentAddress: %v, BackoffTime: %v)",
            ControllerAgentDescriptor_.Address,
            HeartbeatInfo_.FailedHeartbeatBackoffTime);
        
        if (responseOrError.GetCode() == NControllerAgent::EErrorCode::IncarnationMismatch) {
            OnAgentIncarnationOutdated();
        }
        return;
    }

    for (const auto& job : sentEnqueuedJobs) {
        EnqueuedFinishedJobs_.erase(job);
    }

    YT_LOG_INFO(
        "Successfully reported heartbeat to agent (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);

    HeartbeatInfo_.FailedHeartbeatBackoffTime = TDuration::Zero();
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnAgentIncarnationOutdated() noexcept
{
    VERIFY_INVOKER_THREAD_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker(), JobThread);

    HeartbeatExecutor_->Stop();
}

////////////////////////////////////////////////////////////////////////////////

TControllerAgentConnectorPool::TControllerAgentConnectorPool(
    TControllerAgentConnectorConfigPtr config,
    IBootstrap* const bootstrap)
    : StaticConfig_(std::move(config))
    , CurrentConfig_(CloneYsonSerializable(StaticConfig_))
    , Bootstrap_(bootstrap)
{ }

void TControllerAgentConnectorPool::SendOutOfBandHeartbeatsIfNeeded()
{
    for (auto& [agentDescriptor, controllerAgentConnector] : ControllerAgentConnectors_) {
        controllerAgentConnector->SendOutOfBandHeartbeatIfNeeded();
    }
}

TControllerAgentConnectorPool::TControllerAgentConnectorLease TControllerAgentConnectorPool::CreateLeaseOnControllerAgentConnector(
    const TJob* job)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    if (const auto it = ControllerAgentConnectors_.find(job->GetControllerAgentDescriptor());
        it != std::cend(ControllerAgentConnectors_))
    {
        auto result = MakeStrong(it->second);
        YT_VERIFY(result);
        return result;
    }

    auto controllerAgentConnector = New<TControllerAgentConnector>(this, job->GetControllerAgentDescriptor());

    EmplaceOrCrash(ControllerAgentConnectors_, job->GetControllerAgentDescriptor(), controllerAgentConnector.Get());

    return controllerAgentConnector;
}

void TControllerAgentConnectorPool::OnDynamicConfigChanged(
    const TExecNodeDynamicConfigPtr& oldConfig,
    const TExecNodeDynamicConfigPtr& newConfig)
{
    if (!newConfig->ControllerAgentConnector && !oldConfig->ControllerAgentConnector) {
        return;
    }

    Bootstrap_->GetJobInvoker()->Invoke(BIND([this, this_{MakeStrong(this)}, newConfig{std::move(newConfig)}] {
        if (newConfig->ControllerAgentConnector) {
            TestHeartbeatDelay_ = newConfig->ControllerAgentConnector->TestHeartbeatDelay;
            CurrentConfig_ = StaticConfig_->ApplyDynamic(newConfig->ControllerAgentConnector);
        } else {
            TestHeartbeatDelay_ = TDuration{};
            CurrentConfig_ = StaticConfig_;
        }
        OnConfigUpdated();
    }));
}

bool TControllerAgentConnectorPool::AreHeartbeatsEnabled() const noexcept
{
    return HeartbeatsEnabled_;
}

void TControllerAgentConnectorPool::OnControllerAgentConnectorDestroyed(
        const TControllerAgentDescriptor& controllerAgentDescriptor) noexcept
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    EraseOrCrash(ControllerAgentConnectors_, controllerAgentDescriptor);
}

IChannelPtr TControllerAgentConnectorPool::CreateChannel(const TControllerAgentDescriptor& agentDescriptor)
{
    const auto& client = Bootstrap_->GetMasterClient();
    const auto& channelFactory = client->GetNativeConnection()->GetChannelFactory();
    return channelFactory->CreateChannel(agentDescriptor.Address);
}

IChannelPtr TControllerAgentConnectorPool::GetOrCreateChannel(
    const TControllerAgentDescriptor& agentDescriptor)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
    
    if (const auto it = ControllerAgentConnectors_.find(agentDescriptor); it != std::cend(ControllerAgentConnectors_)) {
        return it->second->GetChannel();
    }

    return CreateChannel(agentDescriptor);
}

void TControllerAgentConnectorPool::EnqueueFinishedJob(const TJobPtr& job)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    auto controllerAgentConnector = GetOrCrash(ControllerAgentConnectors_, job->GetControllerAgentDescriptor());
    controllerAgentConnector->EnqueueFinishedJob(job);
}

void TControllerAgentConnectorPool::OnConfigUpdated()
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    HeartbeatsEnabled_ = CurrentConfig_->EnableHeartbeats;

    for (const auto& [agentDescriptor, controllerAgentConnector] : ControllerAgentConnectors_) {
        controllerAgentConnector->OnConfigUpdated();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
