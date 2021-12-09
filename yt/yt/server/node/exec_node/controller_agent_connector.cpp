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

    void UpdateConnectorPeriod(TDuration newPeriod);

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
        ControllerAgentConnectorPool_->StaticConfig_->HeartbeatPeriod,
        ControllerAgentConnectorPool_->StaticConfig_->HeartbeatSplay))
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

void TControllerAgentConnectorPool::TControllerAgentConnector::UpdateConnectorPeriod(const TDuration newPeriod)
{
    VERIFY_THREAD_AFFINITY_ANY();

    HeartbeatExecutor_->SetPeriod(newPeriod);
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

    jobs.reserve(std::size(jobs) + std::size(EnqueuedFinishedJobs_));
    for (const auto job : EnqueuedFinishedJobs_) {
        jobs.push_back(job);
        sentEnqueuedJobs.push_back(job);
    }

    TJobTrackerServiceProxy proxy(Channel_);
    auto request = proxy.Heartbeat();

    request->set_node_id(ControllerAgentConnectorPool_->Bootstrap_->GetNodeId());
    ToProto(request->mutable_node_descriptor(), ControllerAgentConnectorPool_->Bootstrap_->GetLocalDescriptor());
    ToProto(request->mutable_controller_agent_incarnation_id(), ControllerAgentDescriptor_.IncarnationId);

    for (const auto& job : jobs) {
        auto* const jobStatus = request->add_jobs();

        FillJobStatus(jobStatus, job);

        if (auto statistics = job->GetStatistics()) {
            job->ResetStatisticsLastSendTime();
            jobStatus->set_statistics(statistics.ToString());
        }
    }

    HeartbeatInfo_.LastSentHeartbeatTime = TInstant::Now();

    YT_LOG_INFO(
        "Heartbeat sent to agent (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);

    if (ControllerAgentConnectorPool_->TestHeartbeatDelay_ != TDuration{}) {
        TDelayedExecutor::WaitForDuration(ControllerAgentConnectorPool_->TestHeartbeatDelay_);
    }    

    auto responseOrError = WaitFor(request->Invoke());
    if (!responseOrError.IsOK()) {
        HeartbeatInfo_.LastFailedHeartbeatTime = TInstant::Now();
        if (HeartbeatInfo_.FailedHeartbeatBackoffTime == TDuration::Zero()) {
            HeartbeatInfo_.FailedHeartbeatBackoffTime = ControllerAgentConnectorPool_->StaticConfig_->FailedHeartbeatBackoffStartTime;
        } else {
            HeartbeatInfo_.FailedHeartbeatBackoffTime = std::min(
                HeartbeatInfo_.FailedHeartbeatBackoffTime * ControllerAgentConnectorPool_->StaticConfig_->FailedHeartbeatBackoffMultiplier,
                ControllerAgentConnectorPool_->StaticConfig_->FailedHeartbeatBackoffMaxTime);
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
            HeartbeatsEnabled_ = newConfig->ControllerAgentConnector->EnableHeartbeats;
            TestHeartbeatDelay_ = newConfig->ControllerAgentConnector->TestHeartbeatDelay;
            CurrentConfig_ = StaticConfig_->ApplyDynamic(newConfig->ControllerAgentConnector);
        } else {
            HeartbeatsEnabled_ = true;
            TestHeartbeatDelay_ = TDuration{};
            CurrentConfig_ = StaticConfig_;
        }
        UpdateConnectorPeriods(CurrentConfig_->HeartbeatPeriod);
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

void TControllerAgentConnectorPool::UpdateConnectorPeriods(const TDuration newPeriod)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    for (const auto& [agentDescriptor, controllerAgentConnector] : ControllerAgentConnectors_) {
        controllerAgentConnector->UpdateConnectorPeriod(newPeriod);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
