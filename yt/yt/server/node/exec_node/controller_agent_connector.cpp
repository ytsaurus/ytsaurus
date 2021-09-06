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

TControllerAgentConnector::TControllerAgentConnector(
    TControllerAgentConnectorConfigPtr config,
    IBootstrap* const bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , HeartbeatExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TControllerAgentConnector::SendHeartbeats, MakeWeak(this)),
        Config_->HeartbeatPeriod,
        Config_->HeartbeatSplay))
    , ScanOutdatedAgentIncarnationsExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TControllerAgentConnector::ScanOutdatedAgentIncarnations, MakeWeak(this)),
        Config_->OutdatedIncarnationScanPeriod,
        Config_->OutdatedIncarnationScanPeriod))
{ }

IChannelPtr TControllerAgentConnector::CreateChannel(const TControllerAgentDescriptor& agentDescriptor)
{
    const auto& client = Bootstrap_->GetMasterClient();
    const auto& channelFactory = client->GetNativeConnection()->GetChannelFactory();
    return channelFactory->CreateChannel(agentDescriptor.Address);
}

IChannelPtr TControllerAgentConnector::GetChannel(
    const TControllerAgentDescriptor& agentDescriptor)
{
    if (const auto it = ControllerAgentChannels_.find(agentDescriptor.Address); it != std::cend(ControllerAgentChannels_)) {
        return it->second;
    }

    const auto [it, inserted] = ControllerAgentChannels_.emplace(agentDescriptor.Address, CreateChannel(agentDescriptor));

    return it->second;
}

void TControllerAgentConnector::SendHeartbeats()
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    if (!Bootstrap_->IsConnected()) {
        return;
    }

    THashMap<TControllerAgentDescriptor, std::vector<NJobAgent::IJobPtr>> groupedJobs;
    for (auto&& job : Bootstrap_->GetJobController()->GetJobs()) {
        if (TypeFromId(job->GetId()) != EObjectType::SchedulerJob) {
            continue;
        }

        if (job->GetState() != EJobState::Running) {
            continue;
        }

        auto& schedulerJob = static_cast<TJob&>(*job);
        const auto& controllerAgentDescriptor = schedulerJob.GetControllerAgentDescriptor();

        if (!controllerAgentDescriptor) {
            YT_LOG_DEBUG(
                "Skipping heartbeat for job since old agent incarnation is outdated and new incarnation is not received yet (JobId: %v)",
                job->GetId());
            continue;
        }

        if (IsAgentIncarnationOutdated(controllerAgentDescriptor)) {
            schedulerJob.UpdateControllerAgentDescriptor({});
            YT_LOG_INFO(
                "Skipping heartbeat to agent %v since incarnation is outdated (IncarnationId: %v, JobId: %v)",
                controllerAgentDescriptor.Address,
                controllerAgentDescriptor.IncarnationId,
                job->GetId());
            continue;
        }

        groupedJobs[controllerAgentDescriptor].push_back(std::move(job));
    }

    ResetOutdatedAgentIncarnations();

    std::vector<TFuture<TJobTrackerServiceProxy::TRspHeartbeatPtr>> requests;
    std::vector<const TControllerAgentDescriptor*> agentsHeartbeatSentTo;
    requests.reserve(std::size(groupedJobs));
    agentsHeartbeatSentTo.reserve(std::size(groupedJobs));

    for (const auto& [agentDescriptor, jobs] : groupedJobs) {
        auto& heartbeatInfo = HeartbeatInfo_[agentDescriptor];
        if (TInstant::Now() < heartbeatInfo.LastFailedHeartbeatTime_ + heartbeatInfo.FailedHeartbeatBackoffTime_) {
            YT_LOG_INFO(
                "Skipping heartbeat to agent %v since backoff after previous heartbeat failure (IncarnationId: %v)",
                agentDescriptor.Address,
                agentDescriptor.IncarnationId);
            continue;
        }

        TJobTrackerServiceProxy proxy(GetChannel(agentDescriptor));

        auto request = proxy.Heartbeat();

        request->set_node_id(Bootstrap_->GetNodeId());
        ToProto(request->mutable_node_descriptor(), Bootstrap_->GetLocalDescriptor());
        ToProto(request->mutable_controller_agent_incarnation_id(), agentDescriptor.IncarnationId);

        for (const auto& job : jobs) {
            auto* const jobStatus = request->add_jobs();

            FillJobStatus(jobStatus, job);

            if (auto statistics = job->GetStatistics()) {
                job->ResetStatisticsLastSendTime();
                jobStatus->set_statistics(statistics.ToString());
            }
        }

        heartbeatInfo.LastSentHeartbeatTime_ = TInstant::Now();
        requests.push_back(request->Invoke());
        YT_LOG_INFO(
            "Heartbeat sent to agent %v (IncarnationId: %v)",
            agentDescriptor.Address,
            agentDescriptor.IncarnationId);

        agentsHeartbeatSentTo.push_back(&agentDescriptor);
    }

    const auto responsesOrError = WaitFor(AllSet(std::move(requests)));
    YT_VERIFY(responsesOrError.IsOK());
    const auto& responses = responsesOrError.Value();

    for (int index = 0; index < std::ssize(responses); ++index) {
        auto& agentDescriptorPtr = agentsHeartbeatSentTo[index];
        auto& rspOrError = responses[index];

        auto& heartbeatInfo = HeartbeatInfo_[*agentDescriptorPtr];
        if (!rspOrError.IsOK()) {
            heartbeatInfo.LastFailedHeartbeatTime_ = TInstant::Now();
            if (heartbeatInfo.FailedHeartbeatBackoffTime_ == TDuration::Zero()) {
                heartbeatInfo.FailedHeartbeatBackoffTime_ = Config_->FailedHeartbeatBackoffStartTime;
            } else {
                heartbeatInfo.FailedHeartbeatBackoffTime_ = std::min(
                    heartbeatInfo.FailedHeartbeatBackoffTime_ * Config_->FailedHeartbeatBackoffMultiplier,
                    Config_->FailedHeartbeatBackoffMaxTime);
            }
            YT_LOG_ERROR(rspOrError, "Error reporting heartbeat to agent %v (BackoffTime: %v)",
                agentDescriptorPtr->Address,
                heartbeatInfo.FailedHeartbeatBackoffTime_);
            
            if (rspOrError.GetCode() == NControllerAgent::EErrorCode::IncarnationMismatch) {
                MarkAgentIncarnationAsOutdated(*agentDescriptorPtr);
            }
            continue;
        }

        YT_LOG_INFO("Successfully reported heartbeat to agent %v (IncarnationId: %v)", agentDescriptorPtr->IncarnationId);

        heartbeatInfo.FailedHeartbeatBackoffTime_ = TDuration::Zero();
    }
}

void TControllerAgentConnector::ScanOutdatedAgentIncarnations()
{
    const auto now = TInstant::Now();
    std::vector<decltype(HeartbeatInfo_)::iterator> agentsWithOutdatedIncarnation;
    for (auto it = std::begin(HeartbeatInfo_); it != std::cend(HeartbeatInfo_); ++it) {
        const auto& heartbeatInfo = it->second;
        if (now > heartbeatInfo.LastFailedHeartbeatTime_ + Config_->OutdatedIncarnationScanPeriod &&
            now > heartbeatInfo.LastSentHeartbeatTime_ + Config_->OutdatedIncarnationScanPeriod)
        {
            agentsWithOutdatedIncarnation.push_back(it);
        }
    }

    for (const auto agentIterator : agentsWithOutdatedIncarnation) {
        ControllerAgentChannels_.erase(agentIterator->first.Address);

        HeartbeatInfo_.erase(agentIterator);
    }
}

void TControllerAgentConnector::MarkAgentIncarnationAsOutdated(const TControllerAgentDescriptor& agentDescriptor)
{
    YT_VERIFY(OutdatedAgentIncarnations_.insert(agentDescriptor).second);
}

bool TControllerAgentConnector::IsAgentIncarnationOutdated(const TControllerAgentDescriptor& agentDescriptor)
{
    const auto it = OutdatedAgentIncarnations_.find(agentDescriptor);

    return it != std::cend(OutdatedAgentIncarnations_);
}

void TControllerAgentConnector::ResetOutdatedAgentIncarnations()
{
    OutdatedAgentIncarnations_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
