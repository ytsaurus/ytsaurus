#include "controller_agent.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent(
    const TString& id,
    const NNodeTrackerClient::TAddressMap& agentAddresses,
    NRpc::IChannelPtr channel,
    const NControllerAgent::TIncarnationId& incarnationId)
    : SuspiciousJobsYson_(NYson::TYsonString(TString(), NYson::EYsonType::MapFragment))
    , OperationEventsInbox_(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: AgentToSchedulerOperations, AgentId: %v, IncarnationId: %v",
                id,
                incarnationId))
    , JobEventsInbox_(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: AgentToSchedulerJobs, AgentId: %v, IncarnationId: %v",
                id,
                incarnationId))
    , ScheduleJobResponsesInbox_(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: AgentToSchedulerScheduleJobResponses, AgentId: %v, IncarnationId: %v",
                id,
                incarnationId))
    , JobEventsOutbox_(New<TMessageQueueOutbox<TSchedulerToAgentJobEvent>>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: SchedulerToAgentJobs, AgentId: %v, IncarnationId: %v",
                id,
                incarnationId)))
    , OperationEventsOutbox_(New<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: SchedulerToAgentOperations, AgentId: %v, IncarnationId: %v",
                id,
                incarnationId)))
    , ScheduleJobRequestsOutbox_(New<TMessageQueueOutbox<TScheduleJobRequestPtr>>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: SchedulerToAgentScheduleJobRequests, AgentId: %v, IncarnationId: %v",
                id,
                incarnationId)))
    , Id_(id)
    , AgentAddresses_(agentAddresses)
    , Channel_(std::move(channel))
    , IncarnationId_(incarnationId)
{ }

const TAgentId& TControllerAgent::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

const NNodeTrackerClient::TAddressMap& TControllerAgent::GetAgentAddresses() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return AgentAddresses_;
}

const NRpc::IChannelPtr& TControllerAgent::GetChannel() const
{
    return Channel_;
}

const TIncarnationId& TControllerAgent::GetIncarnationId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IncarnationId_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
