#include "controller_agent.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent(const NControllerAgent::TIncarnationId& incarnationId)
    : IncarnationId_(incarnationId)
    , SuspiciousJobsYson_(NYson::TYsonString(TString(), NYson::EYsonType::MapFragment))
    , OperationEventsInbox_(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: AgentToSchedulerOperations, AgentAddress: %v, IncarnationId: %v",
                GetDefaultAddress(),
                IncarnationId_))
    , JobEventsInbox_(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: AgentToSchedulerJobs, AgentAddress: %v, IncarnationId: %v",
                GetDefaultAddress(),
                IncarnationId_))
    , ScheduleJobResponsesInbox_(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: AgentToSchedulerScheduleJobResponses, AgentAddress: %v, IncarnationId: %v",
                GetDefaultAddress(),
                IncarnationId_))
    , JobEventsOutbox_(New<TMessageQueueOutbox<TSchedulerToAgentJobEvent>>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: SchedulerToAgentJobs, AgentAddress: %v, IncarnationId: %v",
                GetDefaultAddress(),
                IncarnationId_)))
    , OperationEventsOutbox_(New<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: SchedulerToAgentOperations, AgentAddress: %v, IncarnationId: %v",
                GetDefaultAddress(),
                IncarnationId_)))
    , ScheduleJobRequestsOutbox_(New<TMessageQueueOutbox<TScheduleJobRequestPtr>>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: SchedulerToAgentScheduleJobRequests, AgentAddress: %v, IncarnationId: %v",
                GetDefaultAddress(),
                IncarnationId_)))
{ }

TString TControllerAgent::GetDefaultAddress() const
{
    // XXX(babenko)
    return "<TODO>";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
