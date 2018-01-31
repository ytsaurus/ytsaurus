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
            // TODO(babenko): agent id
            .AddTag("Kind: AgentToSchedulerOperations, IncarnationId: %v", IncarnationId_))
    , JobEventsInbox_(
        NLogging::TLogger(SchedulerLogger)
            // TODO(babenko): agent id
            .AddTag("Kind: AgentToSchedulerJobs, IncarnationId: %v", IncarnationId_))
    , ScheduleJobResponsesInbox_(
        NLogging::TLogger(SchedulerLogger)
            // TODO(babenko): agent id
            .AddTag("Kind: AgentToSchedulerScheduleJobResponses, IncarnationId: %v", IncarnationId_))
    , JobEventsOutbox_(New<TMessageQueueOutbox<TSchedulerToAgentJobEvent>>(
        NLogging::TLogger(SchedulerLogger)
            // TODO(babenko): agent id
            .AddTag("Kind: SchedulerToAgentJobs, IncarnationId: %v", IncarnationId_)))
    , OperationEventsOutbox_(New<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>(
        NLogging::TLogger(SchedulerLogger)
            // TODO(babenko): agent id
            .AddTag("Kind: SchedulerToAgentOperations, IncarnationId: %v", IncarnationId_)))
    , ScheduleJobRequestsOutbox_(New<TMessageQueueOutbox<TScheduleJobRequestPtr>>(
        NLogging::TLogger(SchedulerLogger)
            // TODO(babenko): agent id
            .AddTag("Kind: SchedulerToAgentScheduleJobRequests, IncarnationId: %v", IncarnationId_)))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
