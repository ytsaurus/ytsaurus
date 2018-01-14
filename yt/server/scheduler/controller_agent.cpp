#include "controller_agent.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent(const NControllerAgent::TIncarnationId& incarnationId)
    : IncarnationId_(incarnationId)
    , SuspiciousJobsYson_(NYson::TYsonString(TString(), NYson::EYsonType::MapFragment))
    , OperationEventsInbox_(NLogging::TLogger(SchedulerLogger)
        // TODO(babenko): agent id
        .AddTag("Kind: AgentToSchedulerOperations, IncarnationId: %v", IncarnationId_))
    , JobEventsInbox_(NLogging::TLogger(SchedulerLogger)
        // TODO(babenko): agent id
        .AddTag("Kind: AgentToSchedulerJobs, IncarnationId: %v", IncarnationId_))
    , JobEventsOutbox_(New<TMessageQueueOutbox<TJobEvent>>(NLogging::TLogger(SchedulerLogger)
        // TODO(babenko): agent id
        .AddTag("Kind: SchedulerToAgentJobs, IncarnationId: %v", IncarnationId_)))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
