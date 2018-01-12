#include "controller_agent.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent(const NControllerAgent::TIncarnationId& incarnationId)
    : IncarnationId_(incarnationId)
    , SuspiciousJobsYson_(NYson::TYsonString(TString(), NYson::EYsonType::MapFragment))
    , OperationEventsQueue_(NLogging::TLogger(SchedulerLogger)
        // TODO(babenko): agent id
        .AddTag("Kind: OperationEvents, IncarnationId: %v", IncarnationId_))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
