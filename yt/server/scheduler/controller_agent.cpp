#include "controller_agent.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent()
    : SuspiciousJobsYson_(NYson::TYsonString(TString(), NYson::EYsonType::MapFragment))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
