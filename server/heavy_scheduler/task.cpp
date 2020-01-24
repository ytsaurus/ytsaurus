#include "task.h"

#include "private.h"

namespace NYP::NServer::NHeavyScheduler {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TTaskBase::TTaskBase(
    TGuid id,
    TInstant startTime)
    : Logger(TLogger(NHeavyScheduler::Logger)
        .AddTag("TaskId: %v", id))
    , State_(ETaskState::Active)
    , Id_(id)
    , StartTime_(startTime)
{ }

TGuid TTaskBase::GetId() const
{
    return Id_;
}

TInstant TTaskBase::GetStartTime() const
{
    return StartTime_;
}

ETaskState TTaskBase::GetState() const
{
    return State_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
