#include "helpers.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

bool IsFinishedState(EControllerState state)
{
    return state == NControllerAgent::EControllerState::Completed ||
        state == NControllerAgent::EControllerState::Failed ||
        state == NControllerAgent::EControllerState::Aborted;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
