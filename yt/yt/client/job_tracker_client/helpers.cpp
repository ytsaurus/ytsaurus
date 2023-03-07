#include "helpers.h"

namespace NYT::NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

bool IsJobFinished(EJobState state)
{
    return state >= EJobState::Completed;
}

bool IsJobInProgress(EJobState state)
{
    return !IsJobFinished(state);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
