#include "distributing_tracker.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TDistributionTrackerStateBase)

////////////////////////////////////////////////////////////////////////////////

TOnDistributedCallback::TOnDistributedCallback(TDistributionTrackerStateBasePtr state)
    : State_(std::move(state))
{ }

void TOnDistributedCallback::operator()()
{
    YT_VERIFY(State_, "TOnDistributedCallback called twice or after tracker destruction");
    State_->Decrement();
    State_ = {};
}

TOnDistributedCallback::operator bool() const
{
    return static_cast<bool>(State_);
}

////////////////////////////////////////////////////////////////////////////////

TOnDistributedCallback TDistributingTracker::AddDestination()
{
    YT_VERIFY(State_, "TDistributingTracker::AddDestination() called after Activate()");
    State_->Remaining.fetch_add(1);
    return TOnDistributedCallback(State_);
}

void TDistributingTracker::Activate()
{
    YT_VERIFY(State_, "TDistributingTracker::Activate() called twice");
    State_->Decrement();
    State_ = {};
}

TDistributingTracker::operator bool() const
{
    return static_cast<bool>(State_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
