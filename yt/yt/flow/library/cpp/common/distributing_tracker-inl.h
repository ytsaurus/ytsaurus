#pragma once

#ifndef DISTRIBUTING_TRACKER_INL_H_
    #error "Direct inclusion of this file is not allowed, include distributing_tracker.h"
    // For the sake of sane code completion.
    #include "distributing_tracker.h"
#endif

#include <yt/yt/core/misc/error.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TOnDistributedCallback TOnDistributedCallback::FromCallback(T&& callback)
{
    auto tracker = TDistributingTracker(std::forward<T>(callback));
    auto onDistributedCallback = tracker.AddDestination();
    tracker.Activate();
    return onDistributedCallback;
}

////////////////////////////////////////////////////////////////////////////////

class TDistributionTrackerStateBase
    : public TRefCounted
{
private:
    friend class TDistributingTracker;
    friend class TOnDistributedCallback;

    // Number of destination callbacks + 1.
    // Every AddDestination() increments Remaining and returned callback decrements it back.
    // Last `1` will be decremented by Activate().
    std::atomic<int> Remaining{1};

    // Method to be called after Remaining becomes zero.
    virtual void CallFinalCallback() = 0;

    // Non-virtual decrement implementation
    void Decrement()
    {
        if (Remaining.fetch_sub(1) == 1) {
            CallFinalCallback();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TFinalCallback>
class TDistributionTrackerState
    : public TDistributionTrackerStateBase
{
public:
    template <class T>
    TDistributionTrackerState(T&& finalCallback)
        : FinalCallback_(std::forward<T>(finalCallback))
    { }

private:
    TFinalCallback FinalCallback_;

    void CallFinalCallback() override
    {
        FinalCallback_();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TFinalCallback>
TDistributingTracker::TDistributingTracker(TFinalCallback&& finalCallback)
    : State_(New<TDistributionTrackerState<std::remove_cvref_t<TFinalCallback>>>(std::forward<TFinalCallback>(finalCallback)))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
