#pragma once

#include <yt/yt/core/actions/callback.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDistributionTrackerStateBase)

////////////////////////////////////////////////////////////////////////////////

//! A lightweight move-only callback object returned by TDistributingTracker::AddDestination().
class TOnDistributedCallback
{
public:
    TOnDistributedCallback() = default;

    TOnDistributedCallback(TOnDistributedCallback&&) = default;
    TOnDistributedCallback& operator=(TOnDistributedCallback&&) = default;

    // Non-copyable
    TOnDistributedCallback(const TOnDistributedCallback&) = delete;
    TOnDistributedCallback& operator=(const TOnDistributedCallback&) = delete;

    //! Calls the underlying callback. Must be called at most once.
    void operator()();

    //! Returns true if the callback has not been called yet (and was not default-constructed).
    explicit operator bool() const;

    // Not efficient compatibility layer.
    template <class T>
    static TOnDistributedCallback FromCallback(T&& callback);

private:
    TDistributionTrackerStateBasePtr State_;

    friend class TDistributingTracker;
    TOnDistributedCallback(TDistributionTrackerStateBasePtr state);
};

////////////////////////////////////////////////////////////////////////////////

//! Tracks distribution of one output message to multiple destinations.
//!
//! Usage: call AddDestination() once per destination before calling Activate().
//! Each returned callback must be called if and only if the message was successfully delivered to that destination;
//! dropping a callback without calling it abandons completion (finalCallback will not be called).
class TDistributingTracker
{
public:
    TDistributingTracker() = default;

    template <class TFinalCallback>
    TDistributingTracker(TFinalCallback&& finalCallback);

    ~TDistributingTracker() = default;

    // Non-copyable, movable.
    TDistributingTracker(const TDistributingTracker&) = delete;
    TDistributingTracker& operator=(const TDistributingTracker&) = delete;
    TDistributingTracker(TDistributingTracker&&) = default;
    TDistributingTracker& operator=(TDistributingTracker&&) = default;

    //! Registers one more destination and returns a lightweight callback object.
    //! The callback object must be called if and only if the message was successfully
    //! distributed to this destination. Must not be called after Activate().
    TOnDistributedCallback AddDestination();

    //! Finalizes the tracker. |finalCallback| is called successfully once all
    //! callbacks returned by AddDestination() have been called. If any callback
    //! is dropped without being called, |finalCallback| is never called. Must be
    //! called exactly once.
    void Activate();

    explicit operator bool() const;

private:
    TDistributionTrackerStateBasePtr State_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define DISTRIBUTING_TRACKER_INL_H_
#include "distributing_tracker-inl.h"
#undef DISTRIBUTING_TRACKER_INL_H_
