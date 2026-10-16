#pragma once

#include "public.h"

#include <yt/yt/client/prerequisite_client/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

namespace NYT::NLockElection {

////////////////////////////////////////////////////////////////////////////////

//! A base interface for lock-based election managers.
/*!
 *  Both Cypress lock-based and chaos lease-based election managers
 *  implement this interface.
 */
struct ILockElectionManager
    : public TRefCounted
{
    //! Starts participating in leader election.
    virtual void Start() = 0;

    //! Stops participating; stops leading if currently leader.
    virtual TFuture<void> Stop() = 0;

    //! Returns whether the instance is participating in election.
    virtual bool IsActive() const = 0;

    //! Immediately stops leading without stopping the election manager.
    virtual TFuture<void> StopLeading() = 0;

    //! Returns the prerequisite ID for the current epoch; null ID if not leader.
    virtual NPrerequisiteClient::TPrerequisiteId GetPrerequisiteId() const = 0;

    //! Returns whether this instance is currently leading (may be stale).
    virtual bool IsLeader() const = 0;

    DECLARE_INTERFACE_SIGNAL(void(), LeadingStarted);
    DECLARE_INTERFACE_SIGNAL(void(), LeadingEnded);
};

DEFINE_REFCOUNTED_TYPE(ILockElectionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLockElection
