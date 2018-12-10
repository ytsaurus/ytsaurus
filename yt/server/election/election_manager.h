#pragma once

#include "public.h"

#include <yt/core/yson/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/actions/cancelable_context.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

//! Provides hooks for election clients enabling them listening
//! to various state transitions.
/*!
 *   Calls to
 *   #OnStartLeading and #OnStopLeading,
 *   #OnStartFollowing and #OnStopFollowing
 *   always come in pairs and do not overlap.
 *
 *   Thread affinity: any
 */
struct IElectionCallbacks
    : public virtual TRefCounted
{
    //! Called when the current peer has started leading.
    virtual void OnStartLeading(TEpochContextPtr epochContext) = 0;

    //! Called when the current peer has stopped leading.
    virtual void OnStopLeading() = 0;

    //! Called when the current peer has started following.
    virtual void OnStartFollowing(TEpochContextPtr epochContext) = 0;

    //! Called when the current peer has stopped following.
    virtual void OnStopFollowing() = 0;

    //! Called when the current peer's priority is needed for voting.
    virtual TPeerPriority GetPriority() = 0;

    //! Enables pretty-printing peer priorities in logs.
    virtual TString FormatPriority(TPeerPriority priority) = 0;
};

DEFINE_REFCOUNTED_TYPE(IElectionCallbacks)

////////////////////////////////////////////////////////////////////////////////

//! Represents an abstract election system.
/*!
 *  Thread affinity: any
 */
struct IElectionManager
    : public virtual TRefCounted
{
    //! Activates the instance.
    virtual void Initialize() = 0;

    //! Deactivates the instance.
    virtual void Finalize() = 0;

    //! Initiates elections.
    /*!
     *  The current epoch, if any, is abandoned.
     *
     *  Upon success, IElectionCallbacks::OnStartLeading or
     *  IElectionCallbacks::OnStartFollowing
     *  is called.
     */
    virtual void Participate() = 0;

    //! Stops the elections, terminates the current epoch (if any).
    /*!
     *  The implementation ensures that all relevant stop-notifications
     *  will be issued via IElectionCallbacks.
     */
    virtual void Abandon() = 0;

    //! Returns the callback for producing the monitoring info.
    virtual NYson::TYsonProducer GetMonitoringProducer() = 0;
};

DEFINE_REFCOUNTED_TYPE(IElectionManager)

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public TRefCounted
{
    //! The id of the leading peer (for this epoch).
    TPeerId LeaderId = InvalidPeerId;

    //! An internally used epoch id.
    TEpochId EpochId;

    //! Time when the epoch has started.
    TInstant StartTime;

    //! This context is canceled whenever the epoch ends.
    TCancelableContextPtr CancelableContext = New<TCancelableContext>();
};

DEFINE_REFCOUNTED_TYPE(TEpochContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
