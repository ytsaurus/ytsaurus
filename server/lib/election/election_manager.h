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
 *   Between these pairs may come calls to #OnStopVoting.
 *
 *   Thread affinity: ControlThread
 */
struct IElectionCallbacks
    : public virtual TRefCounted
{
    //! Called when the current peer has started leading.
    virtual void OnStartLeading(TEpochContextPtr epochContext) = 0;

    //! Called when the current peer has stopped leading.
    virtual void OnStopLeading(const TError& error) = 0;

    //! Called when the current peer has started following.
    virtual void OnStartFollowing(TEpochContextPtr epochContext) = 0;

    //! Called when the current peer has stopped following.
    virtual void OnStopFollowing(const TError& error) = 0;

    //! Called when the current peer has stopped voting.
    virtual void OnStopVoting(const TError& error) = 0;

    //! Called when the current peer's priority is needed for voting.
    virtual TPeerPriority GetPriority() = 0;

    //! Enables pretty-printing peer priorities in logs.
    virtual TString FormatPriority(TPeerPriority priority) = 0;

    //! Called when the set of alive peers changes. Only called on the leader.
    /*!
     *  This means the calls may only occur between #OnStartLeading and #OnStopLeading.
     *  Consequently, one should always consider subscribing to all three of these callbacks.
     *
     *  The peer set includes both voting and non-voting peers.
     */
    virtual void OnAlivePeerSetChanged(const TPeerIdSet& alivePeers) = 0;
};

DEFINE_REFCOUNTED_TYPE(IElectionCallbacks)

////////////////////////////////////////////////////////////////////////////////

//! Represents an abstract election system.
/*
 *   Thread affinity: ControlThread (unless noted otherwise)
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
    virtual void Abandon(const TError& error) = 0;

    //! Replaces the Cell Manager. Restarts the epoch.
    /*!
     *  Cell id cannot be changed.
     */
    virtual void ReconfigureCell(TCellManagerPtr cellManager) = 0;

    //! Returns the callback for producing the monitoring info.
    /*!
     *  Thread affinity: any
     */
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

    //! Cell Manager for this epoch. 
    TCellManagerPtr CellManager;
};

DEFINE_REFCOUNTED_TYPE(TEpochContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
