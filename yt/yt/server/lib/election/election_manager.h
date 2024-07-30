#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/ytlib/election/public.h>

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

    //! Called when the current peer has entered discombobulated state.
    virtual void OnDiscombobulate(i64 sequenceNumber) = 0;
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
    virtual TFuture<void> Abandon(const TError& error) = 0;

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

    //! For leaders only: returns the set of alive peers.
    /*!
     *  This includes both voting and non-voting peers.
     */
    virtual TPeerIdSet GetAlivePeerIds() = 0;

    //! Returns current Cell Manager.
    virtual TCellManagerPtr GetCellManager() = 0;
};

DEFINE_REFCOUNTED_TYPE(IElectionManager)

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public TRefCounted
{
    explicit TEpochContext(TCellManagerPtr cellManager);

    //! Cell Manager for this epoch.
    const TCellManagerPtr CellManager;

    //! This context is canceled whenever the epoch ends.
    const TCancelableContextPtr CancelableContext;

    //! The id of the leading peer (for this epoch).
    int LeaderId = InvalidPeerId;

    //! An internally used epoch id.
    TEpochId EpochId;

    //! Time when the epoch has started.
    TInstant StartTime;

    //! Do not restart nonvoting peer in leader`s absence.
    bool Discombobulated = false;
};

DEFINE_REFCOUNTED_TYPE(TEpochContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
