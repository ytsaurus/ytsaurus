#pragma once

#include "public.h"
#include "mutation_context.h"

#include <ytlib/actions/signal.h>
#include <ytlib/actions/cancelable_context.h>

#include <ytlib/misc/ref.h>
#include <ytlib/misc/error.h>

#include <ytlib/ytree/public.h>

// TODO(babenko): replace with public.h
#include <ytlib/election/election_manager.h>
#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct IMetaStateManager
    : public virtual TRefCounted
{
    //! Boots up the manager.
    /*!
     *  \note Thread affinity: any
     */
    virtual void Start() = 0;

    //! Returns the status as seen in the control thread.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual EPeerStatus GetControlStatus() const = 0;

    //! Returns the status as seen in the state thread.
    /*!
     *  \note Thread affinity: StateThread
     */
    virtual EPeerStatus GetStateStatus() const = 0;

    //! Returns a wrapper invoker used for updating the state.
    /*!
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr CreateGuardedStateInvoker(IInvokerPtr underlyingInvoker) = 0;

    //! Returns True is the peer has a active quorum.
    /*!
     *  \note Thread affinity: any
     */
    virtual bool HasActiveQuorum() const = 0;

    //! Returns the epoch context of the aggregated election manager.
    /*!
     *  \note Thread affinity: any
     */
    virtual NElection::TEpochContextPtr GetEpochContext() const = 0;

    //! Returns the cell manager.
    virtual NElection::TCellManagerPtr GetCellManager() const = 0;

    //! Commits the mutation.
    /*!
     *  If the peer is not the leader then #ECommitResult::InvalidStatus is returned.
     *  If the peer is the leader but has no active quorum, then #ECommitResult::NotCommitted is returned.
     *  If the state is read-only, then #ECommitResult::ReadOnly is returned.
     *  
     *  \param mutationType A string describing the type of the mutation.
     *  \param mutationData A blob describing the mutation itself to be send to followers.
     *  \param mutationAction An optional action that is called to perform the mutation at the leader,
     *  if NULL then #IMetaState::ApplyMutation is invoked.
     *
     *  \note Thread affinity: StateThread
     */
    virtual TFuture< TValueOrError<TMutationResponse> > CommitMutation(const TMutationRequest& request) = 0;

    //! Returns the current mutation context or NULL if no mutation is currently being applied.
    /*!
     *  Checking the return value for NULL can be useful to prevent recursive commits and only log "top-level"
     *  mutations that trigger the whole transformation chain.
     *  
     *  \note Thread affinity: StateThread
     */
    virtual TMutationContext* GetMutationContext() = 0;

    //! Returns True if read only mode is active.
    /*!
     *  \note Thread affinity: any
     */
    virtual bool GetReadOnly() const = 0;

    //! Toggles read-only mode.
    /*!
     *  \note Thread affinity: any
     */
    virtual void SetReadOnly(bool value) = 0;

    //! Returns monitoring info.
    /*!
     *  \note Thread affinity: any
     */
    virtual void GetMonitoringInfo(NYTree::IYsonConsumer* consumer) = 0;

    //! Raised within the state thread when the state has started leading
    //! and enters recovery.
    DECLARE_INTERFACE_SIGNAL(void(), StartLeading);
    //! Raised within the state thread when the leader recovery is complete.
    //! The leader may now serve read requests.
    DECLARE_INTERFACE_SIGNAL(void(), LeaderRecoveryComplete);
    //! Raised within the stat thread when an active quorum is established.
    //! The leader may now serve read-write requests.
    DECLARE_INTERFACE_SIGNAL(void(), ActiveQuorumEstablished);
    //! Raised within the state thread when the peer has stopped leading.
    DECLARE_INTERFACE_SIGNAL(void(), StopLeading);

    //! Raised within the state thread when the state has started following
    //! and enters recovery.
    DECLARE_INTERFACE_SIGNAL(void(), StartFollowing);
    //! Raised within the state thread when the follower recovery is complete.
    //! The follower may now serve read requests.
    DECLARE_INTERFACE_SIGNAL(void(), FollowerRecoveryComplete);
    //! Raised within the state thread when the peer has stopped following.
    DECLARE_INTERFACE_SIGNAL(void(), StopFollowing);

    // Extension methods.
    bool IsLeader() const;
    bool IsFolllower() const;
    bool IsRecovery() const;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
