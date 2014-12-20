#pragma once

#include "public.h"

#include <core/actions/signal.h>
#include <core/actions/future.h>

#include <core/misc/error.h>

#include <core/ytree/public.h>

#include <server/election/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct IHydraManager
    : public virtual TRefCounted
{
    //! Activates the instance.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual void Start() = 0;

    //! Deactivates the instance.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual void Stop() = 0;

    //! Returns the state as seen in the control thread.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual EPeerState GetControlState() const = 0;

    //! Returns the state as seen in the automaton thread.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual EPeerState GetAutomatonState() const = 0;

    //! Returns a wrapper invoker used for accessing the automaton.
    /*!
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr CreateGuardedAutomatonInvoker(IInvokerPtr underlyingInvoker) = 0;

    //! Returns |true| if the peer is a leader ready to carry out distributed commits.
    /*!
     *  \note Thread affinity: any
     */
    virtual bool IsActiveLeader() const = 0;

    //! Returns the current epoch context, as viewed by the Control Thread.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual NElection::TEpochContextPtr GetControlEpochContext() const = 0;

    //! Returns the current epoch context, as viewed by the Automaton Thread.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual NElection::TEpochContextPtr GetAutomatonEpochContext() const = 0;

    //! Commits a mutation.
    /*!
     *  If the peer is not the leader then #EErrorCode::InvalidState is returned.
     *  If the peer is the leader but has no active quorum, then #EErrorCode::NoQuorum is returned.
     *  If the automaton is in read-only state, then #EErrorCode::ReadOnly is returned.
     *
     *  \note Thread affinity: AutomatonThread
     */
    virtual TFuture<TErrorOr<TMutationResponse>> CommitMutation(const TMutationRequest& request) = 0;

    //! Returns the current mutation context or |nullptr| if no mutation is currently being applied.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual TMutationContext* GetMutationContext() = 0;

    //! Returns |true| if a mutation is currently being applied.
    /*!
     *  The method could be useful to prevent recursive commits and only log "top-level"
     *  mutations that trigger the whole transformation chain.
     *
     *  \note Thread affinity: AutomatonThread
     */
    virtual bool IsMutating() = 0;

    //! Returns |true| if read-only mode is active.
    /*!
     *  \note Thread affinity: any
     */
    virtual bool GetReadOnly() const = 0;

    //! Toggles read-only mode.
    /*!
     *  \note Thread affinity: any
     */
    virtual void SetReadOnly(bool value) = 0;

    //! Starts a distributed snapshot build operation.
    //! Once finished, returns the snapshot id.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual TFuture<TErrorOr<int>> BuildSnapshotDistributed() = 0;

    //! Produces monitoring info.
    /*!
     *  \note Thread affinity: any
     */
    virtual NYTree::TYsonProducer GetMonitoringProducer() = 0;

    //! Raised within the automaton thread when the peer has started leading
    //! and enters recovery.
    DECLARE_INTERFACE_SIGNAL(void(), StartLeading);
    //! Raised within the automaton thread when the leader recovery is complete.
    //! The leader may now serve read requests.
    DECLARE_INTERFACE_SIGNAL(void(), LeaderRecoveryComplete);
    //! Raised within the automaton thread when an active quorum is established.
    //! The leader may now serve read-write requests.
    DECLARE_INTERFACE_SIGNAL(void(), LeaderActive);
    //! Raised within the automaton thread when the peer has stopped leading.
    DECLARE_INTERFACE_SIGNAL(void(), StopLeading);

    //! Raised within the automaton thread when the peer has started following
    //! and enters recovery.
    DECLARE_INTERFACE_SIGNAL(void(), StartFollowing);
    //! Raised within the automaton thread when the follower recovery is complete.
    //! The follower may now serve read requests.
    DECLARE_INTERFACE_SIGNAL(void(), FollowerRecoveryComplete);
    //! Raised within the automaton thread when the peer has stopped following.
    DECLARE_INTERFACE_SIGNAL(void(), StopFollowing);

    // Extension methods.
    bool IsLeader() const;
    bool IsFollower() const;
    bool IsRecovery() const;

};

DEFINE_REFCOUNTED_TYPE(IHydraManager)

///////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
