#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/signal.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/public.h>

#include <yt/client/hydra/version.h>

#include <yt/server/election/public.h>

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
    virtual void Initialize() = 0;

    //! Deactivates the instance. The resulting future is set
    //! when the instance is fully stopped, e.g. the automaton thread
    //! will not receive any more callbacks.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual TFuture<void> Finalize() = 0;

    //! Returns the callbacks used by election system to coordinate
    //! multiple Hydra instances.
    virtual NElection::IElectionCallbacksPtr GetElectionCallbacks() = 0;

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

    //! Returns the current automaton version.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual TVersion GetAutomatonVersion() const = 0;

    //! Returns a wrapper invoker used for accessing the automaton.
    /*!
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr CreateGuardedAutomatonInvoker(IInvokerPtr underlyingInvoker) = 0;

    //! Returns |true| if the peer is a leader ready to carry out distributed commits.
    /*!
     *  This check also ensures that the leader has acquired and is still holding the lease.
     *
     *  \note Thread affinity: any
     */
    virtual bool IsActiveLeader() const = 0;

    //! Returns |true| if the peer is a follower ready to serve reads.
    /*!
     *  Any follower still can lag arbitrarily behind the leader.
     *  One should use #SyncWithUpstream to workaround stale reads.
     *
     *  \note Thread affinity: any
     */
    virtual bool IsActiveFollower() const = 0;

    //! Returns the cancelable context for the current epoch, as viewed by the Control Thread.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual TCancelableContextPtr GetControlCancelableContext() const = 0;

    //! Returns the cancelable context for the current epoch, as viewed by the Automaton Thread.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual TCancelableContextPtr GetAutomatonCancelableContext() const = 0;

    //! Synchronizes with the upstream.
    /*!
     *  Used to prevent stale reads by ensuring that the automaton has seen enough mutations
     *  from all "upstream" services.
     *
     *  Synchronization requests are automatically batched together.
     *
     *  Internally, this combines two means of synchronization:
     *  1) follower-with-leader synchronization
     *  2) custom synchronization
     *
     *  In both cases a certain "synchronizer" is invoked that returns a future that gets
     *  set when synchronization is complete.
     *
     *  Synchronizer (1) ensures that when invoked at follower at instant T,
     *  completes when the committed version becomes equal to or larger than
     *  the committed version at leader at T.
     *
     *  Synchronizer (1) has no effect at leader.
     *
     *  Synchronizers (2) are user-supplied (see UpstreamSync signal).
     *
     *  \note Thread affinity: AutomatonThread
     */
    virtual TFuture<void> SyncWithUpstream() = 0;

    //! Commits a mutation.
    /*!
     *  If the automaton is in read-only state then #EErrorCode::ReadOnly is returned.
     *  If the peer is not an active leader then #EErrorCode::InvalidState is returned.
     *
     *  \note Thread affinity: AutomatonThread
     */
    virtual TFuture<TMutationResponse> CommitMutation(TMutationRequest&& request) = 0;

    //! Starts a distributed snapshot build operation.
    //! Once finished, returns the snapshot id.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual TFuture<int> BuildSnapshot(bool setReadOnly) = 0;

    //! Returns the callback for producing the monitoring info.
    /*!
     *  \note Thread affinity: any
     */
    virtual NYson::TYsonProducer GetMonitoringProducer() = 0;

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

    //! Raised during periodic leader lease checks.
    //! A subscriber must start an appropriate check and return a future
    //! summarizing its outcome.
    DECLARE_INTERFACE_SIGNAL(TFuture<void>(), LeaderLeaseCheck);
    //! Raised during upstream sync..
    //! A subscriber must start an appropriate synchronization process and return a future
    //! that gets set when sync is reached.
    DECLARE_INTERFACE_SIGNAL(TFuture<void>(), UpstreamSync);

    // Extension methods.
    bool IsLeader() const;
    bool IsFollower() const;
    bool IsRecovery() const;
    bool IsActive() const;
    void ValidatePeer(EPeerKind kind);

};

DEFINE_REFCOUNTED_TYPE(IHydraManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
