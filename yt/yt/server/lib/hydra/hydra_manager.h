#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/server/lib/election/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

// Hydra interface is divided into two parts. ISimpleHydraManager is a subset of interface as seen from the
// composite automaton (in particular, from tablet write code), while IHydraManager is a full-fledged interface.
// The first one is extracted in order to simplify unit-testing.

////////////////////////////////////////////////////////////////////////////////

struct ISimpleHydraManager
    : public virtual TRefCounted
{
    //! Commits a mutation.
    /*!
     *  If the automaton is in read-only state then #EErrorCode::ReadOnly is returned.
     *  If the peer is not an active leader then #EErrorCode::InvalidState is returned.
     *
     *  \note Thread affinity: AutomatonThread
     */
    virtual TFuture<TMutationResponse> CommitMutation(TMutationRequest&& request) = 0;

    virtual TReign GetCurrentReign() = 0;

    //! Returns the state as seen in the automaton thread.
    /*!
     *  \note Thread affinity: any
     */
    virtual EPeerState GetAutomatonState() const = 0;

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
     *  One should use #SyncWithLeader to workaround stale reads.
     *
     *  \note Thread affinity: any
     */
    virtual bool IsActiveFollower() const = 0;

    //! Returns the cancelable context for the current epoch, as viewed by the automaton thread.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual TCancelableContextPtr GetAutomatonCancelableContext() const = 0;

    //! Returns the id of the current epoch (null if none), as viewed by the automaton thread.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual TEpochId GetAutomatonEpochId() const = 0;

    //! Returns current term (#InvalidTerm if none), as viewed by the automaton thread.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual int GetAutomatonTerm() const = 0;

    //! Applies changes to Hydra config.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual TFuture<void> Reconfigure(TDynamicDistributedHydraManagerConfigPtr config) = 0;

    //! Raised within the automaton thread when the peer has started leading
    //! and enters recovery.
    DECLARE_INTERFACE_SIGNAL(void(), StartLeading);
    //! Raised within the automaton thread when the leader recovery is complete.
    //! The leader may now serve read requests.
    DECLARE_INTERFACE_SIGNAL(void(), AutomatonLeaderRecoveryComplete);
    //! Raised within the control thread when the leader recovery is complete.
    DECLARE_INTERFACE_SIGNAL(void(), ControlLeaderRecoveryComplete);
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
    DECLARE_INTERFACE_SIGNAL(void(), AutomatonFollowerRecoveryComplete);
    //! Raised within the automaton thread when the follower recovery is complete.
    DECLARE_INTERFACE_SIGNAL(void(), ControlFollowerRecoveryComplete);
    //! Raised within the automaton thread when the peer has stopped following.
    DECLARE_INTERFACE_SIGNAL(void(), StopFollowing);


    // Extension methods.
    bool IsLeader() const;
    bool IsFollower() const;
    bool IsRecovery() const;
    bool IsActive() const;
    void ValidatePeer(EPeerKind kind);
};

DEFINE_REFCOUNTED_TYPE(ISimpleHydraManager)

////////////////////////////////////////////////////////////////////////////////

struct IHydraManager
    : public ISimpleHydraManager
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
     *  \note Thread affinity: any
     */
    virtual EPeerState GetControlState() const = 0;

    //! Returns the current automaton version.
    /*!
     *  \note Thread affinity: any
     */
    virtual TVersion GetAutomatonVersion() const = 0;

    //! Returns a wrapper invoker used for accessing the automaton.
    /*!
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr CreateGuardedAutomatonInvoker(IInvokerPtr underlyingInvoker) = 0;

    //! Returns the cancelable context for the current epoch, as viewed by the control thread.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual TCancelableContextPtr GetControlCancelableContext() const = 0;

    //! Synchronizes with the leader.
    /*!
     *  Used to prevent stale reads at followers by ensuring that the automaton
     *  has seen enough mutations from leader.
     *
     *  Synchronization has no effect at leader.
     *
     *  \note Thread affinity: any
     */
    virtual TFuture<void> SyncWithLeader() = 0;

    //! Starts a distributed snapshot build operation.
    //! Once finished, returns the snapshot id.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual TFuture<int> BuildSnapshot(bool setReadOnly, bool waitForSnapshotCompletion) = 0;

    //! Returns the callback for producing the monitoring info.
    /*!
     *  \note Thread affinity: any
     */
    virtual NYson::TYsonProducer GetMonitoringProducer() = 0;

    //! Returns the set of peers that are known to be alive.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    virtual NElection::TPeerIdSet GetAlivePeerIds() = 0;

    virtual bool GetReadOnly() const = 0;

    virtual bool IsDiscombobulated() const = 0;

    virtual i64 GetSequenceNumber() const = 0;

    //! Raised during periodic leader lease checks.
    //! A subscriber must start an appropriate check and return a future
    //! summarizing its outcome.
    DECLARE_INTERFACE_SIGNAL(TFuture<void>(), LeaderLeaseCheck);
};

DEFINE_REFCOUNTED_TYPE(IHydraManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
