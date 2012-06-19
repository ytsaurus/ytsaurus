#pragma once

#include "public.h"
#include "meta_version.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/election/election_manager.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/signal.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! A common base for TFollowerCommitter and TLeaderCommitter.
class TCommitter
    : public TRefCounted
{
public:
    TCommitter(
        TDecoratedMetaState* metaState,
        IInvokerPtr epochControlInvoker,
        IInvokerPtr epochStateInvoker);

    ~TCommitter();

    DECLARE_ENUM(EResult,
        (Committed)
        (MaybeCommitted)
        (LateChanges)
        (OutOfOrderChanges)
    );

    typedef TFuture<EResult> TCommitResult;
    typedef TPromise<EResult> TCommitPromise;

protected:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    TDecoratedMetaStatePtr MetaState;
    IInvokerPtr EpochControlInvoker;
    IInvokerPtr EpochStateInvoker;
    NProfiling::TRateCounter CommitCounter;
    NProfiling::TRateCounter BatchCommitCounter;
    NProfiling::TAggregateCounter CommitTimeCounter;

};

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a leader.
class TLeaderCommitter
    : public TCommitter
{
public:
    //! Creates an instance.
    TLeaderCommitter(
        TLeaderCommitterConfig* config,
        NElection::TCellManager* cellManager,
        TDecoratedMetaState* metaState,
        TChangeLogCache* changeLogCache,
        TFollowerTracker* followerTracker,
        const TEpoch& epoch,
        IInvokerPtr epochControlInvoker,
        IInvokerPtr epochStateInvoker);

    ~TLeaderCommitter();

    //! Initializes the instance.
    /*!
     *  \note Thread affinity: ControlThread
     */
    void Start();

    //! Releases all resources.
    /*!
     *  \note Thread affinity: ControlThread
     */
    void Stop();

    //! Initiates a new distributed commit.
    /*!
     *  \param changeAction An action that will be called in the context of
     *  the state thread and will update the state.
     *  \param changeData A serialized representation of the change that
     *  will be sent down to follower.
     *  \return An asynchronous flag indicating the outcome of the distributed commit.
     *  
     *  The current implementation regards a distributed commit as completed when the update is
     *  received, applied, and flushed to the changelog by a quorum of replicas.
     *  
     *  \note Thread affinity: StateThread
     */
    TCommitResult Commit(
        TClosure changeAction,
        const TSharedRef& changeData);

    //! Force to send all pending changes.
    /*!
     *  \param rotateChangeLog True iff the changelog will be rotated immediately.
     *  \note Thread affinity: StateThread
     */
    void Flush(bool rotateChangeLog);

    //! Raised in the state thread each time a change is applied locally.
    DEFINE_SIGNAL(void(), ChangeApplied);

private:
    class TBatch;
    typedef TIntrusivePtr<TBatch> TBatchPtr;

    typedef TMetaStateManagerProxy TProxy;

    void OnBatchTimeout(TBatchPtr batch);
    TIntrusivePtr<TBatch> GetOrCreateBatch(const TMetaVersion& version);
    TCommitResult BatchChange(
        const TMetaVersion& version,
        const TSharedRef& changeData,
        TFuture<void> changeLogResult);
    void FlushCurrentBatch(bool rotateChangeLog);

    TLeaderCommitterConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    TChangeLogCachePtr ChangeLogCache;
    TFollowerTrackerPtr FollowerTracker;
    TEpoch Epoch;

    //! Protects the rest.
    TSpinLock BatchSpinLock;
    TBatchPtr CurrentBatch;
    TDelayedInvoker::TCookie BatchTimeoutCookie;
};

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a follower.
class TFollowerCommitter
    : public TCommitter
{
public:
    //! Creates an instance.
    TFollowerCommitter(
        TDecoratedMetaState* metaState,
        IInvokerPtr epochControlInvoker,
        IInvokerPtr epochStateInvoker);

    ~TFollowerCommitter();

    //! Commits a bunch of changes at a follower.
    /*!
     *  \param expectedVersion A version that the state is currently expected to have.
     *  \param changes A bunch of serialized changes to apply.
     *  \return An asynchronous flag indicating the outcome of the local commit.
     *  
     *  The current implementation regards a local commit as completed when the update is
     *  flushed to the local changelog.
     *  
     *  \note Thread affinity: ControlThread
     */
    TCommitResult Commit(
        const TMetaVersion& expectedVersion,
        const std::vector<TSharedRef>& changes);

private:
    TCommitResult DoCommit(
        const TMetaVersion& expectedVersion,
        const std::vector<TSharedRef>& changes);

};

////////////////////////////////////////////////////////////////////////////////


} // namespace NMetaState
} // namespace NYT
