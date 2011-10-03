#pragma once

#include "common.h"
#include "meta_state_manager_rpc.h"
#include "decorated_meta_state.h"
#include "change_log_cache.h"
#include "follower_tracker.h"

#include "../election/election_manager.h"
#include "../misc/thread_affinity.h"
#include "../actions/signal.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! A common base for TFollowerCommitter and TLeaderCommitter.
class TCommitterBase
    : public TRefCountedBase
{
public:
    TCommitterBase(
        TDecoratedMetaState::TPtr metaState,
        IInvoker::TPtr controlInvoker);

    DECLARE_ENUM(EResult,
        (Committed)
        (MaybeCommitted)
        (InvalidVersion)
    );
    typedef TFuture<EResult> TResult;

    //! Releases all resources.
    /*!
     *  \note Thread affinity: ControlThread
     */
    void Stop();

protected:
    static EResult OnAppend(TVoid);

    // Corresponds to ControlThread.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    // Corresponds to MetaState->GetInvoker().
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    TDecoratedMetaState::TPtr MetaState;
    TCancelableInvoker::TPtr CancelableControlInvoker;
};

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a leader.
class TLeaderCommitter
    : public TCommitterBase
{
public:
    typedef TIntrusivePtr<TLeaderCommitter> TPtr;

    struct TConfig
    {
        TConfig()
            : RpcTimeout(TDuration::Seconds(3))
            , MaxBatchDelay(TDuration::MilliSeconds(10))
            , MaxBatchSize(100)
        { }

        TDuration RpcTimeout;
        TDuration MaxBatchDelay;
        int MaxBatchSize;
    };

    //! Creates an instance.
    TLeaderCommitter(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr metaState,
        TChangeLogCache::TPtr changeLogCache,
        TFollowerTracker::TPtr followerTracker,
        IInvoker::TPtr controlInvoker,
        const TEpoch& epoch);

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
    TResult::TPtr CommitLeader(
        IAction::TPtr changeAction,
        const TSharedRef& changeData);

    //! Force to send all pending changes.
    /*!
     * \note Thread affinity: any
     */
    void Flush();

    //! A signal that gets raised in the state thread after each commit.
    /*!
     * \note Thread affinity: any
     */
    TSignal& OnApplyChange();

private:
    class TSession;
    typedef TMetaStateManagerProxy TProxy;

    TResult::TPtr DoCommitLeader(
        IAction::TPtr changeAction,
        const TSharedRef& changeData);

    void DelayedFlush(TIntrusivePtr<TSession> session);
    TIntrusivePtr<TSession> GetOrCreateCurrentSession();
    void FlushCurrentSession();

    TConfig Config;
    TCellManager::TPtr CellManager;
    TChangeLogCache::TPtr ChangeLogCache;
    TFollowerTracker::TPtr FollowerTracker;
    TEpoch Epoch;

    TSignal OnApplyChange_;

    //! Protects #CurrentSession and TimeoutCookie.
    TSpinLock SessionSpinLock;
    TIntrusivePtr<TSession> CurrentSession;
    TDelayedInvoker::TCookie TimeoutCookie;
};

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a follower.
class TFollowerCommitter
    : public TCommitterBase
{
public:
    typedef TIntrusivePtr<TFollowerCommitter> TPtr;

    //! Creates an instance.
    TFollowerCommitter(
        TDecoratedMetaState::TPtr metaState,
        IInvoker::TPtr controlInvoker);

    //! Carries out a commit at a follower.
    /*!
     *  \param version A version that the state is currently expected to have.
     *  \param changeData A serialized representation of the change that
     *  the follower must apply.
     *  \return An asynchronous flag indicating the outcome of the local commit.
     *  
     *  The current implementation regards a local commit as completed when the update is
     *  flushed to the local changelog.
     *  
     *  \note Thread affinity: ControlThread
     */
    TResult::TPtr CommitFollower(
        const TMetaVersion& version,
        const TSharedRef& changeData);

private:
    TResult::TPtr DoCommitFollower(
        const TMetaVersion& version,
        const TSharedRef& changeData);

};

////////////////////////////////////////////////////////////////////////////////


} // namespace NMetaState
} // namespace NYT
