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

//! Common part for #TFollowerCommitter and #TLeaderCommitter
class TCommitterBase
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TCommitterBase> TPtr;

    TCommitterBase(
        TDecoratedMetaState::TPtr metaState,
        IInvoker::TPtr controlInvoker);

    virtual ~TCommitterBase();

    DECLARE_ENUM(EResult,
        (Committed)
        (MaybeCommitted)
        (InvalidVersion)
    );
    typedef TFuture<EResult> TResult;

    static EResult OnAppend(TVoid);

    /*!
     * \note Thread affinity: any
     */
    void Stop();

protected:
    // Corresponds to ControlThread.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    // Corresponds to MetaState->GetInvoker().
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    TCancelableInvoker::TPtr CancelableControlInvoker;
    TDecoratedMetaState::TPtr MetaState;
};

////////////////////////////////////////////////////////////////////////////////

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

    TLeaderCommitter(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr metaState,
        TChangeLogCache::TPtr changeLogCache,
        TFollowerTracker::TPtr followerTracker,
        IInvoker::TPtr controlInvoker,
        const TEpoch& epoch);

    /*!
     * \note Thread affinity: StateThread
     */
    TResult::TPtr CommitLeader(
        IAction::TPtr changeAction,
        const TSharedRef& changeData);

    //! Force to send all pending changes.
    /*!
     * \note Thread affinity: Any
     */
    void Flush();

    /*!
     * \note Thread affinity: Any
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

    TSpinLock SessionSpinLock;
    TIntrusivePtr<TSession> CurrentSession;
    TDelayedInvoker::TCookie TimeoutCookie;
};

////////////////////////////////////////////////////////////////////////////////

class TFollowerCommitter
    : public TCommitterBase
{
public:
    typedef TIntrusivePtr<TFollowerCommitter> TPtr;

    TFollowerCommitter(
        TDecoratedMetaState::TPtr metaState,
        IInvoker::TPtr controlInvoker);

    /*!
     * \note Thread affinity: ControlThread
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
