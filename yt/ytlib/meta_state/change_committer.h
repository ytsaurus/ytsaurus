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

////////////////////////////////////////////////////////////////////////////////

// TODO: split into TLeaderCommitter and TFollowerCommitter
class TChangeCommitter
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChangeCommitter> TPtr;

    DECLARE_ENUM(EResult,
        (Committed)
        (MaybeCommitted)
        (InvalidVersion)
    );

    typedef TAsyncResult<EResult> TResult;

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

    TChangeCommitter(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr metaState,
        TChangeLogCache::TPtr changeLogCache,
        TFollowerTracker::TPtr followerTracker,
        IInvoker::TPtr controlInvoker,
        const TEpoch& epoch);

    /*!
     * \note Thread affinity: any
     */
    void Stop();

    /*!
     * \note Thread affinity: StateThread
     */
    TResult::TPtr CommitLeader(
        IAction::TPtr changeAction,
        const TSharedRef& changeData);

    /*!
     * \note Thread affinity: ControlThread
     */
    TResult::TPtr CommitFollower(
        const TMetaVersion& version,
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
    TResult::TPtr DoCommitFollower(
        const TMetaVersion& version,
        const TSharedRef& changeData);
    static EResult OnAppend(TVoid);

    void DelayedFlush(TIntrusivePtr<TSession> session);
    TIntrusivePtr<TSession> GetOrCreateCurrentSession();
    void FlushCurrentSession();

    // Corresponds to ControlThread.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    // Corresponds to MetaState->GetInvoker().
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    TConfig Config;
    TCellManager::TPtr CellManager;
    TDecoratedMetaState::TPtr MetaState;
    TChangeLogCache::TPtr ChangeLogCache;
    TFollowerTracker::TPtr FollowerTracker;
    TCancelableInvoker::TPtr CancelableControlInvoker;
    TEpoch Epoch;

    TSignal OnApplyChange_;

    TSpinLock SessionSpinLock;
    TIntrusivePtr<TSession> CurrentSession;
    TDelayedInvoker::TCookie TimeoutCookie;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
