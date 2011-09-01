#pragma once

#include "common.h"
#include "meta_state_manager_rpc.h"
#include "decorated_meta_state.h"
#include "change_log_cache.h"

#include "../election/election_manager.h"

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
        IInvoker::TPtr serviceInvoker,
        const TEpoch& epoch);

    void Stop();

    TResult::TPtr CommitLeader(
        IAction::TPtr changeAction,
        TSharedRef changeData);

    TResult::TPtr CommitFollower(
        TMetaVersion version,
        TSharedRef changeData);

    // TODO: use TSignal here
    void SetOnApplyChange(IAction::TPtr onApplyChange);

    //! Forcely send an rpc request with changes
    void Flush();

private:
    class TSession;
    typedef TMetaStateManagerProxy TProxy;

    TResult::TPtr DoCommitLeader(
        IAction::TPtr changeAction,
        TSharedRef changeData);
    TResult::TPtr DoCommitFollower(
        TMetaVersion version,
        TSharedRef changeData);
    static EResult OnAppend(TVoid);

    void DelayedFlush(TIntrusivePtr<TSession> session);
    void FlushCurrentSession();

    TConfig Config;
    TCellManager::TPtr CellManager;
    TDecoratedMetaState::TPtr MetaState;
    TChangeLogCache::TPtr ChangeLogCache;
    TCancelableInvoker::TPtr CancelableServiceInvoker;
    TEpoch Epoch;
    IAction::TPtr OnApplyChange;

    TIntrusivePtr<TSession> CurrentSession;
    TSpinLock SpinLock; // for work with session
    TDelayedInvoker::TCookie TimeoutCookie; // for session
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
