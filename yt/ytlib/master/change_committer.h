#pragma once

#include "common.h"
#include "master_state_manager_rpc.h"
#include "decorated_master_state.h"
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
        (InvalidStateId)
    );

    typedef TAsyncResult<EResult> TResult;

    TChangeCommitter(
        TCellManager::TPtr cellManager,
        TDecoratedMasterState::TPtr masterState,
        TChangeLogCache::TPtr changeLogCache,
        IInvoker::TPtr serviceInvoker,
        const TMasterEpoch& epoch);

    TResult::TPtr CommitLeader(
        IAction::TPtr changeAction,
        TSharedRef changeData);

    TResult::TPtr CommitFollower(
        TMasterStateId stateId,
        TSharedRef changeData);

    // TODO: refactor this
    TDuration GetTimeout() const;
    void SetTimeout(TDuration timeout);

    void SetOnApplyChange(IAction::TPtr onApplyChange);

private:
    class TSession;
    typedef TMasterStateManagerProxy TProxy;

    TResult::TPtr DoCommitLeader(
        IAction::TPtr changeAction,
        TSharedRef changeData);
    TResult::TPtr DoCommitFollower(
        TMasterStateId stateId,
        TSharedRef changeData);
    static EResult OnAppend(TVoid);

    TCellManager::TPtr CellManager;
    TDecoratedMasterState::TPtr MasterState;
    TChangeLogCache::TPtr ChangeLogCache;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr StateInvoker;
    TMasterEpoch Epoch;
    IAction::TPtr OnApplyChange;
    // TODO: refactor this
    TDuration Timeout;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
