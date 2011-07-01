#pragma once

#include "common.h"
#include "master_state_manager_rpc.h"
#include "cell_manager.h"
#include "decorated_master_state.h"
#include "change_log_cache.h"
#include "election_manager.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChangeCommitter
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChangeCommitter> TPtr;

    enum EResult
    {
        Committed,
        MaybeCommitted,
        InvalidStateId
    };

    typedef TAsyncResult<EResult> TResult;

    TChangeCommitter(
        TCellManager::TPtr cellManager,
        TDecoratedMasterState::TPtr masterState,
        TChangeLogCache::TPtr changeLogCache,
        IInvoker::TPtr serviceInvoker,
        IInvoker::TPtr workInvoker,
        const TMasterEpoch& epoch);

    TResult::TPtr CommitDistributed(TSharedRef change);

    TResult::TPtr CommitLocal(
        TMasterStateId stateId,
        TSharedRef change);

    TDuration GetTimeout() const;
    void SetTimeout(TDuration timeout);

    void SetOnApplyChange(IAction::TPtr onApplyChange);

private:
    class TSession;
    typedef TMasterStateManagerProxy TProxy;

    TResult::TPtr DoCommitLocal(
        TMasterStateId stateId,
        const TSharedRef& changeData);
    static EResult OnAppend(TChangeLogWriter::EResult result);

    TCellManager::TPtr CellManager;
    TDecoratedMasterState::TPtr MasterState;
    TChangeLogCache::TPtr ChangeLogCache;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr WorkInvoker;
    TMasterEpoch Epoch;
    IAction::TPtr OnApplyChange;
    // TODO: refactor this
    TDuration Timeout;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
