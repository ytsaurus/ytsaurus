#include "change_committer.h"

#include "../misc/serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MasterLogger;

////////////////////////////////////////////////////////////////////////////////

class TChangeCommitter::TSession
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TChangeCommitter::TPtr committer,
        IAction::TPtr changeAction,
        TSharedRef changeData)
        : Committer(committer)
        , ChangeAction(changeAction)
        , ChangeData(changeData)
        , Result(new TResult())
        , Awaiter(new TParallelAwaiter(committer->ServiceInvoker))
        // Change is always committed locally.
        , CommitCount(1)
    { }

    TResult::TPtr Run() // WorkQueue thread
    {
        StateId = Committer->MasterState->GetStateId();

        TCellManager::TPtr cellManager = Committer->CellManager;

        for (TMasterId id = 0; id < cellManager->GetMasterCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;
            
            THolder<TProxy> proxy(cellManager->GetMasterProxy<TProxy>(id));
            
            TProxy::TReqApplyChange::TPtr request = proxy->ApplyChange();
            request->SetSegmentId(StateId.SegmentId);
            request->SetChangeCount(StateId.ChangeCount);
            request->SetEpoch(Committer->Epoch.ToProto());
            request->Attachments().push_back(ChangeData);
            
            Awaiter->Await(
                request->Invoke(Committer->Timeout),
                FromMethod(&TSession::OnCommitted, TPtr(this), id));
        }

        Committer->DoCommitLeader(ChangeAction, ChangeData);
        LOG_DEBUG("Change %s is committed locally", ~StateId.ToString());

        Awaiter->Complete(FromMethod(&TSession::OnCompleted, TPtr(this)));

        return Result;
    }

private:
    // Service thread
    bool CheckQuorum()
    {
        if (CommitCount < Committer->CellManager->GetQuorum())
            return false;

        Result->Set(EResult::Committed);
        Awaiter->Cancel();
        
        LOG_DEBUG("Change %s is committed by quorum",
            ~StateId.ToString());

        return true;
    }

    void OnCommitted(TProxy::TRspApplyChange::TPtr response, TMasterId masterId)
    {
        if (!response->IsOK()) {
            LOG_WARNING("Error %s committing change %s at master %d",
                ~response->GetErrorCode().ToString(),
                ~StateId.ToString(),
                masterId);
            return;
        }

        if (response->GetCommitted()) {
            LOG_DEBUG("Change %s is committed by master %d",
                ~StateId.ToString(),
                masterId);

            ++CommitCount;
            CheckQuorum();
        } else {
            LOG_DEBUG("Change %s is acknowledged but not committed by master %d",
                ~StateId.ToString(),
                masterId);
        }
    }

    void OnCompleted()
    {
        if (CheckQuorum())
            return;

        LOG_WARNING("Change %s is uncertain, committed by %d master(s)",
            ~StateId.ToString(), CommitCount);
        Result->Set(EResult::MaybeCommitted);
    }

private:
    TChangeCommitter::TPtr Committer;
    IAction::TPtr ChangeAction;
    TSharedRef ChangeData;
    TResult::TPtr Result;
    TParallelAwaiter::TPtr Awaiter;
    i32 CommitCount; // Service thread
    TMasterStateId StateId;
};

////////////////////////////////////////////////////////////////////////////////

TChangeCommitter::TChangeCommitter(
    TCellManager::TPtr cellManager,
    TDecoratedMasterState::TPtr masterState,
    TChangeLogCache::TPtr changeLogCache,
    IInvoker::TPtr serviceInvoker,
    const TMasterEpoch& epoch)
    : CellManager(cellManager)
    , MasterState(masterState)
    , ChangeLogCache(changeLogCache)
    , ServiceInvoker(serviceInvoker)
    , StateInvoker(masterState->GetInvoker())
    , Epoch(epoch)
    , Timeout(TDuration::Seconds(10))
{ }

TDuration TChangeCommitter::GetTimeout() const
{
    return Timeout;
}

void TChangeCommitter::SetTimeout(TDuration timeout)
{
    Timeout = timeout;
}

void TChangeCommitter::SetOnApplyChange(IAction::TPtr onApplyChange)
{
    OnApplyChange = onApplyChange;
}

TChangeCommitter::TResult::TPtr TChangeCommitter::CommitLeader(
    IAction::TPtr changeAction,
    TSharedRef changeData)
{
    TSession::TPtr session = new TSession(
        this,
        changeAction,
        changeData);
    return session->Run();
}

TChangeCommitter::TResult::TPtr TChangeCommitter::CommitFollower(
    TMasterStateId stateId,
    TSharedRef changeData)
{
    return
        FromMethod(
            &TChangeCommitter::DoCommitFollower,
            TPtr(this),
            stateId,
            changeData)
        ->AsyncVia(StateInvoker)
        ->Do();
}

TChangeCommitter::TResult::TPtr TChangeCommitter::DoCommitLeader(
    IAction::TPtr changeAction,
    TSharedRef changeData)
{
    TChangeCommitter::TResult::TPtr appendResult = MasterState
        ->LogChange(changeData)
        ->Apply(FromMethod(&TChangeCommitter::OnAppend));

    MasterState->ApplyChange(changeAction);

    // OnApplyChange can be modified concurrently.
    IAction::TPtr onApplyChange = OnApplyChange;
    if (~onApplyChange != NULL) {
        onApplyChange->Do();
    }

    return appendResult;
}

TChangeCommitter::TResult::TPtr TChangeCommitter::DoCommitFollower(
    TMasterStateId stateId,
    TSharedRef changeData)
{
    if (MasterState->GetStateId() != stateId) {
        return new TResult(EResult::InvalidStateId);
    }

    MasterState->ApplyChange(changeData);

    return MasterState
           ->LogChange(changeData)
           ->Apply(FromMethod(&TChangeCommitter::OnAppend));
}

TChangeCommitter::EResult TChangeCommitter::OnAppend(TVoid)
{
    return EResult::Committed;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
