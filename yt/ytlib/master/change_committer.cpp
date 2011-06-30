#include "change_committer.h"

#include "../misc/serialize.h"
#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChangeCommiter");

////////////////////////////////////////////////////////////////////////////////

class TChangeCommitter::TSession
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TChangeCommitter::TPtr committer,
        TSharedRef data,
        TResult::TPtr result)
        : Committer(committer)
        , Data(data)
        , Result(result)
        , Awaiter(new TParallelAwaiter(committer->ServiceInvoker))
        , CommitCount(0)
    { }

    void Run() // WorkQueue thread
    {
        StateId = Committer->MasterState->GetStateId();

        TCellManager* cellManager = ~Committer->CellManager;

        for (TMasterId id = 0; id < cellManager->GetMasterCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;
            
            THolder<TProxy> proxy(cellManager->GetMasterProxy<TProxy>(id));
            
            TProxy::TReqApplyChange::TPtr request = proxy->ApplyChange();
            request->SetSegmentId(StateId.SegmentId);
            request->SetChangeCount(StateId.ChangeCount);
            request->SetEpoch(ProtoGuidFromGuid(Committer->Epoch));
            request->Attachments().push_back(Data);
            
            Awaiter->Await(
                request->Invoke(Committer->Timeout),
                FromMethod(&TSession::OnRemoteCommit, TPtr(this), id));
        }

        Awaiter->Await(
            Committer->DoCommitLocal(StateId, Data),
            FromMethod(&TSession::OnLocalCommit, TPtr(this)));

        Awaiter->Complete(
            FromMethod(&TSession::OnFail, TPtr(this)));
    }

private:
    void OnRemoteCommit(TProxy::TRspApplyChange::TPtr response, TMasterId masterId) // Service thread
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

            OnCommit();
        } else {
            LOG_DEBUG("Change %s is acknowledged by master %d",
                ~StateId.ToString(),
                masterId);
        }
    }

    void OnLocalCommit(EResult result) // Service thread
    {
        YASSERT(result == Committed);
        LOG_DEBUG("Change %s is committed locally",
            ~StateId.ToString());
        OnCommit();
    }

    void OnCommit() // Service thread
    {
        ++CommitCount;
        if (CommitCount >= Committer->CellManager->GetQuorum()) {
            LOG_DEBUG("Change %s is committed by quorum",
                ~StateId.ToString());
            Result->Set(Committed);
            Awaiter->Cancel();
        }
    }

    void OnFail()
    {
        LOG_WARNING("Change %s is uncertain, committed by %d master(s)",
            ~StateId.ToString(), CommitCount);
        Result->Set(MaybeCommitted);
    }

private:
    TChangeCommitter::TPtr Committer;
    TSharedRef Data;
    TResult::TPtr Result;
    TParallelAwaiter::TPtr Awaiter;
    i32 CommitCount; // Service thread
    TMasterStateId StateId;
};

////////////////////////////////////////////////////////////////////////////////

TChangeCommitter::TChangeCommitter(
    TCellManager::TPtr cellManager,
    TDecoratedMasterState* masterState,
    TChangeLogCache::TPtr changeLogCache,
    IInvoker::TPtr serviceInvoker,
    IInvoker::TPtr workInvoker,
    const TMasterEpoch& epoch)
    : CellManager(cellManager)
    , MasterState(masterState)
    , ChangeLogCache(changeLogCache)
    , ServiceInvoker(serviceInvoker)
    , WorkInvoker(workInvoker)
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

TChangeCommitter::EResult TChangeCommitter::OnAppend(
    TChangeLogWriter::EResult result)
{
    YASSERT(result == TChangeLogWriter::OK);
    return Committed;
}

TChangeCommitter::TResult::TPtr TChangeCommitter::CommitDistributed(
    TSharedRef change)
{
    TResult::TPtr result = new TResult();
    TSession::TPtr session = new TSession(this, change, result);
    WorkInvoker->Invoke(FromMethod(&TSession::Run, session));
    return result;
}

TChangeCommitter::TResult::TPtr TChangeCommitter::CommitLocal(
    TMasterStateId stateId,
    TSharedRef change)
{
    return
        FromMethod(
            &TChangeCommitter::DoCommitLocal,
            TPtr(this),
            stateId,
            change)
        ->AsyncVia(WorkInvoker)
        ->Do();
}

TChangeCommitter::TResult::TPtr TChangeCommitter::DoCommitLocal(
    TMasterStateId stateId,
    TSharedRef change)
{
    if (MasterState->GetStateId() != stateId) {
        return new TResult(InvalidStateId);
    }

    TCachedChangeLog::TPtr changeLog = ChangeLogCache->Get(stateId.SegmentId);
    if (~changeLog == NULL) {
        LOG_FATAL("The current changelog %d is missing",
                   stateId.SegmentId);
    }

    TChangeLogWriter& writer = changeLog->GetWriter();
    TChangeLogWriter::TAppendResult::TPtr appendResult = writer.Append(
        stateId.ChangeCount,
        change);

    MasterState->ApplyChange(change);

    // Can be modified concurrently.
    IAction::TPtr onApplyChange = OnApplyChange;
    if (~onApplyChange != NULL) {
        onApplyChange->Do();
    }

    return appendResult->Apply(FromMethod(&TChangeCommitter::OnAppend));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
