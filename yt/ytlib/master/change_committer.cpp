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
        , Result(New<TResult>())
        , Awaiter(New<TParallelAwaiter>(~committer->CancelableServiceInvoker))
        // Count the local commit.
        , CommitCount(1)
    { }

    // State invoker
    TResult::TPtr Run()
    {
        Version = Committer->MetaState->GetVersion();

        LOG_DEBUG("Starting commit of change %s", ~Version.ToString());

        TCellManager::TPtr cellManager = Committer->CellManager;

        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;
            
            THolder<TProxy> proxy(cellManager->GetMasterProxy<TProxy>(id));
            
            TProxy::TReqApplyChange::TPtr request = proxy->ApplyChange();
            request->SetSegmentId(Version.SegmentId);
            request->SetRecordCount(Version.RecordCount);
            request->SetEpoch(Committer->Epoch.ToProto());
            request->Attachments().push_back(ChangeData);
            
            Awaiter->Await(
                request->Invoke(Committer->Config.RpcTimeout),
                FromMethod(&TSession::OnCommitted, TPtr(this), id));

            LOG_DEBUG("Change %s is sent to peer %d",
                ~Version.ToString(),
                id);
        }

        Committer->DoCommitLeader(ChangeAction, ChangeData);
        LOG_DEBUG("Change %s is committed locally", ~Version.ToString());

        Awaiter->Complete(FromMethod(&TSession::OnCompleted, TPtr(this)));

        return Result;
    }

private:
    // Service invoker
    bool CheckCommitQuorum()
    {
        if (CommitCount < Committer->CellManager->GetQuorum())
            return false;

        Result->Set(EResult::Committed);
        Awaiter->Cancel();
        
        LOG_DEBUG("Change %s is committed by quorum",
            ~Version.ToString());

        return true;
    }

    // Service invoker
    void OnCommitted(TProxy::TRspApplyChange::TPtr response, TPeerId peerId)
    {
        if (!response->IsOK()) {
            LOG_WARNING("Error committing change %s at peer %d (ErrorCode: %s)",
                ~Version.ToString(),
                peerId,
                ~response->GetErrorCode().ToString());
            return;
        }

        if (response->GetCommitted()) {
            LOG_DEBUG("Change %s is committed by peer %d",
                ~Version.ToString(),
                peerId);

            ++CommitCount;
            CheckCommitQuorum();
        } else {
            LOG_DEBUG("Change %s is acknowledged but not committed by peer %d",
                ~Version.ToString(),
                peerId);
        }
    }

    // Service invoker
    void OnCompleted()
    {
        if (CheckCommitQuorum())
            return;

        LOG_WARNING("Change %s is uncertain as it was committed by %d masters out of %d",
            ~Version.ToString(),
            CommitCount,
            Committer->CellManager->GetPeerCount());
        Result->Set(EResult::MaybeCommitted);
    }

private:
    TChangeCommitter::TPtr Committer;
    IAction::TPtr ChangeAction;
    TSharedRef ChangeData;
    TResult::TPtr Result;
    TParallelAwaiter::TPtr Awaiter;
    i32 CommitCount; // Service thread
    TMetaVersion Version;
};

////////////////////////////////////////////////////////////////////////////////

TChangeCommitter::TChangeCommitter(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    IInvoker::TPtr serviceInvoker,
    const TEpoch& epoch)
    : Config(config)
    , CellManager(cellManager)
    , MetaState(metaState)
    , ChangeLogCache(changeLogCache)
    , CancelableServiceInvoker(New<TCancelableInvoker>(serviceInvoker))
    , Epoch(epoch)
{ }

void TChangeCommitter::Stop()
{
    CancelableServiceInvoker->Cancel();
}

void TChangeCommitter::SetOnApplyChange(IAction::TPtr onApplyChange)
{
    OnApplyChange = onApplyChange;
}

TChangeCommitter::TResult::TPtr TChangeCommitter::CommitLeader(
    IAction::TPtr changeAction,
    TSharedRef changeData)
{
    return New<TSession>(
        this,
        changeAction,
        changeData)
    ->Run();
}

TChangeCommitter::TResult::TPtr TChangeCommitter::CommitFollower(
    TMetaVersion version,
    TSharedRef changeData)
{
    return
        FromMethod(
            &TChangeCommitter::DoCommitFollower,
            TPtr(this),
            version,
            changeData)
        ->AsyncVia(MetaState->GetInvoker())
        ->Do();
}

TChangeCommitter::TResult::TPtr TChangeCommitter::DoCommitLeader(
    IAction::TPtr changeAction,
    TSharedRef changeData)
{
    TChangeCommitter::TResult::TPtr appendResult = MetaState
        ->LogChange(changeData)
        ->Apply(FromMethod(&TChangeCommitter::OnAppend));

    MetaState->ApplyChange(changeAction);

    // OnApplyChange can be modified concurrently.
    IAction::TPtr onApplyChange = OnApplyChange;
    if (~onApplyChange != NULL) {
        onApplyChange->Do();
    }

    return appendResult;
}

TChangeCommitter::TResult::TPtr TChangeCommitter::DoCommitFollower(
    TMetaVersion version,
    TSharedRef changeData)
{
    if (MetaState->GetVersion() != version) {
        return New<TResult>(EResult::InvalidVersion);
    }

    TChangeCommitter::TResult::TPtr appendResult = MetaState
        ->LogChange(changeData)
        ->Apply(FromMethod(&TChangeCommitter::OnAppend));

    MetaState->ApplyChange(changeData);

    return appendResult;
}

TChangeCommitter::EResult TChangeCommitter::OnAppend(TVoid)
{
    return EResult::Committed;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
