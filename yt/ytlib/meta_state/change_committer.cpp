#include "change_committer.h"

#include "../misc/serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

class TChangeCommitter::TSession
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TChangeCommitter::TPtr committer,
        const TMetaVersion& version)
        : Committer(committer)
        , Result(New<TResult>())
        , Awaiter(New<TParallelAwaiter>(~committer->CancelableServiceInvoker))
        , Version(version)
        // Count the local commit.
        , CommitCount(1)
    { }

    TResult::TPtr AddChange(TSharedRef changeData)
    {
        BatchedChanges.push_back(changeData);
        LOG_DEBUG("Added %d change from version %s to batch",
            GetChangeCount(),
            ~Version.ToString());
        return Result;
    }

    // Service invoker
    void SendChanges()
    {
        LOG_DEBUG("Starting commit of %d changes of version %s",
            GetChangeCount(),
            ~Version.ToString());

        TCellManager::TPtr cellManager = Committer->CellManager;

        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;

            THolder<TProxy> proxy(cellManager->GetMasterProxy<TProxy>(id));

            TProxy::TReqApplyChanges::TPtr request = proxy->ApplyChanges();
            request->SetSegmentId(Version.SegmentId);
            request->SetRecordCount(Version.RecordCount);
            request->SetEpoch(Committer->Epoch.ToProto());
            for (int i = 0; i < BatchedChanges.ysize(); ++i) {
                request->Attachments().push_back(BatchedChanges[i]);
            }
            Awaiter->Await(
                request->Invoke(Committer->Config.RpcTimeout),
                FromMethod(&TSession::OnCommitted, TPtr(this), id));

            LOG_DEBUG("Change of %s is sent to peer %d",
                ~Version.ToString(),
                id);
        }

        Awaiter->Complete(FromMethod(&TSession::OnCompleted, TPtr(this)));
    }

    int GetChangeCount() const
    {
        return BatchedChanges.ysize();
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
    void OnCommitted(TProxy::TRspApplyChanges::TPtr response, TPeerId peerId)
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

    yvector<TSharedRef> BatchedChanges;

    TChangeCommitter::TPtr Committer;
    TResult::TPtr Result;
    TParallelAwaiter::TPtr Awaiter;
    i32 CommitCount; // Service thread
    TMetaVersion Version;
    IInvoker::TPtr ServiceInvoker;
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

void TChangeCommitter::Flush()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (~CurrentSession != NULL) {
        FlushCurrentSession();
    }
}

TChangeCommitter::TResult::TPtr TChangeCommitter::CommitLeader(
    IAction::TPtr changeAction,
    TSharedRef changeData)
{
    TMetaVersion version = MetaState->GetVersion();
    TResult::TPtr result;
    {
        TGuard<TSpinLock> guard(SpinLock);
        LOG_DEBUG("Starting commit of change %s", ~version.ToString());
        if (~CurrentSession == NULL) {
            YASSERT(~TimeoutCookie == NULL);
            CurrentSession = New<TSession>(TPtr(this), version);
            TimeoutCookie = TDelayedInvoker::Get()->Submit(
                FromMethod(
                    &TChangeCommitter::DelayedFlush,
                    TPtr(this),
                    CurrentSession),
                Config.MaxBatchDelay);
        }

        result = CurrentSession->AddChange(changeData);

        if (CurrentSession->GetChangeCount() >= Config.MaxBatchSize) {
            FlushCurrentSession();
        }
    }

    DoCommitLeader(changeAction, changeData);
    LOG_DEBUG("Change %s is committed locally", ~version.ToString());

    return result;
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

void TChangeCommitter::FlushCurrentSession()
{
    YASSERT(~CurrentSession != NULL);
    CurrentSession->SendChanges();
    TDelayedInvoker::Get()->Cancel(TimeoutCookie);
    CurrentSession.Drop();
    TimeoutCookie = TDelayedInvoker::TCookie();
}

void TChangeCommitter::DelayedFlush(TSession::TPtr session)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (session != CurrentSession) {
        return;
    }
    TMetaVersion version = MetaState->GetVersion();
    LOG_DEBUG("Batch timeout occured at version %s", ~version.ToString())
    FlushCurrentSession();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
