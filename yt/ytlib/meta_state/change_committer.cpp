#include "change_committer.h"

#include "../misc/serialize.h"
#include "../misc/foreach.h"

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
        , Awaiter(New<TParallelAwaiter>(~committer->CancelableControlInvoker))
        , Version(version)
        // Count the local commit.
        , CommitCount(1)
        , IsSent(false)
    { }

    TResult::TPtr AddChange(const TSharedRef& changeData)
    {
        VERIFY_THREAD_AFFINITY(Committer->StateThread);
        YASSERT(!IsSent);

        BatchedChanges.push_back(changeData);
        LOG_DEBUG("Added %d change from version %s to batch",
            GetChangeCount(),
            ~Version.ToString());
        return Result;
    }

    // Service invoker
    void SendChanges()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        IsSent = true;

        LOG_DEBUG("Starting commit of %d changes of version %s",
            BatchedChanges.ysize(),
            ~Version.ToString());

        auto cellManager = Committer->CellManager;

        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == cellManager->GetSelfId() ||
                !Committer->FollowerTracker->IsFollowerActive(id))
                continue;

            auto proxy = cellManager->GetMasterProxy<TProxy>(id);
            auto request = proxy->ApplyChanges();
            request->SetSegmentId(Version.SegmentId);
            request->SetRecordCount(Version.RecordCount);
            request->SetEpoch(Committer->Epoch.ToProto());
            FOREACH(const auto& change, BatchedChanges) {
                request->Attachments().push_back(change);
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
        VERIFY_THREAD_AFFINITY(Committer->StateThread);
        YASSERT(!IsSent);

        return BatchedChanges.ysize();
    }


private:
    // Service invoker
    bool CheckCommitQuorum()
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

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
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

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
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        if (CheckCommitQuorum())
            return;

        LOG_WARNING("Change %s is uncertain as it was committed by %d masters out of %d",
            ~Version.ToString(),
            CommitCount,
            Committer->CellManager->GetPeerCount());

        Result->Set(EResult::MaybeCommitted);
    }

    TChangeCommitter::TPtr Committer;
    TResult::TPtr Result;
    TParallelAwaiter::TPtr Awaiter;
    TMetaVersion Version;
    i32 CommitCount;
    volatile bool IsSent;
    yvector<TSharedRef> BatchedChanges;

};

////////////////////////////////////////////////////////////////////////////////

TChangeCommitter::TChangeCommitter(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TFollowerTracker::TPtr followerTracker,
    IInvoker::TPtr controlInvoker,
    const TEpoch& epoch)
    : Config(config)
    , CellManager(cellManager)
    , MetaState(metaState)
    , ChangeLogCache(changeLogCache)
    , FollowerTracker(followerTracker)
    , CancelableControlInvoker(New<TCancelableInvoker>(controlInvoker))
    , Epoch(epoch)
{
    YASSERT(~cellManager != NULL);
    YASSERT(~metaState != NULL);
    YASSERT(~changeLogCache != NULL);
    // TODO: this check only makes sense for leader
    //YASSERT(~followerTracker != NULL);
    YASSERT(~controlInvoker != NULL);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(metaState->GetInvoker(), StateThread);
}

void TChangeCommitter::Stop()
{
    VERIFY_THREAD_AFFINITY_ANY();

    CancelableControlInvoker->Cancel();
}

TSignal& TChangeCommitter::OnApplyChange()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return OnApplyChange_;
}

void TChangeCommitter::Flush()
{
    TGuard<TSpinLock> guard(SessionSpinLock);
    if (~CurrentSession != NULL) {
        FlushCurrentSession();
    }
}

TChangeCommitter::TResult::TPtr TChangeCommitter::CommitLeader(
    IAction::TPtr changeAction,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto version = MetaState->GetVersion();
    LOG_DEBUG("Starting commit of change %s", ~version.ToString());

    TResult::TPtr result;
    {
        TGuard<TSpinLock> guard(SessionSpinLock);
        auto session = GetOrCreateCurrentSession();
        result = session->AddChange(changeData);
        if (session->GetChangeCount() >= Config.MaxBatchSize) {
            FlushCurrentSession();
        }
    }

    DoCommitLeader(changeAction, changeData);
    LOG_DEBUG("Change %s is committed locally", ~version.ToString());

    return result;
}

TChangeCommitter::TResult::TPtr TChangeCommitter::CommitFollower(
    const TMetaVersion& version,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

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
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto appendResult = MetaState
        ->LogChange(changeData)
        ->Apply(FromMethod(&TChangeCommitter::OnAppend));

    MetaState->ApplyChange(changeAction);

    OnApplyChange_.Fire();

    return appendResult;
}

TChangeCommitter::TResult::TPtr TChangeCommitter::DoCommitFollower(
    const TMetaVersion& version,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (MetaState->GetVersion() != version) {
        return New<TResult>(EResult::InvalidVersion);
    }

    auto appendResult = MetaState
        ->LogChange(changeData)
        ->Apply(FromMethod(&TChangeCommitter::OnAppend));

    MetaState->ApplyChange(changeData);

    return appendResult;
}

TChangeCommitter::EResult TChangeCommitter::OnAppend(TVoid)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return EResult::Committed;
}

void TChangeCommitter::FlushCurrentSession()
{
    VERIFY_SPINLOCK_AFFINITY(SessionSpinLock);
    YASSERT(~CurrentSession != NULL);

    CurrentSession->SendChanges();
    TDelayedInvoker::Get()->Cancel(TimeoutCookie);
    CurrentSession.Drop();
    TimeoutCookie = TDelayedInvoker::TCookie();
}

TChangeCommitter::TSession::TPtr TChangeCommitter::GetOrCreateCurrentSession()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    VERIFY_SPINLOCK_AFFINITY(SessionSpinLock);

    if (~CurrentSession == NULL) {
        YASSERT(~TimeoutCookie == NULL);
        auto version = MetaState->GetVersion();
        CurrentSession = New<TSession>(TPtr(this), version);
        TimeoutCookie = TDelayedInvoker::Get()->Submit(
            FromMethod(
                &TChangeCommitter::DelayedFlush,
                TPtr(this),
                CurrentSession)
            ->Via(~CancelableControlInvoker),
            Config.MaxBatchDelay);
    }

    return CurrentSession;
}

void TChangeCommitter::DelayedFlush(TSession::TPtr session)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TGuard<TSpinLock> guard(SessionSpinLock);
    if (session != CurrentSession)
        return;

    LOG_DEBUG("Flushing batched changes");

    FlushCurrentSession();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
