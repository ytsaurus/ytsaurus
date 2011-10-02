#include "change_committer.h"

#include "../misc/serialize.h"
#include "../misc/foreach.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TCommitterBase::TCommitterBase(
    TDecoratedMetaState::TPtr metaState,
    IInvoker::TPtr controlInvoker)
    : MetaState(metaState)
    , CancelableControlInvoker(New<TCancelableInvoker>(controlInvoker))
{
    YASSERT(~metaState != NULL);
    YASSERT(~controlInvoker != NULL);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(metaState->GetInvoker(), StateThread);
}

TCommitterBase::~TCommitterBase()
{ }

void TCommitterBase::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    CancelableControlInvoker->Cancel();
}

TCommitterBase::EResult TCommitterBase::OnAppend(TVoid)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return EResult::Committed;
}

////////////////////////////////////////////////////////////////////////////////

class TLeaderCommitter::TSession
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TLeaderCommitter::TPtr committer,
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

    TLeaderCommitter::TPtr Committer;
    TResult::TPtr Result;
    TParallelAwaiter::TPtr Awaiter;
    TMetaVersion Version;
    i32 CommitCount;
    volatile bool IsSent;
    yvector<TSharedRef> BatchedChanges;

};

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TFollowerTracker::TPtr followerTracker,
    IInvoker::TPtr controlInvoker,
    const TEpoch& epoch)
    : TCommitterBase(metaState, controlInvoker)
    , Config(config)
    , CellManager(cellManager)
    , ChangeLogCache(changeLogCache)
    , FollowerTracker(followerTracker)
    , Epoch(epoch)
{
    YASSERT(~cellManager != NULL);
    YASSERT(~metaState != NULL);
    YASSERT(~changeLogCache != NULL);
    YASSERT(~followerTracker != NULL);
    YASSERT(~controlInvoker != NULL);
}

void TLeaderCommitter::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TCommitterBase::Stop();
    OnApplyChange().Clear();
}

void TLeaderCommitter::Flush()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SessionSpinLock);
    if (~CurrentSession != NULL) {
        FlushCurrentSession();
    }
}

TLeaderCommitter::TResult::TPtr TLeaderCommitter::CommitLeader(
    IAction::TPtr changeAction,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(~changeAction != NULL);

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

void TLeaderCommitter::FlushCurrentSession()
{
    VERIFY_SPINLOCK_AFFINITY(SessionSpinLock);
    YASSERT(~CurrentSession != NULL);

    CurrentSession->SendChanges();
    TDelayedInvoker::Get()->Cancel(TimeoutCookie);
    CurrentSession.Drop();
    TimeoutCookie = TDelayedInvoker::TCookie();
}

TLeaderCommitter::TSession::TPtr TLeaderCommitter::GetOrCreateCurrentSession()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    VERIFY_SPINLOCK_AFFINITY(SessionSpinLock);

    if (~CurrentSession == NULL) {
        YASSERT(~TimeoutCookie == NULL);
        auto version = MetaState->GetVersion();
        CurrentSession = New<TSession>(TPtr(this), version);
        TimeoutCookie = TDelayedInvoker::Get()->Submit(
            FromMethod(
                &TLeaderCommitter::DelayedFlush,
                TPtr(this),
                CurrentSession)
            ->Via(~CancelableControlInvoker),
            Config.MaxBatchDelay);
    }

    return CurrentSession;
}

void TLeaderCommitter::DelayedFlush(TSession::TPtr session)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TGuard<TSpinLock> guard(SessionSpinLock);
    if (session != CurrentSession)
        return;

    LOG_DEBUG("Flushing batched changes");

    FlushCurrentSession();
}

TSignal& TLeaderCommitter::OnApplyChange()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return OnApplyChange_;
}

TLeaderCommitter::TResult::TPtr TLeaderCommitter::DoCommitLeader(
    IAction::TPtr changeAction,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto appendResult = MetaState
        ->LogChange(changeData)
        ->Apply(FromMethod(&TLeaderCommitter::OnAppend));

    MetaState->ApplyChange(changeAction);

    OnApplyChange_.Fire();

    return appendResult;
}

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TDecoratedMetaState::TPtr metaState,
    IInvoker::TPtr controlInvoker)
    : TCommitterBase(metaState, controlInvoker)
{
    YASSERT(~metaState != NULL);
    YASSERT(~controlInvoker != NULL);
}

TCommitterBase::TResult::TPtr TFollowerCommitter::CommitFollower(
    const TMetaVersion& version,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return
        FromMethod(
            &TFollowerCommitter::DoCommitFollower,
            TPtr(this),
            version,
            changeData)
        ->AsyncVia(MetaState->GetInvoker())
        ->Do();
}

TCommitterBase::TResult::TPtr TFollowerCommitter::DoCommitFollower(
    const TMetaVersion& version,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (MetaState->GetVersion() != version) {
        return New<TResult>(EResult::InvalidVersion);
    }

    auto appendResult = MetaState
        ->LogChange(changeData)
        ->Apply(FromMethod(&TLeaderCommitter::OnAppend));

    MetaState->ApplyChange(changeData);

    return appendResult;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
