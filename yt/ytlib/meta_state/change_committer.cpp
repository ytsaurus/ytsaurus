#include "stdafx.h"
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
    VERIFY_INVOKER_AFFINITY(metaState->GetStateInvoker(), StateThread);
}

void TCommitterBase::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    CancelableControlInvoker->Cancel();
}

////////////////////////////////////////////////////////////////////////////////

class TLeaderCommitter::TBatch
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TBatch> TPtr;

    TBatch(
        TLeaderCommitter::TPtr committer,
        const TMetaVersion& version)
        : Committer(committer)
        , Result(New<TResult>())
        , Awaiter(New<TParallelAwaiter>(~committer->CancelableControlInvoker))
        , StartVersion(version)
        // Count the local commit.
        , CommitCount(0)
        , IsSent(false)
    { }

    TResult::TPtr AddChange(const TSharedRef& changeData)
    {
        VERIFY_THREAD_AFFINITY(Committer->StateThread);
        YASSERT(!IsSent);

        TMetaVersion currentVersion(
            StartVersion.SegmentId,
            StartVersion.RecordCount + BatchedChanges.ysize());
        BatchedChanges.push_back(changeData);

        LOG_DEBUG("Change is added to batch (Version: %s)", ~currentVersion.ToString());

        return Result;
    }

    void SetLastChangeLogResult(TFuture<TVoid>::TPtr result)
    {
        LogResult = result;
    }

    void SendChanges()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        IsSent = true;

        LOG_DEBUG("Sending batched changes (Version: %s, ChangeCount: %d)",
            ~StartVersion.ToString(),
            BatchedChanges.ysize());

        YASSERT(~LogResult != NULL);
        Awaiter->Await(
            LogResult,
            FromMethod(&TBatch::OnLocalCommit, TPtr(this)));

        auto cellManager = Committer->CellManager;
        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;

            auto proxy = cellManager->GetMasterProxy<TProxy>(id);
            proxy->SetTimeout(Committer->Config->RpcTimeout);

            auto request = proxy->ApplyChanges();
            request->set_segmentid(StartVersion.SegmentId);
            request->set_recordcount(StartVersion.RecordCount);
            request->set_epoch(Committer->Epoch.ToProto());
            FOREACH(const auto& change, BatchedChanges) {
                request->Attachments().push_back(change);
            }

            Awaiter->Await(
                request->Invoke(),
                FromMethod(&TBatch::OnRemoteCommit, TPtr(this), id));
            LOG_DEBUG("Batched changes sent (FollowerId: %d)", id);
        }

        Awaiter->Complete(FromMethod(&TBatch::OnCompleted, TPtr(this)));
    }

    int GetChangeCount() const
    {
        VERIFY_THREAD_AFFINITY(Committer->StateThread);
        YASSERT(!IsSent);

        return BatchedChanges.ysize();
    }


private:
    bool CheckCommitQuorum()
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        if (CommitCount < Committer->CellManager->GetQuorum())
            return false;

        Result->Set(EResult::Committed);
        Awaiter->Cancel();
        
        LOG_DEBUG("Changes are committed by quorum (Version: %s, ChangeCount: %d)",
            ~StartVersion.ToString(),
            BatchedChanges.ysize());

        return true;
    }

    void OnRemoteCommit(TProxy::TRspApplyChanges::TPtr response, TPeerId peerId)
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        if (!response->IsOK()) {
            LOG_WARNING("Error committing changes by follower (Version: %s, ChangeCount: %d, FollowerId: %d, Error: %s)",
                ~StartVersion.ToString(),
                BatchedChanges.ysize(),
                peerId,
                ~response->GetError().ToString());
            return;
        }

        if (response->committed()) {
            LOG_DEBUG("Changes are committed by follower (Version: %s, ChangeCount: %d, FollowerId: %d)",
                ~StartVersion.ToString(),
                BatchedChanges.ysize(),
                peerId);

            ++CommitCount;
            CheckCommitQuorum();
        } else {
            LOG_DEBUG("Changes are acknowledged by follower (Version: %s, ChangeCount: %d, FollowerId: %d)",
                ~StartVersion.ToString(),
                BatchedChanges.ysize(),
                peerId);
        }
    }
    
    void OnLocalCommit(TVoid /* fake */)
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        LOG_DEBUG("Changes are committed locally (Version: %s, ChangeCount: %d)",
            ~StartVersion.ToString(),
            BatchedChanges.ysize());
        ++CommitCount;
        CheckCommitQuorum();
    }

    // Service invoker
    void OnCompleted()
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        if (CheckCommitQuorum())
            return;

        LOG_WARNING("Changes are uncertain (Version: %s, ChangeCount: %d, CommitCount: %d)",
            ~StartVersion.ToString(),
            BatchedChanges.ysize(),
            CommitCount);

        Result->Set(EResult::MaybeCommitted);
    }

    TFuture<TVoid>::TPtr LogResult;
    TLeaderCommitter::TPtr Committer;
    TResult::TPtr Result;
    TParallelAwaiter::TPtr Awaiter;
    TMetaVersion StartVersion;
    i32 CommitCount;
    volatile bool IsSent;
    yvector<TSharedRef> BatchedChanges;

};

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TConfig* config,
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

    TGuard<TSpinLock> guard(BatchSpinLock);
    if (~CurrentBatch != NULL) {
        FlushCurrentBatch();
    }
}

TLeaderCommitter::TResult::TPtr TLeaderCommitter::Commit(
    IAction::TPtr changeAction,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(~changeAction != NULL);

    auto version = MetaState->GetVersion();
    LOG_DEBUG("Starting commit (Version: %s)", ~version.ToString());

    auto logResult = MetaState->LogChange(version, changeData);
    auto batchResult = BatchChange(version, changeData, logResult);

    MetaState->ApplyChange(changeAction);

    LOG_DEBUG("Change is applied locally (Version: %s)", ~version.ToString());

    OnApplyChange_.Fire();

    return batchResult;
}


TLeaderCommitter::TResult::TPtr TLeaderCommitter::BatchChange(
    const TMetaVersion& version,
    const TSharedRef& changeData,
    TFuture<TVoid>::TPtr changeLogResult)
{
    TGuard<TSpinLock> guard(BatchSpinLock);
    auto batch = GetOrCreateBatch(version);
    auto result = batch->AddChange(changeData);
    batch->SetLastChangeLogResult(changeLogResult);
    if (batch->GetChangeCount() >= Config->MaxBatchSize) {
        FlushCurrentBatch();
    }
    return result;
}

void TLeaderCommitter::FlushCurrentBatch()
{
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock);
    YASSERT(~CurrentBatch != NULL);

    CurrentBatch->SendChanges();
    TDelayedInvoker::Cancel(BatchTimeoutCookie);
    CurrentBatch.Reset();
    BatchTimeoutCookie = TDelayedInvoker::NullCookie;
}

TLeaderCommitter::TBatch::TPtr TLeaderCommitter::GetOrCreateBatch(
    const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock);

    if (~CurrentBatch == NULL) {
        YASSERT(~BatchTimeoutCookie == NULL);
        CurrentBatch = New<TBatch>(TPtr(this), version);
        BatchTimeoutCookie = TDelayedInvoker::Submit(
            ~FromMethod(
                &TLeaderCommitter::DelayedFlush,
                TPtr(this),
                CurrentBatch)
            ->Via(~CancelableControlInvoker),
            Config->MaxBatchDelay);
    }

    return CurrentBatch;
}

void TLeaderCommitter::DelayedFlush(TBatch::TPtr batch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TGuard<TSpinLock> guard(BatchSpinLock);
    if (batch != CurrentBatch)
        return;

    LOG_DEBUG("Flushing batched changes");

    FlushCurrentBatch();
}

TSignal& TLeaderCommitter::OnApplyChange()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return OnApplyChange_;
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

TCommitterBase::TResult::TPtr TFollowerCommitter::Commit(
    const TMetaVersion& expectedVersion,
    const yvector<TSharedRef>& changes)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(!changes.empty());

    return
        FromMethod(
            &TFollowerCommitter::DoCommit,
            TPtr(this),
            expectedVersion,
            changes)
        ->AsyncVia(MetaState->GetStateInvoker())
        ->Do();
}

TCommitterBase::TResult::TPtr TFollowerCommitter::DoCommit(
    const TMetaVersion& expectedVersion,
    const yvector<TSharedRef>& changes)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto currentVersion = MetaState->GetVersion();
    if (currentVersion > expectedVersion) {
        LOG_WARNING("Late changes received by follower, ignored (ExpectedVersion: %s, ReceivedVersion: %s)",
            ~expectedVersion.ToString(),
            ~currentVersion.ToString());
        return New<TResult>(EResult::LateChanges);
    }

    if (currentVersion != expectedVersion) {
        LOG_WARNING("Out-of-order changes received by follower, restarting (ExpectedVersion: %s, ReceivedVersion: %s)",
            ~expectedVersion.ToString(),
            ~currentVersion.ToString());
        return New<TResult>(EResult::OutOfOrderChanges);
    }

    LOG_DEBUG("Applying changes at follower (Version: %s, ChangeCount: %d)",
        ~currentVersion.ToString(),
        changes.ysize());

    TAsyncChangeLog::TAppendResult::TPtr result;
    FOREACH (const auto& change, changes) {
        result = MetaState->LogChange(currentVersion, change);
        MetaState->ApplyChange(change);
        ++currentVersion.RecordCount;
    }

    return result->Apply(FromFunctor([] (TVoid) -> TCommitterBase::EResult
        {
            return TCommitterBase::EResult::Committed;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
