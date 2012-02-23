#include "stdafx.h"
#include "change_committer.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MetaState");
static NProfiling::TProfiler Profiler("meta_state/change_committer");

////////////////////////////////////////////////////////////////////////////////

TCommitterBase::TCommitterBase(
    TDecoratedMetaState::TPtr metaState,
    IInvoker::TPtr controlInvoker)
    : MetaState(metaState)
    , CancelableControlInvoker(New<TCancelableInvoker>(controlInvoker))
{
    YASSERT(metaState);
    YASSERT(controlInvoker);
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
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TBatch> TPtr;

    TBatch(
        TLeaderCommitter::TPtr committer,
        const TMetaVersion& version)
        : Committer(committer)
        , Result(New<TResult>())
        , StartVersion(version)
        // The local commit is also counted.
        , CommitCount(0)
        , IsSent(false)
        , Logger(MetaStateLogger)
    {
        Logger.AddTag(Sprintf("StartVersion: %s", ~StartVersion.ToString()));
    }

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

        LOG_DEBUG("Sending %d batched changes", BatchedChanges.size());
		Profiler.Enqueue("batch_size", BatchedChanges.size());

		YASSERT(LogResult);
		auto cellManager = Committer->CellManager;
        
		Awaiter = New<TParallelAwaiter>(
			~Committer->CancelableControlInvoker,
			&Profiler,
			"batch_commit_time");

		Awaiter->Await(
            LogResult,
			cellManager->GetSelfAddress(),
            FromMethod(&TBatch::OnLocalCommit, TPtr(this)));

        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;

            auto request =
                cellManager->GetMasterProxy<TProxy>(id)
                ->ApplyChanges()
                ->SetTimeout(Committer->Config->RpcTimeout);
            request->set_segment_id(StartVersion.SegmentId);
            request->set_record_count(StartVersion.RecordCount);
            request->set_epoch(Committer->Epoch.ToProto());
            FOREACH(const auto& change, BatchedChanges) {
                request->Attachments().push_back(change);
            }

            Awaiter->Await(
                request->Invoke(),
				cellManager->GetPeerAddress(id),
                FromMethod(&TBatch::OnRemoteCommit, TPtr(this), id));

            LOG_DEBUG("Changes are sent to follower %d", id);
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
        
        LOG_DEBUG("Changes are committed by quorum");

        return true;
    }

    void OnRemoteCommit(TProxy::TRspApplyChanges::TPtr response, TPeerId peerId)
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        if (!response->IsOK()) {
            LOG_WARNING("Error committing changes by follower %d\n%s",
				peerId,
                ~response->GetError().ToString());
            return;
        }

        if (response->committed()) {
            LOG_DEBUG("Changes are committed by follower %d", peerId);

            ++CommitCount;
            CheckCommitQuorum();
        } else {
            LOG_DEBUG("Changes are acknowledged by follower %d", peerId);
        }
    }
    
    void OnLocalCommit(TVoid /* fake */)
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        LOG_DEBUG("Changes are committed locally (ChangeCount: %d)",
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

        LOG_WARNING("Changes are uncertain (ChangeCount: %d, CommitCount: %d)",
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

    NLog::TTaggedLogger Logger;
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
    YASSERT(cellManager);
    YASSERT(metaState);
    YASSERT(changeLogCache);
    YASSERT(followerTracker);
    YASSERT(controlInvoker);
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
    if (CurrentBatch) {
        FlushCurrentBatch();
    }
}

TLeaderCommitter::TResult::TPtr TLeaderCommitter::Commit(
    IAction::TPtr changeAction,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(changeAction);

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
    YASSERT(CurrentBatch);

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

    if (!CurrentBatch) {
        YASSERT(!BatchTimeoutCookie);
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
    YASSERT(metaState);
    YASSERT(controlInvoker);
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
