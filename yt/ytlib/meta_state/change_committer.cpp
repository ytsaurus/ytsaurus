#include "stdafx.h"
#include "change_committer.h"
#include "meta_version.h"
#include "decorated_meta_state.h"
#include "change_log_cache.h"
#include "follower_tracker.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MetaState");
static NProfiling::TProfiler Profiler("meta_state");

////////////////////////////////////////////////////////////////////////////////

TCommitter::TCommitter(
    TDecoratedMetaState* metaState,
    IInvoker* epochControlInvoker,
    IInvoker* epochStateInvoker)
    : MetaState(metaState)
    , EpochControlInvoker(epochControlInvoker)
    , EpochStateInvoker(epochStateInvoker)
    , CommitCounter("commit_rate")
    , BatchCommitCounter("commit_batch_rate")
{
    YASSERT(metaState);
    YASSERT(epochControlInvoker);
    YASSERT(epochStateInvoker);
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(epochStateInvoker, StateThread);
}

////////////////////////////////////////////////////////////////////////////////

class TLeaderCommitter::TBatch
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TBatch> TPtr;

    TBatch(
        TLeaderCommitterPtr committer,
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
            StartVersion.RecordCount + BatchedChanges.size());
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

        LOG_DEBUG("Sending %d batched changes", static_cast<int>(BatchedChanges.size()));
		Profiler.Enqueue("commit_batch_size", BatchedChanges.size());

		YASSERT(LogResult);
		auto cellManager = Committer->CellManager;
        
		Awaiter = New<TParallelAwaiter>(
			~Committer->EpochControlInvoker,
			&Profiler,
			"commit_batch_time");

		Awaiter->Await(
            LogResult,
			cellManager->GetSelfAddress(),
            FromMethod(&TBatch::OnLocalCommit, MakeStrong(this)));

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
                FromMethod(&TBatch::OnRemoteCommit, MakeStrong(this), id));

            LOG_DEBUG("Changes are sent to follower %d", id);
        }

        Awaiter->Complete(FromMethod(&TBatch::OnCompleted, MakeStrong(this)));
    }

    int GetChangeCount() const
    {
        VERIFY_THREAD_AFFINITY(Committer->StateThread);
        YASSERT(!IsSent);

        return static_cast<int>(BatchedChanges.size());
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
            static_cast<int>(BatchedChanges.size()));
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
            static_cast<int>(BatchedChanges.size()),
            CommitCount);

        Result->Set(EResult::MaybeCommitted);
    }

    TLeaderCommitterPtr Committer;
    TResult::TPtr Result;
    TMetaVersion StartVersion;
    i32 CommitCount;
    volatile bool IsSent;
    NLog::TTaggedLogger Logger;

    TParallelAwaiter::TPtr Awaiter;
    TFuture<TVoid>::TPtr LogResult;
    std::vector<TSharedRef> BatchedChanges;

};

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TConfig* config,
    TCellManager* cellManager,
    TDecoratedMetaState* metaState,
    TChangeLogCache* changeLogCache,
    TFollowerTracker* followerTracker,
    const TEpoch& epoch,
    IInvoker* epochControlInvoker,
    IInvoker* epochStateInvoker)
    : TCommitter(metaState, epochControlInvoker, epochStateInvoker)
    , Config(config)
    , CellManager(cellManager)
    , ChangeLogCache(changeLogCache)
    , FollowerTracker(followerTracker)
    , Epoch(epoch)
{
    YASSERT(config);
    YASSERT(cellManager);
    YASSERT(changeLogCache);
    YASSERT(followerTracker);
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

    ChangeApplied_.Fire();

    Profiler.Increment(CommitCounter);

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
    BatchTimeoutCookie = NULL;
    Profiler.Increment(BatchCommitCounter);
}

TLeaderCommitter::TBatch::TPtr TLeaderCommitter::GetOrCreateBatch(
    const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock);

    if (!CurrentBatch) {
        YASSERT(!BatchTimeoutCookie);
        CurrentBatch = New<TBatch>(MakeStrong(this), version);
        BatchTimeoutCookie = TDelayedInvoker::Submit(
            ~FromMethod(
                &TLeaderCommitter::DelayedFlush,
                MakeStrong(this),
                CurrentBatch)
            ->Via(~EpochControlInvoker),
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

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TDecoratedMetaState* metaState,
    IInvoker* epochControlInvoker,
    IInvoker* epochStateInvoker)
    : TCommitter(metaState, epochControlInvoker, epochStateInvoker)
{ }

TCommitter::TResult::TPtr TFollowerCommitter::Commit(
    const TMetaVersion& expectedVersion,
    const std::vector<TSharedRef>& changes)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(!changes.empty());

    Profiler.Increment(CommitCounter, changes.size());
    Profiler.Increment(BatchCommitCounter);

    return
        FromMethod(
            &TFollowerCommitter::DoCommit,
            MakeStrong(this),
            expectedVersion,
            changes)
        ->AsyncVia(EpochStateInvoker)
        ->Do();
}

TCommitter::TResult::TPtr TFollowerCommitter::DoCommit(
    const TMetaVersion& expectedVersion,
    const std::vector<TSharedRef>& changes)
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
        static_cast<int>(changes.size()));

    TAsyncChangeLog::TAppendResult::TPtr result;
    FOREACH (const auto& change, changes) {
        result = MetaState->LogChange(currentVersion, change);
        MetaState->ApplyChange(change);
        ++currentVersion.RecordCount;
    }

    return result->Apply(FromFunctor([] (TVoid) -> TCommitter::EResult {
        return TCommitter::EResult::Committed;
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
