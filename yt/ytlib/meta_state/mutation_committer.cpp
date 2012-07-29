#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "mutation_committer.h"
#include "meta_version.h"
#include "decorated_meta_state.h"
#include "change_log_cache.h"
#include "follower_tracker.h"

#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static NProfiling::TProfiler& Profiler = MetaStateProfiler;

////////////////////////////////////////////////////////////////////////////////

TCommitter::TCommitter(
    TDecoratedMetaStatePtr metaState,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochStateInvoker)
    : MetaState(metaState)
    , EpochControlInvoker(epochControlInvoker)
    , EpochStateInvoker(epochStateInvoker)
    , CommitCounter("/commit_rate")
    , BatchCommitCounter("/commit_batch_rate")
    , CommitTimeCounter("/commit_time")
{
    YASSERT(metaState);
    YASSERT(epochControlInvoker);
    YASSERT(epochStateInvoker);
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(epochStateInvoker, StateThread);
}

TCommitter::~TCommitter()
{ }

////////////////////////////////////////////////////////////////////////////////

class TLeaderCommitter::TBatch
    : public TRefCounted
{
public:
    TBatch(
        TLeaderCommitterPtr committer,
        const TMetaVersion& startVersion)
        : Committer(committer)
        , Promise(NewPromise<TCommitPromise::TValueType>())
        , StartVersion(startVersion)
        // The local commit is also counted.
        , CommitCount(0)
        , IsSent(false)
        , Logger(MetaStateLogger)
    {
        Logger.AddTag(Sprintf("StartVersion: %s", ~StartVersion.ToString()));
    }

    TCommitResult AddMutation(const TSharedRef& recordData)
    {
        VERIFY_THREAD_AFFINITY(Committer->StateThread);
        YASSERT(!IsSent);

        TMetaVersion currentVersion(
            StartVersion.SegmentId,
            StartVersion.RecordCount + BatchedRecordsData.size());
        BatchedRecordsData.push_back(recordData);

        LOG_DEBUG("Mutation is added to batch (Version: %s)", ~currentVersion.ToString());

        return Promise;
    }

    void SetLastChangeLogResult(TFuture<void> result)
    {
        LogResult = result;
    }

    void FlushMutations(bool rotateChangeLog)
    {
        Logger.AddTag(Sprintf("MutationCount: %d", static_cast<int>(BatchedRecordsData.size())));
        Committer->EpochControlInvoker->Invoke(BIND(
            &TBatch::DoFlushMutations,
            MakeStrong(this),
            rotateChangeLog));
    }

    int GetMutationCount() const
    {
        VERIFY_THREAD_AFFINITY(Committer->StateThread);
        YASSERT(!IsSent);

        return static_cast<int>(BatchedRecordsData.size());
    }

private:
    void DoFlushMutations(bool rotateChangeLog)
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        IsSent = true;

        if (!BatchedRecordsData.empty()) {
            Profiler.Enqueue("/commit_batch_size", BatchedRecordsData.size());

            YASSERT(!LogResult.IsNull());
            auto cellManager = Committer->CellManager;

            Awaiter = New<TParallelAwaiter>(
                ~Committer->EpochControlInvoker,
                &Profiler,
                "/commit_batch_time");

            Awaiter->Await(
                LogResult,
                EscapeYPathToken(cellManager->GetSelfAddress()),
                BIND(&TBatch::OnLocalCommit, MakeStrong(this)));

            LOG_DEBUG("Sending batched mutations to followers");
            for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
                if (id == cellManager->GetSelfId()) continue;

                LOG_DEBUG("Sending mutations to follower %d", id);

                auto request =
                    cellManager->GetMasterProxy<TProxy>(id)
                    ->ApplyMutations()
                    ->SetTimeout(Committer->Config->RpcTimeout);
                request->set_segment_id(StartVersion.SegmentId);
                request->set_record_count(StartVersion.RecordCount);
                *request->mutable_epoch() = Committer->Epoch.ToProto();
                FOREACH (const auto& mutation, BatchedRecordsData) {
                    request->Attachments().push_back(mutation);
                }
                Awaiter->Await(
                    request->Invoke(),
                    EscapeYPathToken(cellManager->GetPeerAddress(id)),
                    BIND(&TBatch::OnRemoteCommit, MakeStrong(this), id));
            }
            LOG_DEBUG("Batched mutations sent");

            Awaiter->Complete(BIND(&TBatch::OnCompleted, MakeStrong(this)));

        }
        
        // This is the version the next batch will have.
        Committer->MetaState->SetPingVersion(
            rotateChangeLog
            ? TMetaVersion(StartVersion.SegmentId + 1, 0)
            : TMetaVersion(StartVersion.SegmentId, StartVersion.RecordCount + BatchedRecordsData.size()));
    }

    bool CheckCommitQuorum()
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        if (CommitCount < Committer->CellManager->GetQuorum())
            return false;

        Promise.Set(EResult::Committed);
        Awaiter->Cancel();
        
        LOG_DEBUG("Mutations are committed by quorum");

        return true;
    }

    void OnRemoteCommit(TPeerId peerId, TProxy::TRspApplyMutationsPtr response)
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        if (!response->IsOK()) {
            LOG_WARNING("Error committing mutations by follower %d\n%s",
                peerId,
                ~response->GetError().ToString());
            return;
        }

        if (response->committed()) {
            LOG_DEBUG("Mutations are committed by follower %d", peerId);

            ++CommitCount;
            CheckCommitQuorum();
        } else {
            LOG_DEBUG("Mutations are acknowledged by follower %d", peerId);
        }
    }
    
    void OnLocalCommit()
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        LOG_DEBUG("Mutations are committed locally");
        ++CommitCount;
        CheckCommitQuorum();
    }

    void OnCompleted()
    {
        VERIFY_THREAD_AFFINITY(Committer->ControlThread);

        if (CheckCommitQuorum())
            return;

        LOG_WARNING("Mutations are uncertain (CommitCount: %d)", CommitCount);

        Promise.Set(EResult::MaybeCommitted);
    }

    TLeaderCommitterPtr Committer;
    TCommitPromise Promise;
    TMetaVersion StartVersion;
    int CommitCount;
    volatile bool IsSent;
    NLog::TTaggedLogger Logger;

    TParallelAwaiterPtr Awaiter;
    TFuture<void> LogResult;
    std::vector<TSharedRef> BatchedRecordsData;

};

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TLeaderCommitterConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    TChangeLogCachePtr changeLogCache,
    TFollowerTrackerPtr followerTracker,
    const TEpoch& epoch,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochStateInvoker)
    : TCommitter(decoratedState, epochControlInvoker, epochStateInvoker)
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

TLeaderCommitter::~TLeaderCommitter()
{ }

void TLeaderCommitter::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Do nothing.
}

void TLeaderCommitter::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Kill the cyclic reference.
    TGuard<TSpinLock> guard(BatchSpinLock);
    CurrentBatch.Reset();
    TDelayedInvoker::CancelAndClear(BatchTimeoutCookie);
}

void TLeaderCommitter::Flush(bool rotateChangeLog)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TGuard<TSpinLock> guard(BatchSpinLock);
    // If no current batch exists but the changelog is about to be rotated
    // we have to create a dummy batch and flush it to update ping version.
    if (rotateChangeLog && !CurrentBatch) {
        auto version = MetaState->GetVersion();
        GetOrCreateBatch(version);
    }
    if (CurrentBatch) {
        FlushCurrentBatch(rotateChangeLog);
    }
}

TLeaderCommitter::TCommitResult TLeaderCommitter::Commit(
    const TSharedRef& recordData,
    const TClosure& mutationAction)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(!mutationAction.IsNull());

    PROFILE_AGGREGATED_TIMING (CommitTimeCounter) {
        auto version = MetaState->GetVersion();
        LOG_DEBUG("Starting commit at version %s", ~version.ToString());

        auto logResult = MetaState->LogMutation(version, recordData);
        auto batchResult = AddMutationToBatch(version, recordData, logResult);

        MetaState->ApplyMutation(recordData, mutationAction);

        LOG_DEBUG("Change is applied locally at version %s", ~version.ToString());

        MutationApplied_.Fire();

        Profiler.Increment(CommitCounter);

        return batchResult;
    }
}

TLeaderCommitter::TCommitResult TLeaderCommitter::AddMutationToBatch(
    const TMetaVersion& version,
    const TSharedRef& recordData,
    TFuture<void> changeLogResult)
{
    TGuard<TSpinLock> guard(BatchSpinLock);
    auto batch = GetOrCreateBatch(version);
    auto result = batch->AddMutation(recordData);
    batch->SetLastChangeLogResult(changeLogResult);
    if (batch->GetMutationCount() >= Config->MaxBatchSize) {
        FlushCurrentBatch(false);
    }
    return result;
}

void TLeaderCommitter::FlushCurrentBatch(bool rotateChangeLog)
{
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock);
    YASSERT(CurrentBatch);

    CurrentBatch->FlushMutations(rotateChangeLog);
    TDelayedInvoker::CancelAndClear(BatchTimeoutCookie);
    CurrentBatch.Reset();
    Profiler.Increment(BatchCommitCounter);
}

TLeaderCommitter::TBatchPtr TLeaderCommitter::GetOrCreateBatch(
    const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock);

    if (!CurrentBatch) {
        YASSERT(!BatchTimeoutCookie);
        CurrentBatch = New<TBatch>(MakeStrong(this), version);
        BatchTimeoutCookie = TDelayedInvoker::Submit(
            BIND(
                &TLeaderCommitter::OnBatchTimeout,
                MakeStrong(this),
                CurrentBatch)
            .Via(EpochControlInvoker),
            Config->MaxBatchDelay);
    }

    return CurrentBatch;
}

void TLeaderCommitter::OnBatchTimeout(TBatchPtr batch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TGuard<TSpinLock> guard(BatchSpinLock);
    if (batch != CurrentBatch)
        return;

    LOG_DEBUG("Flushing batched mutations");

    FlushCurrentBatch(false);
}

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TDecoratedMetaStatePtr metaState,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochStateInvoker)
    : TCommitter(metaState, epochControlInvoker, epochStateInvoker)
{ }

TFollowerCommitter::~TFollowerCommitter()
{ }

TCommitter::TCommitResult TFollowerCommitter::Commit(
    const TMetaVersion& expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(!recordsData.empty());

    PROFILE_AGGREGATED_TIMING (CommitTimeCounter) {
        Profiler.Increment(CommitCounter, recordsData.size());
        Profiler.Increment(BatchCommitCounter);

        return
            BIND(
                &TFollowerCommitter::DoCommit,
                MakeStrong(this),
                expectedVersion,
                recordsData)
            .AsyncVia(EpochStateInvoker)
            .Run();
    }
}

TCommitter::TCommitResult TFollowerCommitter::DoCommit(
    const TMetaVersion& expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto currentVersion = MetaState->GetVersion();
    if (currentVersion > expectedVersion) {
        LOG_WARNING("Late mutations received by follower, ignored: expected %s but got %s",
            ~currentVersion.ToString(),
            ~expectedVersion.ToString());
        return MakeFuture(EResult(EResult::LateMutations));
    }

    if (currentVersion != expectedVersion) {
        LOG_WARNING("Out-of-order mutations received by follower, restarting: expected %s but got %s",
            ~currentVersion.ToString(),
            ~expectedVersion.ToString());
        return MakeFuture(EResult(EResult::OutOfOrderMutations));
    }

    LOG_DEBUG("Applying %d mutations at version %s",
        static_cast<int>(recordsData.size()),
        ~currentVersion.ToString());

    TFuture<void> result;
    FOREACH (const auto& recordData, recordsData) {
        result = MetaState->LogMutation(currentVersion, recordData);
        
        MetaState->ApplyMutation(recordData);
        
        ++currentVersion.RecordCount;
    }

    return result.Apply(BIND([] () -> TCommitter::EResult {
        return TCommitter::EResult::Committed;
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
