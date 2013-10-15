#include "stdafx.h"
#include "mutation_committer.h"
#include "private.h"
#include "config.h"
#include "decorated_automaton.h"
#include "serialize.h"
#include "mutation_context.h"
#include "follower_tracker.h"
#include "changelog.h"

#include <core/concurrency/parallel_awaiter.h>

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Profiler = HydraProfiler;

////////////////////////////////////////////////////////////////////////////////

TCommitter::TCommitter(
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochAutomatonInvoker)
    : CellManager(cellManager)
    , DecoratedAutomaton(decoratedAutomaton)
    , EpochControlInvoker(epochControlInvoker)
    , EpochAutomatonInvoker(epochAutomatonInvoker)
    , CommitCounter("/commit_rate")
    , BatchFlushCounter("/batch_flush_rate")
    , Logger(HydraLogger)
{
    YCHECK(DecoratedAutomaton);
    YCHECK(EpochControlInvoker);
    YCHECK(EpochAutomatonInvoker);
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(epochAutomatonInvoker, AutomatonThread);

    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(CellManager->GetCellGuid())));
}

TCommitter::~TCommitter()
{ }

////////////////////////////////////////////////////////////////////////////////

class TLeaderCommitter::TBatch
    : public TRefCounted
{
public:
    TBatch(
        TLeaderCommitter* owner,
        TVersion startVersion)
        : Owner(owner)
        , StartVersion(startVersion)
        // The local flush is also counted.
        , FlushCount(0)
        , LocalFlushResult(NewPromise())
        , QuorumFlushResult(NewPromise())
        , Logger(Owner->Logger)
    {
        Logger.AddTag(Sprintf("StartVersion: %s", ~ToString(StartVersion)));
    }

    void AddMutation(const TSharedRef& recordData)
    {
        VERIFY_THREAD_AFFINITY(Owner->AutomatonThread);

        TVersion currentVersion(
            StartVersion.SegmentId,
            StartVersion.RecordId + BatchedRecordsData.size());
        BatchedRecordsData.push_back(recordData);

        LOG_DEBUG("Mutation is batched at version %s", ~ToString(currentVersion));
    }

    void SetLocalFlushResult(TFuture<void> result)
    {
        LocalFlushResult = std::move(result);
    }

    TFuture<void> GetQuorumFlushResult()
    {
        return QuorumFlushResult;
    }

    void Flush()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        Logger.AddTag(Sprintf("MutationCount: %d",
            static_cast<int>(BatchedRecordsData.size())));
        CommittedVersion = TVersion(StartVersion.SegmentId, StartVersion.RecordId + BatchedRecordsData.size());

        Profiler.Enqueue("/commit_batch_size", BatchedRecordsData.size());

        Awaiter = New<TParallelAwaiter>(
            Owner->EpochControlInvoker,
            &Profiler,
            "/changelog_flush_time");

        if (!BatchedRecordsData.empty()) {
            YCHECK(LocalFlushResult);
            Awaiter->Await(
                LocalFlushResult,
                Owner->CellManager->GetPeerTags(Owner->CellManager->GetSelfId()),
                BIND(&TBatch::OnLocalFlush, MakeStrong(this)));

            LOG_DEBUG("Sending mutations to followers");

            for (auto followerId = 0; followerId < Owner->CellManager->GetPeerCount(); ++followerId) {
                if (followerId == Owner->CellManager->GetSelfId())
                    continue;

                auto channel = Owner->CellManager->GetPeerChannel(followerId);
                if (!channel)
                    continue;

                LOG_DEBUG("Sending mutations to follower %d", followerId);

                THydraServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(Owner->Config->RpcTimeout);

                const auto committedVersion = Owner->DecoratedAutomaton->GetAutomatonVersion();

                auto request = proxy.LogMutations();
                request->set_start_segment_id(StartVersion.SegmentId);
                request->set_start_record_id(StartVersion.RecordId);
                request->set_committed_segment_id(committedVersion.SegmentId);
                request->set_committed_record_id(committedVersion.RecordId);
                ToProto(request->mutable_epoch_id(), Owner->EpochId);
                FOREACH (const auto& mutation, BatchedRecordsData) {
                    request->Attachments().push_back(mutation);
                }
                Awaiter->Await(
                    request->Invoke(),
                    Owner->CellManager->GetPeerTags(followerId),
                    BIND(&TBatch::OnRemoteFlush, MakeStrong(this), followerId));
            }
        }

        Awaiter->Complete(
            BIND(&TBatch::OnCompleted, MakeStrong(this)),
            Owner->CellManager->GetPeerQuorumTags());
    }

    int GetMutationCount() const
    {
        VERIFY_THREAD_AFFINITY(Owner->AutomatonThread);

        return static_cast<int>(BatchedRecordsData.size());
    }

    TVersion GetStartVersion() const
    {
        return StartVersion;
    }

    TVersion GetCommittedVersion() const
    {
        return CommittedVersion;
    }

private:
    bool CheckQuorum()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (FlushCount < Owner->CellManager->GetQuorumCount())
            return false;

        LOG_DEBUG("Mutations are flushed by quorum");

        Awaiter->Cancel();
        QuorumFlushResult.Set();

        return true;
    }

    void OnRemoteFlush(TPeerId peerId, THydraServiceProxy::TRspLogMutationsPtr response)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (!response->IsOK()) {
            LOG_WARNING(*response, "Error logging mutations at follower %d",
                peerId);
            return;
        }

        if (response->logged()) {
            LOG_DEBUG("Mutations are flushed by follower %d", peerId);

            ++FlushCount;
            CheckQuorum();
        } else {
            LOG_DEBUG("Mutations are acknowledged by follower %d", peerId);
        }
    }

    void OnLocalFlush()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        LOG_DEBUG("Mutations are flushed locally");

        ++FlushCount;
        CheckQuorum();
    }

    void OnCompleted()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        auto error = TError(
            NHydra::EErrorCode::MaybeCommitted,
            "Mutations are uncertain: %d out of %d commits were successful",
            FlushCount,
            Owner->CellManager->GetQuorumCount());
        Owner->EpochAutomatonInvoker->Invoke(
            BIND(&TLeaderCommitter::OnBatchCommitted, Owner, MakeStrong(this), error));
    }


    // NB: TBatch cannot outlive its owner.
    TLeaderCommitter* Owner;
    TVersion StartVersion;

    int FlushCount;

    TParallelAwaiterPtr Awaiter;
    TFuture<void> LocalFlushResult;
    TPromise<void> QuorumFlushResult;
    std::vector<TSharedRef> BatchedRecordsData;
    TVersion CommittedVersion;

    NLog::TTaggedLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TLeaderCommitterConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    TFollowerTrackerPtr followerTracker,
    const TEpochId& epochId,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochAutomatonInvoker)
    : TCommitter(
        cellManager,
        decoratedAutomaton,
        epochControlInvoker,
        epochAutomatonInvoker)
    , Config(config)
    , ChangelogStore(changelogStore)
    , FollowerTracker(followerTracker)
    , EpochId(epochId)
    , LoggingSuspended(false)
{
    YCHECK(Config);
    YCHECK(CellManager);
    YCHECK(ChangelogStore);
    YCHECK(FollowerTracker);
}

TLeaderCommitter::~TLeaderCommitter()
{ }

TFuture< TErrorOr<TMutationResponse> > TLeaderCommitter::Commit(const TMutationRequest& request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (LoggingSuspended) {
        TPendingMutation pendingMutation;
        pendingMutation.Request = request;
        pendingMutation.CommitPromise = NewPromise<TErrorOr<TMutationResponse>>();
        PendingMutations.push(pendingMutation);

        return pendingMutation.CommitPromise;
    } else {
        auto version = DecoratedAutomaton->GetLoggedVersion();

        TSharedRef recordData;
        TFuture<void> logResult;
        auto commitResult = NewPromise<TErrorOr<TMutationResponse>>();
        DecoratedAutomaton->LogMutationAtLeader(
            request,
            &recordData,
            &logResult,
            commitResult);

        AddToBatch(version, std::move(recordData), std::move(logResult));

        auto period = Config->ChangelogRotationPeriod;
        if (period && (version.RecordId + 1) % *period == 0) {
            ChangelogLimitReached_.Fire();
        }

        return commitResult;
    }
}

void TLeaderCommitter::Flush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TGuard<TSpinLock> guard(BatchSpinLock);
    if (CurrentBatch) {
        FlushCurrentBatch();
    }
}

TFuture<void> TLeaderCommitter::GetQuorumFlushResult()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TGuard<TSpinLock> guard(BatchSpinLock);
    return CurrentBatch ? CurrentBatch->GetQuorumFlushResult() : MakeFuture();
}

void TLeaderCommitter::SuspendLogging()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(!LoggingSuspended);

    LOG_DEBUG("Mutations logging suspended");

    LoggingSuspended = true;
    YCHECK(PendingMutations.empty());
}

void TLeaderCommitter::ResumeLogging()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(LoggingSuspended);

    LOG_DEBUG("Mutations logging resumed");

    while (!PendingMutations.empty()) {
        auto& pendingMutation = PendingMutations.front();
        auto version = DecoratedAutomaton->GetAutomatonVersion();

        TSharedRef recordData;
        TFuture<void> logResult;
        DecoratedAutomaton->LogMutationAtLeader(
            pendingMutation.Request,
            &recordData,
            &logResult,
            pendingMutation.CommitPromise);

        AddToBatch(version, recordData, logResult);

        PendingMutations.pop();
    }

    LoggingSuspended = false;
    YCHECK(PendingMutations.empty());
}

void TLeaderCommitter::AddToBatch(
    TVersion version,
    const TSharedRef& recordData,
    TFuture<void> localResult)
{
    TGuard<TSpinLock> guard(BatchSpinLock);
    auto batch = GetOrCreateBatch(version);
    batch->AddMutation(recordData);
    batch->SetLocalFlushResult(localResult);
    if (batch->GetMutationCount() >= Config->MaxBatchSize) {
        FlushCurrentBatch();
    }
}

void TLeaderCommitter::FlushCurrentBatch()
{
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock);
    YCHECK(CurrentBatch);

    CurrentBatch->Flush();
    CurrentBatch.Reset();

    TDelayedExecutor::CancelAndClear(BatchTimeoutCookie);

    Profiler.Increment(BatchFlushCounter);
}

TLeaderCommitter::TBatchPtr TLeaderCommitter::GetOrCreateBatch(TVersion version)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock);

    if (!CurrentBatch) {
        CurrentBatch = New<TBatch>(this, version);

        CurrentBatch->GetQuorumFlushResult().Subscribe(
            BIND(&TLeaderCommitter::OnBatchCommitted, MakeWeak(this), CurrentBatch, TError())
                .Via(EpochAutomatonInvoker));

        YCHECK(!BatchTimeoutCookie);
        BatchTimeoutCookie = TDelayedExecutor::Submit(
            BIND(&TLeaderCommitter::OnBatchTimeout, MakeWeak(this), CurrentBatch)
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

    FlushCurrentBatch();
}

void TLeaderCommitter::OnBatchCommitted(TBatchPtr batch, TError error)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    DecoratedAutomaton->CommitMutations(batch->GetCommittedVersion());
}

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochAutomatonInvoker)
    : TCommitter(
        cellManager,
        decoratedAutomaton,
        epochControlInvoker,
        epochAutomatonInvoker)
{ }

TFollowerCommitter::~TFollowerCommitter()
{ }

TAsyncError TFollowerCommitter::LogMutations(
    TVersion expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(!recordsData.empty());

    Profiler.Increment(CommitCounter, recordsData.size());
    Profiler.Increment(BatchFlushCounter);

    return
        BIND(
            &TFollowerCommitter::DoLogMutations,
            MakeStrong(this),
            expectedVersion,
            recordsData)
        .AsyncVia(EpochAutomatonInvoker)
        .Run();
}

TAsyncError TFollowerCommitter::DoLogMutations(
    TVersion expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto currentVersion = DecoratedAutomaton->GetLoggedVersion();
    if (currentVersion != expectedVersion) {
        return MakeFuture(TError(
            NHydra::EErrorCode::OutOfOrderMutations,
            "Out-of-order mutations received by follower: expected %s but got %s",
            ~ToString(currentVersion),
            ~ToString(expectedVersion)));
    }

    TFuture<void> logResult;
    FOREACH (const auto& recordData, recordsData) {
        DecoratedAutomaton->LogMutationAtFollower(recordData, &logResult);
    }

    return logResult.Apply(BIND([] () -> TError {
        return TError();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
