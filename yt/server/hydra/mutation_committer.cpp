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

#include <core/profiling/profiler.h>

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TCommitter::TCommitter(
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochAutomatonInvoker,
    const NProfiling::TProfiler& profiler)
    : CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , EpochControlInvoker_(epochControlInvoker)
    , EpochAutomatonInvoker_(epochAutomatonInvoker)
    , CommitCounter_("/commit_rate")
    , BatchFlushCounter_("/batch_flush_rate")
    , Logger(HydraLogger)
    , Profiler(profiler)
{
    YCHECK(DecoratedAutomaton_);
    YCHECK(EpochControlInvoker_);
    YCHECK(EpochAutomatonInvoker_);
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(epochAutomatonInvoker, AutomatonThread);

    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(CellManager_->GetCellGuid())));
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
        : Owner_(owner)
        , StartVersion_(startVersion)
        // The local flush is also counted.
        , FlushCount_(0)
        , LocalFlushResult_(NewPromise())
        , QuorumFlushResult_(NewPromise())
        , Logger(Owner_->Logger)
    {
        Logger.AddTag(Sprintf("StartVersion: %s", ~ToString(StartVersion_)));
    }

    void AddMutation(const TSharedRef& recordData)
    {
        TVersion currentVersion(
            StartVersion_.SegmentId,
            StartVersion_.RecordId + BatchedRecordsData_.size());
        BatchedRecordsData_.push_back(recordData);

        LOG_DEBUG("Mutation is batched at version %s", ~ToString(currentVersion));
    }

    void SetLocalFlushResult(TFuture<void> result)
    {
        LocalFlushResult_ = std::move(result);
    }

    TFuture<void> GetQuorumFlushResult()
    {
        return QuorumFlushResult_;
    }

    void Flush()
    {
        int mutationCount = static_cast<int>(BatchedRecordsData_.size());
        Logger.AddTag(Sprintf("MutationCount: %d", mutationCount));
        CommittedVersion_ = TVersion(StartVersion_.SegmentId, StartVersion_.RecordId + mutationCount);

        Owner_->Profiler.Enqueue("/commit_batch_size", mutationCount);

        Awaiter_ = New<TParallelAwaiter>(Owner_->EpochControlInvoker_);

        Timer_ = Owner_->Profiler.TimingStart(
            "/changelog_flush_time",
            NProfiling::EmptyTagIds,
            NProfiling::ETimerMode::Parallel);

        if (!BatchedRecordsData_.empty()) {
            YCHECK(LocalFlushResult_);
            Awaiter_->Await(
                LocalFlushResult_,
                BIND(&TBatch::OnLocalFlush, MakeStrong(this)));

            for (auto followerId = 0; followerId < Owner_->CellManager_->GetPeerCount(); ++followerId) {
                if (followerId == Owner_->CellManager_->GetSelfId())
                    continue;

                auto channel = Owner_->CellManager_->GetPeerChannel(followerId);
                if (!channel)
                    continue;

                LOG_DEBUG("Sending mutations to follower %d", followerId);

                THydraServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(Owner_->Config_->RpcTimeout);

                const auto committedVersion = Owner_->DecoratedAutomaton_->GetAutomatonVersion();

                auto request = proxy.LogMutations();
                ToProto(request->mutable_epoch_id(), Owner_->EpochId_);
                request->set_start_revision(StartVersion_.ToRevision());
                request->set_committed_revision(committedVersion.ToRevision());
                request->Attachments().insert(
                    request->Attachments().end(),
                    BatchedRecordsData_.begin(),
                    BatchedRecordsData_.end());

                Awaiter_->Await(
                    request->Invoke(),
                    BIND(&TBatch::OnRemoteFlush, MakeStrong(this), followerId));
            }
        }

        Awaiter_->Complete(BIND(&TBatch::OnCompleted, MakeStrong(this)));
    }

    int GetMutationCount() const
    {
        return static_cast<int>(BatchedRecordsData_.size());
    }

    TVersion GetStartVersion() const
    {
        return StartVersion_;
    }

    TVersion GetCommittedVersion() const
    {
        return CommittedVersion_;
    }

private:
    bool CheckQuorum()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (FlushCount_ < Owner_->CellManager_->GetQuorumCount())
            return false;

        LOG_DEBUG("Mutations are flushed by quorum");

        Owner_->Profiler.TimingCheckpoint(
            Timer_,
            Owner_->CellManager_->GetPeerQuorumTags());

        Awaiter_->Cancel();

        QuorumFlushResult_.Set();

        return true;
    }

    void OnRemoteFlush(TPeerId followerId, THydraServiceProxy::TRspLogMutationsPtr response)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        Owner_->Profiler.TimingCheckpoint(
            Timer_,
            Owner_->CellManager_->GetPeerTags(followerId));

        if (!response->IsOK()) {
            LOG_WARNING(*response, "Error logging mutations at follower %d",
                followerId);
            return;
        }

        if (response->logged()) {
            LOG_DEBUG("Mutations are flushed by follower %d", followerId);

            ++FlushCount_;
            CheckQuorum();
        } else {
            LOG_DEBUG("Mutations are acknowledged by follower %d", followerId);
        }
    }

    void OnLocalFlush()
    {
        LOG_DEBUG("Mutations are flushed locally");

        Owner_->Profiler.TimingCheckpoint(
            Timer_,
            Owner_->CellManager_->GetPeerTags(Owner_->CellManager_->GetSelfId()));

        ++FlushCount_;
        CheckQuorum();
    }

    void OnCompleted()
    {
        Owner_->Profiler.TimingCheckpoint(
            Timer_,
            Owner_->CellManager_->GetPeerQuorumTags());

        auto error = TError(
            NHydra::EErrorCode::MaybeCommitted,
            "Mutations are uncertain: %d out of %d commits were successful",
            FlushCount_,
            Owner_->CellManager_->GetQuorumCount());

        Owner_->EpochAutomatonInvoker_->Invoke(BIND(
            &TLeaderCommitter::OnBatchCommitted,
            MakeStrong(Owner_),
            MakeStrong(this),
            error));
    }


    // NB: TBatch cannot outlive its owner.
    TLeaderCommitter* Owner_;
    TVersion StartVersion_;

    int FlushCount_;

    TParallelAwaiterPtr Awaiter_;
    TFuture<void> LocalFlushResult_;
    TPromise<void> QuorumFlushResult_;
    std::vector<TSharedRef> BatchedRecordsData_;
    TVersion CommittedVersion_;

    NLog::TTaggedLogger Logger;
    
    NProfiling::TTimer Timer_;

};

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    TFollowerTrackerPtr followerTracker,
    const TEpochId& epochId,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochAutomatonInvoker,
    const NProfiling::TProfiler& profiler)
    : TCommitter(
        cellManager,
        decoratedAutomaton,
        epochControlInvoker,
        epochAutomatonInvoker,
        profiler)
    , Config_(config)
    , ChangelogStore_(changelogStore)
    , FollowerTracker_(followerTracker)
    , EpochId_(epochId)
    , LoggingSuspended_(false)
{
    YCHECK(Config_);
    YCHECK(CellManager_);
    YCHECK(ChangelogStore_);
    YCHECK(FollowerTracker_);
}

TLeaderCommitter::~TLeaderCommitter()
{ }

TFuture< TErrorOr<TMutationResponse> > TLeaderCommitter::Commit(const TMutationRequest& request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (LoggingSuspended_) {
        TPendingMutation pendingMutation;
        pendingMutation.Request = request;
        pendingMutation.CommitPromise = NewPromise<TErrorOr<TMutationResponse>>();
        PendingMutations_.push(pendingMutation);

        return pendingMutation.CommitPromise;
    } else {
        auto version = DecoratedAutomaton_->GetLoggedVersion();

        TSharedRef recordData;
        TFuture<void> logResult;
        auto commitResult = NewPromise<TErrorOr<TMutationResponse>>();
        DecoratedAutomaton_->LogMutationAtLeader(
            request,
            &recordData,
            &logResult,
            commitResult);

        AddToBatch(version, std::move(recordData), std::move(logResult));

        auto period = Config_->LeaderCommitter->ChangelogRotationPeriod;
        if (period && (version.RecordId + 1) % *period == 0) {
            ChangelogLimitReached_.Fire();
        }

        return commitResult;
    }
}

void TLeaderCommitter::Flush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TGuard<TSpinLock> guard(BatchSpinLock_);
    if (CurrentBatch_) {
        FlushCurrentBatch();
    }
}

TFuture<void> TLeaderCommitter::GetQuorumFlushResult()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TGuard<TSpinLock> guard(BatchSpinLock_);
    return CurrentBatch_ ? CurrentBatch_->GetQuorumFlushResult() : MakeFuture();
}

void TLeaderCommitter::SuspendLogging()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(!LoggingSuspended_);

    LOG_DEBUG("Mutations logging suspended");

    LoggingSuspended_ = true;
    YCHECK(PendingMutations_.empty());
}

void TLeaderCommitter::ResumeLogging()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(LoggingSuspended_);

    LOG_DEBUG("Mutations logging resumed");

    while (!PendingMutations_.empty()) {
        auto& pendingMutation = PendingMutations_.front();
        auto version = DecoratedAutomaton_->GetAutomatonVersion();

        TSharedRef recordData;
        TFuture<void> logResult;
        DecoratedAutomaton_->LogMutationAtLeader(
            pendingMutation.Request,
            &recordData,
            &logResult,
            pendingMutation.CommitPromise);

        AddToBatch(version, recordData, logResult);

        PendingMutations_.pop();
    }

    LoggingSuspended_ = false;
    YCHECK(PendingMutations_.empty());
}

void TLeaderCommitter::AddToBatch(
    TVersion version,
    const TSharedRef& recordData,
    TFuture<void> localResult)
{
    TGuard<TSpinLock> guard(BatchSpinLock_);
    auto batch = GetOrCreateBatch(version);
    batch->AddMutation(recordData);
    batch->SetLocalFlushResult(localResult);
    if (batch->GetMutationCount() >= Config_->LeaderCommitter->MaxBatchSize) {
        FlushCurrentBatch();
    }
}

void TLeaderCommitter::FlushCurrentBatch()
{
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock_);
    YCHECK(CurrentBatch_);

    CurrentBatch_->Flush();
    CurrentBatch_.Reset();

    TDelayedExecutor::CancelAndClear(BatchTimeoutCookie_);

    Profiler.Increment(BatchFlushCounter_);
}

TLeaderCommitter::TBatchPtr TLeaderCommitter::GetOrCreateBatch(TVersion version)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock_);

    if (!CurrentBatch_) {
        CurrentBatch_ = New<TBatch>(this, version);

        CurrentBatch_->GetQuorumFlushResult().Subscribe(
            BIND(&TLeaderCommitter::OnBatchCommitted, MakeWeak(this), CurrentBatch_, TError())
                .Via(EpochAutomatonInvoker_));

        YCHECK(!BatchTimeoutCookie_);
        BatchTimeoutCookie_ = TDelayedExecutor::Submit(
            BIND(&TLeaderCommitter::OnBatchTimeout, MakeWeak(this), CurrentBatch_)
                .Via(EpochControlInvoker_),
            Config_->LeaderCommitter->MaxBatchDelay);
    }

    return CurrentBatch_;
}

void TLeaderCommitter::OnBatchTimeout(TBatchPtr batch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TGuard<TSpinLock> guard(BatchSpinLock_);
    if (batch != CurrentBatch_)
        return;

    LOG_DEBUG("Flushing batched mutations");

    FlushCurrentBatch();
}

void TLeaderCommitter::OnBatchCommitted(TBatchPtr batch, TError error)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    DecoratedAutomaton_->CommitMutations(batch->GetCommittedVersion());
}

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochAutomatonInvoker,
    const NProfiling::TProfiler& profiler)
    : TCommitter(
        cellManager,
        decoratedAutomaton,
        epochControlInvoker,
        epochAutomatonInvoker,
        profiler)
{ }

TFollowerCommitter::~TFollowerCommitter()
{ }

TAsyncError TFollowerCommitter::LogMutations(
    TVersion expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(!recordsData.empty());

    Profiler.Increment(CommitCounter_, recordsData.size());
    Profiler.Increment(BatchFlushCounter_);

    return
        BIND(
            &TFollowerCommitter::DoLogMutations,
            MakeStrong(this),
            expectedVersion,
            recordsData)
        .AsyncVia(EpochAutomatonInvoker_)
        .Run();
}

TAsyncError TFollowerCommitter::DoLogMutations(
    TVersion expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto currentVersion = DecoratedAutomaton_->GetLoggedVersion();
    if (currentVersion != expectedVersion) {
        return MakeFuture(TError(
            NHydra::EErrorCode::OutOfOrderMutations,
            "Out-of-order mutations received by follower: expected %s but got %s",
            ~ToString(currentVersion),
            ~ToString(expectedVersion)));
    }

    TFuture<void> logResult;
    for (const auto& recordData : recordsData) {
        DecoratedAutomaton_->LogMutationAtFollower(recordData, &logResult);
    }

    return logResult.Apply(BIND([] () -> TError {
        return TError();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
