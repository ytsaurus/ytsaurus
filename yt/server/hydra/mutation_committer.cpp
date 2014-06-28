#include "stdafx.h"
#include "mutation_committer.h"
#include "private.h"
#include "config.h"
#include "decorated_automaton.h"
#include "serialize.h"
#include "mutation_context.h"
#include "changelog.h"

#include <core/concurrency/parallel_awaiter.h>

#include <core/profiling/profiler.h>

#include <core/tracing/trace_context.h>

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
    TEpochContextPtr epochContext,
    const NProfiling::TProfiler& profiler)
    : CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , EpochContext_(epochContext)
    , CommitCounter_("/commit_rate")
    , BatchFlushCounter_("/batch_flush_rate")
    , Logger(HydraLogger)
    , Profiler(profiler)
{
    YCHECK(DecoratedAutomaton_);
    YCHECK(EpochContext_);
    VERIFY_INVOKER_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(EpochContext_->EpochUserAutomatonInvoker, AutomatonThread);

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
        , LocalFlushResult_(NewPromise<TError>())
        , QuorumFlushResult_(NewPromise<TError>())
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

    void SetLocalFlushResult(TAsyncError result)
    {
        LocalFlushResult_ = std::move(result);
    }

    TAsyncError GetQuorumFlushResult()
    {
        return QuorumFlushResult_;
    }

    void Flush()
    {
        int mutationCount = static_cast<int>(BatchedRecordsData_.size());
        Logger.AddTag(Sprintf("MutationCount: %d", mutationCount));
        CommittedVersion_ = TVersion(StartVersion_.SegmentId, StartVersion_.RecordId + mutationCount);

        Owner_->Profiler.Enqueue("/commit_batch_size", mutationCount);

        Awaiter_ = New<TParallelAwaiter>(Owner_->EpochContext_->EpochControlInvoker);

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
                ToProto(request->mutable_epoch_id(), Owner_->EpochContext_->EpochId);
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

    void OnLocalFlush(TError error)
    {
        if (!error.IsOK()) {
            SetFailed(TError(
                NHydra::EErrorCode::MaybeCommitted,
                "Mutations are uncertain: local commit failed")
                << error);
            return;
        }

        LOG_DEBUG("Mutations are flushed locally");

        Owner_->Profiler.TimingCheckpoint(
            Timer_,
            Owner_->CellManager_->GetPeerTags(Owner_->CellManager_->GetSelfId()));

        ++FlushCount_;
        CheckQuorum();
    }

    void OnCompleted()
    {
        SetFailed(TError(
            NHydra::EErrorCode::MaybeCommitted,
            "Mutations are uncertain: %d out of %d commits were successful",
            FlushCount_,
            Owner_->CellManager_->GetQuorumCount()));
    }


    bool CheckQuorum()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (FlushCount_ < Owner_->CellManager_->GetQuorumCount())
            return false;

        SetSucceded();
        return true;
    }

    void SetSucceded()
    {
        LOG_DEBUG("Mutations are flushed by quorum");

        Owner_->Profiler.TimingCheckpoint(
            Timer_,
            Owner_->CellManager_->GetPeerQuorumTags());

        Awaiter_->Cancel();
        QuorumFlushResult_.Set(TError());
    }

    void SetFailed(const TError& error)
    {
        LOG_ERROR(error);

        Owner_->Profiler.TimingCheckpoint(
            Timer_,
            Owner_->CellManager_->GetPeerQuorumTags());

        Awaiter_->Cancel();
        QuorumFlushResult_.Set(error);
    }


    // NB: TBatch cannot outlive its owner.
    TLeaderCommitter* Owner_;
    TVersion StartVersion_;

    // Counting with the local flush.
    int FlushCount_ = 0;

    TParallelAwaiterPtr Awaiter_;
    TAsyncError LocalFlushResult_;
    TAsyncErrorPromise QuorumFlushResult_;
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
    TEpochContextPtr epochContext,
    const NProfiling::TProfiler& profiler)
    : TCommitter(
        cellManager,
        decoratedAutomaton,
        epochContext,
        profiler)
    , Config_(config)
    , ChangelogStore_(changelogStore)
{
    YCHECK(Config_);
    YCHECK(CellManager_);
    YCHECK(ChangelogStore_);
}

TLeaderCommitter::~TLeaderCommitter()
{ }

TFuture< TErrorOr<TMutationResponse> > TLeaderCommitter::Commit(const TMutationRequest& request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    NTracing::TNullTraceContextGuard guard;
    
    if (LoggingSuspended_) {
        TPendingMutation pendingMutation;
        pendingMutation.Request = request;
        pendingMutation.CommitPromise = NewPromise<TErrorOr<TMutationResponse>>();
        PendingMutations_.push(pendingMutation);

        return pendingMutation.CommitPromise;
    } else {
        auto version = DecoratedAutomaton_->GetLoggedVersion();

        TSharedRef recordData;
        TAsyncError logResult;
        auto commitResult = NewPromise<TErrorOr<TMutationResponse>>();
        DecoratedAutomaton_->LogMutationAtLeader(
            request,
            &recordData,
            &logResult,
            commitResult);

        AddToBatch(version, std::move(recordData), std::move(logResult));

        if (version.RecordId + 1 >= Config_->LeaderCommitter->MaxChangelogRecordCount ||
            DecoratedAutomaton_->GetLoggedDataSize() > Config_->LeaderCommitter->MaxChangelogDataSize)
        {
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

TAsyncError TLeaderCommitter::GetQuorumFlushResult()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TGuard<TSpinLock> guard(BatchSpinLock_);
    return CurrentBatch_ ? CurrentBatch_->GetQuorumFlushResult() : OKFuture;
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
        TAsyncError logResult;
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
    TAsyncError localResult)
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
            BIND(&TLeaderCommitter::OnBatchCommitted, MakeWeak(this), CurrentBatch_)
                .Via(EpochContext_->EpochUserAutomatonInvoker));

        YCHECK(!BatchTimeoutCookie_);
        BatchTimeoutCookie_ = TDelayedExecutor::Submit(
            BIND(&TLeaderCommitter::OnBatchTimeout, MakeWeak(this), CurrentBatch_)
                .Via(EpochContext_->EpochControlInvoker),
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

    if (!error.IsOK())
        return;

    DecoratedAutomaton_->CommitMutations(batch->GetCommittedVersion());
}

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContextPtr epochContext,
    const NProfiling::TProfiler& profiler)
    : TCommitter(
        cellManager,
        decoratedAutomaton,
        epochContext,
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
        .AsyncVia(EpochContext_->EpochUserAutomatonInvoker)
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

    TAsyncError logResult;
    for (const auto& recordData : recordsData) {
        DecoratedAutomaton_->LogMutationAtFollower(recordData, &logResult);
    }
    return logResult;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
