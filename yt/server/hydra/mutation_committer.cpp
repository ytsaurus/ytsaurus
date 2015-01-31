#include "stdafx.h"
#include "mutation_committer.h"
#include "private.h"
#include "config.h"
#include "decorated_automaton.h"
#include "serialize.h"
#include "mutation_context.h"
#include "changelog.h"

#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/periodic_executor.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>
#include <core/profiling/timing.h>

#include <core/tracing/trace_context.h>

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NYTree;
using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto AutoCheckpointCheckPeriod = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

TCommitterBase::TCommitterBase(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContext* epochContext,
    const NProfiling::TProfiler& profiler)
    : Config_(config)
    , CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , EpochContext_(epochContext)
    , CommitCounter_("/commit_rate")
    , BatchFlushCounter_("/batch_flush_rate")
    , Logger(HydraLogger)
    , Profiler(profiler)
{
    YCHECK(Config_);
    YCHECK(DecoratedAutomaton_);
    YCHECK(EpochContext_);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochUserAutomatonInvoker, AutomatonThread);

    Logger.AddTag("CellId: %v", CellManager_->GetCellId());
}

TCommitterBase::~TCommitterBase()
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
        , Logger(Owner_->Logger)
    {
        Logger.AddTag("StartVersion: %v", StartVersion_);
    }

    void AddMutation(const TSharedRef& recordData, TFuture<void> localFlushResult)
    {
        TVersion currentVersion(
            StartVersion_.SegmentId,
            StartVersion_.RecordId + BatchedRecordsData_.size());

        BatchedRecordsData_.push_back(recordData);
        LocalFlushResult_ = std::move(localFlushResult);

        LOG_DEBUG("Mutation is batched (Version: %v)", currentVersion);
    }

    TFuture<void> GetQuorumFlushResult()
    {
        return QuorumFlushResult_;
    }

    void Flush()
    {
        int mutationCount = static_cast<int>(BatchedRecordsData_.size());
        Logger.AddTag("MutationCount: %v", mutationCount);
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
                if (followerId == Owner_->CellManager_->GetSelfPeerId())
                    continue;

                auto channel = Owner_->CellManager_->GetPeerChannel(followerId);
                if (!channel)
                    continue;

                LOG_DEBUG("Sending mutations to follower %v", followerId);

                THydraServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(Owner_->Config_->ControlRpcTimeout);

                auto committedVersion = Owner_->DecoratedAutomaton_->GetAutomatonVersion();

                auto request = proxy.LogMutations();
                ToProto(request->mutable_epoch_id(), Owner_->EpochContext_->EpochId);
                request->set_start_revision(StartVersion_.ToRevision());
                request->set_committed_revision(committedVersion.ToRevision());
                request->Attachments() = BatchedRecordsData_;

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
    void OnRemoteFlush(TPeerId followerId, const THydraServiceProxy::TErrorOrRspLogMutationsPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        Owner_->Profiler.TimingCheckpoint(
            Timer_,
            Owner_->CellManager_->GetPeerTags(followerId));

        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error logging mutations at follower %v",
                followerId);
            return;
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->logged()) {
            LOG_DEBUG("Mutations are flushed by follower %v", followerId);

            ++FlushCount_;
            CheckQuorum();
        } else {
            LOG_DEBUG("Mutations are acknowledged by follower %v", followerId);
        }
    }

    void OnLocalFlush(const TError& error)
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
            Owner_->CellManager_->GetPeerTags(Owner_->CellManager_->GetSelfPeerId()));

        ++FlushCount_;
        CheckQuorum();
    }

    void OnCompleted()
    {
        SetFailed(TError(
            NHydra::EErrorCode::MaybeCommitted,
            "Mutations are uncertain: %v out of %v commits were successful",
            FlushCount_,
            Owner_->CellManager_->GetPeerCount()));
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
        Awaiter_.Reset();

        QuorumFlushResult_.Set(TError());
    }

    void SetFailed(const TError& error)
    {
        Owner_->Profiler.TimingCheckpoint(
            Timer_,
            Owner_->CellManager_->GetPeerQuorumTags());

        Awaiter_->Cancel();
        Awaiter_.Reset();

        QuorumFlushResult_.Set(error);

        Owner_->EpochContext_->EpochUserAutomatonInvoker->Invoke(BIND(
            &TLeaderCommitter::FireCommitFailed,
            MakeStrong(Owner_),
            error));
    }


    // NB: TBatch cannot outlive its owner.
    TLeaderCommitter* const Owner_;
    const TVersion StartVersion_;

    // Counting with the local flush.
    int FlushCount_ = 0;

    TParallelAwaiterPtr Awaiter_;
    TFuture<void> LocalFlushResult_;
    TPromise<void> QuorumFlushResult_ = NewPromise<void>();
    std::vector<TSharedRef> BatchedRecordsData_;
    TVersion CommittedVersion_;

    NLog::TLogger Logger;
    
    NProfiling::TTimer Timer_;

};

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    TEpochContext* epochContext,
    const NProfiling::TProfiler& profiler)
    : TCommitterBase(
        config,
        cellManager,
        decoratedAutomaton,
        epochContext,
        profiler)
    , ChangelogStore_(changelogStore)
{
    YCHECK(CellManager_);
    YCHECK(ChangelogStore_);

    AutoCheckpointCheckExecutor_ = New<TPeriodicExecutor>(
        EpochContext_->EpochUserAutomatonInvoker,
        BIND(&TLeaderCommitter::OnAutoCheckpointCheck, MakeWeak(this)),
        AutoCheckpointCheckPeriod);
    AutoCheckpointCheckExecutor_->Start();
}

TLeaderCommitter::~TLeaderCommitter()
{ }

TFuture<TMutationResponse> TLeaderCommitter::Commit(const TMutationRequest& request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    NTracing::TNullTraceContextGuard guard;
    
    if (LoggingSuspended_) {
        TPendingMutation pendingMutation;
        pendingMutation.Request = request;
        pendingMutation.Promise = NewPromise<TMutationResponse>();
        PendingMutations_.push_back(pendingMutation);
        return pendingMutation.Promise;
    }

    auto version = DecoratedAutomaton_->GetLoggedVersion();

    TSharedRef recordData;
    TFuture<void> localFlushResult;
    TFuture<TMutationResponse> commitResult;
    DecoratedAutomaton_->LogLeaderMutation(
        request,
        &recordData,
        &localFlushResult,
        &commitResult);

    AddToBatch(
        version,
        std::move(recordData),
        std::move(localFlushResult));

    if (version.RecordId + 1 >= Config_->MaxChangelogRecordCount ||
        DecoratedAutomaton_->GetLoggedDataSize() > Config_->MaxChangelogDataSize)
    {
        CheckpointNeeded_.Fire();
    }

    return commitResult;
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
    return CurrentBatch_
        ? CurrentBatch_->GetQuorumFlushResult()
        : PrevBatchQuorumFlushResult_;
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

    for (auto& pendingMutation : PendingMutations_) {
        auto version = DecoratedAutomaton_->GetLoggedVersion();

        TSharedRef recordData;
        TFuture<void> localFlushResult;
        TFuture<TMutationResponse> commitResult;
        DecoratedAutomaton_->LogLeaderMutation(
            pendingMutation.Request,
            &recordData,
            &localFlushResult,
            &commitResult);

        AddToBatch(version, recordData, std::move(localFlushResult));
        pendingMutation.Promise.SetFrom(std::move(commitResult));
    }

    PendingMutations_.clear();
    LoggingSuspended_ = false;
}

void TLeaderCommitter::AddToBatch(
    TVersion version,
    const TSharedRef& recordData,
    TFuture<void> localFlushResult)
{
    TGuard<TSpinLock> guard(BatchSpinLock_);
    auto batch = GetOrCreateBatch(version);
    batch->AddMutation(recordData, std::move(localFlushResult));
    if (batch->GetMutationCount() >= Config_->MaxCommitBatchRecordCount) {
        FlushCurrentBatch();
    }
}

void TLeaderCommitter::FlushCurrentBatch()
{
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock_);
    YCHECK(CurrentBatch_);

    CurrentBatch_->Flush();
    PrevBatchQuorumFlushResult_ = CurrentBatch_->GetQuorumFlushResult();

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
            Config_->MaxCommitBatchDelay);
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

void TLeaderCommitter::OnBatchCommitted(TBatchPtr batch, const TError& error)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!error.IsOK())
        return;

    DecoratedAutomaton_->CommitMutations(batch->GetCommittedVersion());
}

void TLeaderCommitter::OnAutoCheckpointCheck()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (TInstant::Now() > DecoratedAutomaton_->GetLastSnapshotTime() + Config_->SnapshotBuildPeriod) {
        CheckpointNeeded_.Fire();
    }
}

void TLeaderCommitter::FireCommitFailed(const TError& error)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    CommitFailed_.Fire(error);
}

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContext* epochContext,
    const NProfiling::TProfiler& profiler)
    : TCommitterBase(
        config,
        cellManager,
        decoratedAutomaton,
        epochContext,
        profiler)
{ }

TFollowerCommitter::~TFollowerCommitter()
{ }

TFuture<void> TFollowerCommitter::LogMutations(
    TVersion expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (LoggingSuspended_) {
        TPendingMutation pendingMutation;
        pendingMutation.RecordsData = recordsData;
        pendingMutation.ExpectedVersion = expectedVersion;
        pendingMutation.Promise = NewPromise<void>();
        PendingMutations_.push_back(pendingMutation);
        return pendingMutation.Promise;
    }

    return DoLogMutations(expectedVersion, recordsData);
}

TFuture<void> TFollowerCommitter::DoLogMutations(
    TVersion expectedVersion,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto currentVersion = DecoratedAutomaton_->GetLoggedVersion();
    if (currentVersion != expectedVersion) {
        return MakeFuture(TError(
            NHydra::EErrorCode::OutOfOrderMutations,
            "Out-of-order mutations received by follower: expected %v, actual %v",
            expectedVersion,
            currentVersion));
    }

    auto result = VoidFuture;
    int recordsCount = static_cast<int>(recordsData.size());
    for (int index = 0; index < recordsCount; ++index) {
        DecoratedAutomaton_->LogFollowerMutation(
            recordsData[index],
            index == recordsCount - 1 ? &result : nullptr);
    }

    Profiler.Increment(CommitCounter_, recordsCount);
    Profiler.Increment(BatchFlushCounter_);

    return result;
}

bool TFollowerCommitter::IsLoggingSuspended() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return LoggingSuspended_;
}

void TFollowerCommitter::SuspendLogging()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(!LoggingSuspended_);

    LOG_DEBUG("Mutations logging suspended");

    LoggingSuspended_ = true;
    YCHECK(PendingMutations_.empty());
}

void TFollowerCommitter::ResumeLogging()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(LoggingSuspended_);

    LOG_DEBUG("Mutations logging resumed");

    for (auto& pendingMutation : PendingMutations_) {
        auto result = DoLogMutations(pendingMutation.ExpectedVersion, pendingMutation.RecordsData);
        pendingMutation.Promise.SetFrom(std::move(result));
    }

    PendingMutations_.clear();
    LoggingSuspended_ = false;
}

TFuture<TMutationResponse> TFollowerCommitter::Forward(const TMutationRequest& request)
{
    auto channel = CellManager_->GetPeerChannel(EpochContext_->LeaderId);
    YCHECK(channel);

    THydraServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config_->CommitForwardingRpcTimeout);

    auto req = proxy.CommitMutation();
    req->set_type(request.Type);
    req->Attachments().push_back(request.Data);

    return req->Invoke().Apply(BIND([] (const THydraServiceProxy::TErrorOrRspCommitMutationPtr& rspOrError) {
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error forwarding mutation to leader");
        const auto& rsp = rspOrError.Value();
        return TMutationResponse(TSharedRefArray(rsp->Attachments()));
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
