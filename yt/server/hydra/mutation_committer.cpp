#include "mutation_committer.h"
#include "private.h"
#include "changelog.h"
#include "config.h"
#include "decorated_automaton.h"
#include "checkpointer.h"
#include "mutation_context.h"
#include "serialize.h"

#include <yt/ytlib/election/cell_manager.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/rpc/response_keeper.h>

#include <utility>

namespace NYT::NHydra {

using namespace NElection;
using namespace NYTree;
using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto AutoSnapshotCheckPeriod = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

TCommitterBase::TCommitterBase(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContext* epochContext)
    : Config_(std::move(config))
    , Options_(options)
    , CellManager_(std::move(cellManager))
    , DecoratedAutomaton_(std::move(decoratedAutomaton))
    , EpochContext_(epochContext)
    , Logger(NLogging::TLogger(HydraLogger)
        .AddTag("CellId: %v", CellManager_->GetCellId()))
    , Profiler(HydraProfiler.AddTags(Options_.ProfilingTagIds))
{
    YCHECK(Config_);
    YCHECK(DecoratedAutomaton_);
    YCHECK(EpochContext_);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochUserAutomatonInvoker, AutomatonThread);
}

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
        , Logger(NLogging::TLogger(owner->Logger))
    { }

    void AddMutation(
        const TMutationRequest& request,
        TSharedRef recordData,
        TFuture<void> localFlushResult)
    {
        auto currentVersion = GetStartVersion().Advance(GetMutationCount());

        BatchedRecordsData_.push_back(std::move(recordData));
        LocalFlushResult_ = std::move(localFlushResult);

        LOG_DEBUG("Mutation batched (Version: %v, StartVersion: %v, MutationType: %v, MutationId: %v)",
            currentVersion,
            GetStartVersion(),
            request.Type,
            request.MutationId);
    }

    TFuture<void> GetQuorumFlushResult()
    {
        return QuorumFlushResult_;
    }

    void Flush()
    {
        auto owner = Owner_.Lock();
        if (!owner) {
            return;
        }

        int mutationCount = GetMutationCount();
        CommittedVersion_ = GetStartVersion().Advance(mutationCount);

        LOG_DEBUG("Flushing batched mutations (StartVersion: %v, MutationCount: %v)",
            GetStartVersion(),
            mutationCount);

        owner->Profiler.Enqueue("/commit_batch_size", mutationCount, EMetricType::Gauge);

        std::vector<TFuture<void>> asyncResults;

        Timer_ = owner->Profiler.TimingStart(
            "/changelog_flush_time",
            EmptyTagIds,
            ETimerMode::Parallel);

        if (!BatchedRecordsData_.empty()) {
            YCHECK(LocalFlushResult_);
            asyncResults.push_back(LocalFlushResult_.Apply(
                BIND(&TBatch::OnLocalFlush, MakeStrong(this))
                    .AsyncVia(owner->EpochContext_->EpochControlInvoker)));

            for (auto followerId = 0; followerId < owner->CellManager_->GetTotalPeerCount(); ++followerId) {
                if (followerId == owner->CellManager_->GetSelfPeerId()) {
                    continue;
                }

                auto channel = owner->CellManager_->GetPeerChannel(followerId);
                if (!channel) {
                    continue;
                }

                LOG_DEBUG("Sending mutations to follower (PeerId: %v, StartVersion: %v, MutationCount: %v)",
                    followerId,
                    GetStartVersion(),
                    GetMutationCount());

                THydraServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(owner->Config_->CommitFlushRpcTimeout);

                auto committedVersion = owner->DecoratedAutomaton_->GetCommittedVersion();

                auto request = proxy.AcceptMutations();
                ToProto(request->mutable_epoch_id(), owner->EpochContext_->EpochId);
                request->set_start_revision(GetStartVersion().ToRevision());
                request->set_committed_revision(committedVersion.ToRevision());
                request->Attachments() = BatchedRecordsData_;

                asyncResults.push_back(request->Invoke().Apply(
                    BIND(&TBatch::OnRemoteFlush, MakeStrong(this), followerId)
                        .AsyncVia(owner->EpochContext_->EpochControlInvoker)));
            }
        }

        Combine(asyncResults).Subscribe(
            BIND(&TBatch::OnCompleted, MakeStrong(this))
                .Via(owner->EpochContext_->EpochControlInvoker));
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
    const TWeakPtr<TLeaderCommitter> Owner_;
    const TVersion StartVersion_;

    const NLogging::TLogger Logger;

    // Counting with the local flush.
    int FlushCount_ = 0;

    TFuture<void> LocalFlushResult_;
    TPromise<void> QuorumFlushResult_ = NewPromise<void>();
    std::vector<TSharedRef> BatchedRecordsData_;
    TVersion CommittedVersion_;

    TTimer Timer_;


    void OnRemoteFlush(TPeerId followerId, const THydraServiceProxy::TErrorOrRspAcceptMutationsPtr& rspOrError)
    {
        auto owner = Owner_.Lock();
        if (!owner) {
            return;
        }

        VERIFY_THREAD_AFFINITY(owner->ControlThread);

        auto time = owner->Profiler.TimingCheckpoint(
            Timer_,
            {owner->CellManager_->GetPeerTag(followerId)});

        if (!rspOrError.IsOK()) {
            LOG_DEBUG(rspOrError, "Error logging mutations at follower (PeerId: %v, StartVersion: %v, MutationCount: %v)",
                followerId,
                GetStartVersion(),
                GetMutationCount());
            return;
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->logged()) {
            LOG_DEBUG("Mutations are logged by follower (PeerId: %v, StartVersion: %v, MutationCount: %v, WallTime: %v)",
                followerId,
                GetStartVersion(),
                GetMutationCount(),
                time);
            OnSuccessfulFlush(owner);
        } else {
            LOG_DEBUG("Mutations are acknowledged by follower (PeerId: %v, StartVersion: %v, MutationCount: %v, WallTime: %v)",
                followerId,
                GetStartVersion(),
                GetMutationCount(),
                time);
        }
    }

    void OnLocalFlush(const TError& error)
    {
        auto owner = Owner_.Lock();
        if (!owner) {
            return;
        }

        VERIFY_THREAD_AFFINITY(owner->ControlThread);

        if (!error.IsOK()) {
            SetFailed(
                owner,
                TError(
                    NRpc::EErrorCode::Unavailable,
                    "Mutations are uncertain: local commit failed")
                    << error);
            return;
        }

        auto time = owner->Profiler.TimingCheckpoint(
            Timer_,
            {owner->CellManager_->GetPeerTag(owner->CellManager_->GetSelfPeerId())});

        LOG_DEBUG("Mutations are flushed locally (StartVersion: %v, MutationCount: %v, WallTime: %v)",
            GetStartVersion(),
            GetMutationCount(),
            time);

        OnSuccessfulFlush(owner);
    }

    void OnCompleted(const TError&)
    {
        auto owner = Owner_.Lock();
        if (!owner) {
            return;
        }

        VERIFY_THREAD_AFFINITY(owner->ControlThread);

        SetFailed(
            owner,
            TError(
                NRpc::EErrorCode::Unavailable,
                "Mutations are uncertain: %v out of %v commits were successful",
                FlushCount_,
                owner->CellManager_->GetTotalPeerCount()));
    }


    void OnSuccessfulFlush(const TLeaderCommitterPtr& owner)
    {
        VERIFY_THREAD_AFFINITY(owner->ControlThread);

        ++FlushCount_;
        if (FlushCount_ == owner->CellManager_->GetQuorumPeerCount()) {
            SetSucceeded(owner);
        }
    }

    void SetSucceeded(const TLeaderCommitterPtr& owner)
    {
        if (QuorumFlushResult_.IsSet()) {
            return;
        }

        auto time = owner->Profiler.TimingCheckpoint(
            Timer_,
            {owner->CellManager_->GetPeerQuorumTag()});

        LOG_DEBUG("Mutations are flushed by quorum (StartVersion: %v, MutationCount: %v, WallTime: %v)",
            GetStartVersion(),
            GetMutationCount(),
            time);

        QuorumFlushResult_.Set(TError());
    }

    void SetFailed(const TLeaderCommitterPtr& owner, const TError& error)
    {
        if (QuorumFlushResult_.IsSet()) {
            return;
        }

        QuorumFlushResult_.Set(error);

        owner->EpochContext_->EpochUserAutomatonInvoker->Invoke(BIND(
            &TLeaderCommitter::FireCommitFailed,
            owner,
            error));
    }
};

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    TEpochContext* epochContext)
    : TCommitterBase(
        config,
        options,
        cellManager,
        decoratedAutomaton,
        epochContext)
    , ChangelogStore_(changelogStore)
    , AutoSnapshotCheckExecutor_(New<TPeriodicExecutor>(
        EpochContext_->EpochUserAutomatonInvoker,
        BIND(&TLeaderCommitter::OnAutoSnapshotCheck, MakeWeak(this)),
        AutoSnapshotCheckPeriod))
{
    YCHECK(CellManager_);
    YCHECK(ChangelogStore_);

    AutoSnapshotCheckExecutor_->Start();
}

TFuture<TMutationResponse> TLeaderCommitter::Commit(TMutationRequest&& request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto keptResponse = DecoratedAutomaton_->TryBeginKeptRequest(request);
    if (keptResponse) {
        return keptResponse;
    }

    NTracing::TNullTraceContextGuard guard;
    auto commitStartTime = GetInstant();

    if (LoggingSuspended_) {
        PendingMutations_.emplace_back(std::move(request), commitStartTime);
        return PendingMutations_.back().CommitPromise;
    }

    auto version = DecoratedAutomaton_->GetLoggedVersion();

    TSharedRef recordData;
    TFuture<void> localFlushResult;
    TFuture<TMutationResponse> commitResult;
    const auto& loggedRequest = DecoratedAutomaton_->LogLeaderMutation(
        commitStartTime,
        std::move(request),
        &recordData,
        &localFlushResult,
        &commitResult);

    AddToBatch(
        version,
        loggedRequest,
        std::move(recordData),
        std::move(localFlushResult));

    if (DecoratedAutomaton_->GetRecordCountSinceLastCheckpoint() >= Config_->MaxChangelogRecordCount) {
        LOG_INFO("Requesting checkpoint due to record count limit (RecordCountSinceLastCheckpoint: %v, MaxChangelogRecordCount: %v)",
            DecoratedAutomaton_->GetRecordCountSinceLastCheckpoint(),
            Config_->MaxChangelogRecordCount);
        CheckpointNeeded_.Fire(false);
    } else if (DecoratedAutomaton_->GetDataSizeSinceLastCheckpoint() >= Config_->MaxChangelogDataSize)  {
        LOG_INFO("Requesting checkpoint due to data size limit (DataSizeSinceLastCheckpoint: %v, MaxChangelogDataSize: %v)",
            DecoratedAutomaton_->GetDataSizeSinceLastCheckpoint(),
            Config_->MaxChangelogDataSize);
        CheckpointNeeded_.Fire(false);
    }

    return commitResult;
}

void TLeaderCommitter::Flush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto guard = Guard(BatchSpinLock_);
    if (CurrentBatch_) {
        FlushCurrentBatch();
    }
}

TFuture<void> TLeaderCommitter::GetQuorumFlushResult()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto guard = Guard(BatchSpinLock_);
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
        const auto& loggedMutation = DecoratedAutomaton_->LogLeaderMutation(
            pendingMutation.Timestamp,
            std::move(pendingMutation.Request),
            &recordData,
            &localFlushResult,
            &commitResult);

        AddToBatch(
            version,
            loggedMutation,
            std::move(recordData),
            std::move(localFlushResult));

        pendingMutation.CommitPromise.SetFrom(std::move(commitResult));
    }

    PendingMutations_.clear();
    LoggingSuspended_ = false;
}

void TLeaderCommitter::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
    for (auto& mutation : PendingMutations_) {
        mutation.CommitPromise.Set(error);
    }
}

void TLeaderCommitter::AddToBatch(
    TVersion version,
    const TMutationRequest& request,
    TSharedRef recordData,
    TFuture<void> localFlushResult)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto guard = Guard(BatchSpinLock_);
    auto batch = GetOrCreateBatch(version);
    batch->AddMutation(
        request,
        std::move(recordData),
        std::move(localFlushResult));
    if (batch->GetMutationCount() >= Config_->MaxCommitBatchRecordCount) {
        FlushCurrentBatch();
    }
}

void TLeaderCommitter::FlushCurrentBatch()
{
    VERIFY_SPINLOCK_AFFINITY(BatchSpinLock_);

    TBatchPtr currentBatch;
    std::swap(currentBatch, CurrentBatch_);
    PrevBatchQuorumFlushResult_ = currentBatch->GetQuorumFlushResult();
    TDelayedExecutor::CancelAndClear(BatchTimeoutCookie_);

    currentBatch->Flush();
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

void TLeaderCommitter::OnBatchTimeout(const TBatchPtr& batch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto guard = Guard(BatchSpinLock_);
    if (batch == CurrentBatch_) {
        FlushCurrentBatch();
    }
}

void TLeaderCommitter::OnBatchCommitted(const TBatchPtr& batch, const TError& error)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!error.IsOK()) {
        return;
    }

    DecoratedAutomaton_->CommitMutations(batch->GetCommittedVersion(), true);
}

void TLeaderCommitter::OnAutoSnapshotCheck()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (DecoratedAutomaton_->GetLastSnapshotTime() != TInstant::Zero() &&
        TInstant::Now() > DecoratedAutomaton_->GetLastSnapshotTime() + Config_->SnapshotBuildPeriod)
    {
        LOG_INFO("Requesting periodic snapshot (LastSnapshotTime: %v, SnapshotBuildPeriod: %v)",
            DecoratedAutomaton_->GetLastSnapshotTime(),
            Config_->SnapshotBuildPeriod);
        CheckpointNeeded_.Fire(true);
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
    const TDistributedHydraManagerOptions& options,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContext* epochContext)
    : TCommitterBase(
        config,
        options,
        cellManager,
        decoratedAutomaton,
        epochContext)
{ }

TFuture<void> TFollowerCommitter::AcceptMutations(
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

    return DoAcceptMutations(expectedVersion, recordsData);
}

TFuture<void> TFollowerCommitter::DoAcceptMutations(
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
        auto result = DoAcceptMutations(pendingMutation.ExpectedVersion, pendingMutation.RecordsData);
        pendingMutation.Promise.SetFrom(std::move(result));
    }

    PendingMutations_.clear();
    LoggingSuspended_ = false;
}

TFuture<TMutationResponse> TFollowerCommitter::Forward(TMutationRequest&& request)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto channel = CellManager_->GetPeerChannel(EpochContext_->LeaderId);
    YCHECK(channel);

    THydraServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config_->CommitForwardingRpcTimeout);

    auto req = proxy.CommitMutation();
    req->set_type(request.Type);
    if (request.MutationId) {
        ToProto(req->mutable_mutation_id(), request.MutationId);
        req->set_retry(request.Retry);
    }
    req->Attachments().push_back(request.Data);

    return req->Invoke().Apply(BIND([] (const THydraServiceProxy::TErrorOrRspCommitMutationPtr& rspOrError) {
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error forwarding mutation to leader");
        const auto& rsp = rspOrError.Value();
        return TMutationResponse{TSharedRefArray(rsp->Attachments())};
    }));
}

void TFollowerCommitter::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
    for (auto& pendingMutation : PendingMutations_) {
        pendingMutation.Promise.Set(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
