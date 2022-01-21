#include "mutation_committer.h"
#include "private.h"
#include "decorated_automaton.h"
#include "changelog_acquisition.h"
#include "lease_tracker.h"

#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/mutation_context.h>
#include <yt/yt/server/lib/hydra_common/serialize.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/profiling/profiler.h>
#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <utility>

namespace NYT::NHydra2 {

using namespace NElection;
using namespace NYTree;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NTracing;
using namespace NRpc;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TCommitterBase::TCommitterBase(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContext* epochContext,
    NLogging::TLogger logger,
    NProfiling::TProfiler /*profiler*/)
    : Config_(std::move(config))
    , Options_(options)
    , DecoratedAutomaton_(std::move(decoratedAutomaton))
    , EpochContext_(epochContext)
    , Logger(std::move(logger))
    , CellManager_(EpochContext_->CellManager)
{
    YT_VERIFY(Config_);
    YT_VERIFY(DecoratedAutomaton_);
    YT_VERIFY(EpochContext_);

    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochUserAutomatonInvoker, AutomatonThread);
}

TFuture<void> TCommitterBase::DoCommitMutations(std::vector<TPendingMutationPtr> mutations)
{
    return BIND(&TDecoratedAutomaton::ApplyMutations, DecoratedAutomaton_)
        .AsyncViaGuarded(
            EpochContext_->EpochUserAutomatonInvoker,
            TError("meh"))
        .Run(std::move(mutations), EpochContext_->Term);
}

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TLeaderLeasePtr leaderLease,
    TMpscQueue<TMutationDraft>* queue,
    int changelogId,
    IChangelogPtr changelog,
    TReachableState reachableState,
    TEpochContext* epochContext,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : TCommitterBase(
        std::move(config),
        options,
        std::move(decoratedAutomaton),
        epochContext,
        std::move(logger),
        profiler)
    , BatchAlarm_(New<TInvokerAlarm>(
        EpochContext_->EpochUserAutomatonInvoker))
    , LeaderLease_(std::move(leaderLease))
    , AcceptMutationsExecutor_(New<TPeriodicExecutor>(
        EpochContext_->EpochControlInvoker,
        BIND(&TLeaderCommitter::Flush, MakeWeak(this)),
        Config_->MaxCommitBatchDuration))
    , SerializeMutationsExecutor_(New<TPeriodicExecutor>(
        EpochContext_->EpochControlInvoker,
        BIND(&TLeaderCommitter::SerializeMutations, MakeWeak(this)),
        Config_->MaxCommitBatchDuration))
    , CommittedState_(std::move(reachableState))
    , PreliminaryMutationQueue_(queue)
    , BatchSummarySize_(profiler.Summary("/mutation_batch_size"))
{
    PeerStates_.assign(CellManager_->GetTotalPeerCount(), {-1, -1});

    ChangelogId_ = changelogId;
    Changelog_ = std::move(changelog);

    auto selfId = CellManager_->GetSelfPeerId();
    PeerStates_[selfId].NextExpectedSequenceNumber = CommittedState_.SequenceNumber + 1;
    PeerStates_[selfId].LastLoggedSequenceNumber = CommittedState_.SequenceNumber;

    LastOffloadedSequenceNumber_ = CommittedState_.SequenceNumber;

    AcceptMutationsExecutor_->Start();
}

TLeaderCommitter::~TLeaderCommitter()
{ }

void TLeaderCommitter::SetReadOnly()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReadOnly_ = true;
}

void TLeaderCommitter::SerializeMutations()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_LOG_DEBUG("Started serializing mutations");

    if (!LeaderLease_->IsValid() || EpochContext_->LeaderLeaseExpired) {
        auto error = TError(
            NRpc::EErrorCode::Unavailable,
            "Leader lease is no longer valid");
        // Ensure monotonicity: once Hydra rejected a mutation, no more mutations are accepted.
        EpochContext_->LeaderLeaseExpired = true;
        LoggingFailed_.Fire(TError(
            NRpc::EErrorCode::Unavailable,
            "Leader lease is no longer valid"));
        return;
    }

    if (EpochContext_->LeaderSwitchStarted) {
        // This check is also monotonic (see above).
        YT_LOG_INFO("Cannot serialize mutation while leader switch is in progress");
        return;
    }

    // meh
    NTracing::TNullTraceContextGuard traceContextGuard;

    TMutationDraft mutationDraft;
    while (PreliminaryMutationQueue_->TryDequeue(&mutationDraft)) {
        if (ReadOnly_) {
            auto error = TError(
                EErrorCode::ReadOnly,
                "Read-only mode is active");
            mutationDraft.Promise.Set(TError(
                NRpc::EErrorCode::Unavailable,
                "Cannot commit a mutation at the moment")
                << error);
            continue;
        }

        auto epochId = mutationDraft.Request.EpochId;
        auto currentEpochId = *CurrentEpochId;
        if (epochId && epochId != currentEpochId) {
            mutationDraft.Promise.Set(TError(
                NRpc::EErrorCode::Unavailable,
                "Mutation has invalid epoch id %v in epoch %v",
                epochId,
                currentEpochId));
            continue;
        }
        LogMutation(std::move(mutationDraft));
    }

    MaybeSendBatch();
}

void TLeaderCommitter::Start(int changelogId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LastRandomSeed_ = DecoratedAutomaton_->GetRandomSeed();
    YT_VERIFY(changelogId == ChangelogId_);
    LoggedVersion_ = {changelogId, 0};

    auto sequenceNumber = DecoratedAutomaton_->GetSequenceNumber();
    YT_VERIFY(CommittedState_.SequenceNumber == sequenceNumber);

    YT_LOG_INFO("Leader committer started (LastRandomSeed: %llx, LoggedVersion: %v)",
        LastRandomSeed_,
        LoggedVersion_);

    SerializeMutationsExecutor_->Start();
}

void TLeaderCommitter::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
    for (const auto& mutation : MutationQueue_) {
        if (!mutation->LocalCommitPromise.IsSet()) {
            mutation->LocalCommitPromise.Set(error);
        }
    }

    MutationQueue_.clear();
    MutationQueueDataSize_ = 0;

    LastSnapshotInfo_ = std::nullopt;
    PeerStates_.clear();
}

void TLeaderCommitter::Flush()
{
    YT_LOG_DEBUG("Started flushing mutations");

    for (auto followerId = 0; followerId < CellManager_->GetTotalPeerCount(); ++followerId) {
        if (followerId == CellManager_->GetSelfPeerId()) {
            continue;
        }

        auto channel = CellManager_->GetPeerChannel(followerId);
        if (!channel) {
            continue;
        }

        auto followerState = PeerStates_[followerId];
        if (!MutationQueue_.empty() && followerState.NextExpectedSequenceNumber < MutationQueue_.front()->SequenceNumber) {
            if (followerState.NextExpectedSequenceNumber == -1) {
                // This is ok, it actually means that follower hasn't received initial ping (and hasn't recovered) yet,
                // Lets just wait for him to recover.

                // Something usefull might or might not happen here.
            } else {
                TError error("Follower %v needs a mutation %v that was already lost",
                    followerId,
                    followerState.NextExpectedSequenceNumber);

                YT_LOG_ERROR(error, "Requesting follower restart (FollowerId: %v)",
                    followerId);

                THydraServiceProxy proxy(channel);
                auto req = proxy.ForceRestart();
                ToProto(req->mutable_reason(), error);

                req->Invoke();
                continue;
            }
        }

        TInternalHydraServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->CommitFlushRpcTimeout);

        auto getMutationCount = [&] () -> i64 {
            if (MutationQueue_.empty() || followerState.NextExpectedSequenceNumber == -1) {
                return 0;
            }
            return std::min<i64>(
                Config_->MaxCommitBatchRecordCount,
                MutationQueue_.back()->SequenceNumber - followerState.NextExpectedSequenceNumber + 1);
        };

        auto mutationCount = getMutationCount();

        auto request = proxy.AcceptMutations();
        ToProto(request->mutable_epoch_id(), EpochContext_->EpochId);
        request->set_start_sequence_number(followerState.NextExpectedSequenceNumber);
        request->set_committed_sequence_number(CommittedState_.SequenceNumber);
        request->set_committed_segment_id(CommittedState_.SegmentId);
        request->set_term(EpochContext_->Term);

        if (LastSnapshotInfo_ && LastSnapshotInfo_->SequenceNumber != -1) {
            auto* snapshotRequest = request->mutable_snapshot_request();
            snapshotRequest->set_snapshot_id(LastSnapshotInfo_->SnapshotId);
            snapshotRequest->set_sequence_number(LastSnapshotInfo_->SequenceNumber);
        }

        YT_LOG_DEBUG("Sending mutations to follower (PeerId: %v, NextExpectedSequenceNumber: %v, MutationCount: %v, CommittedState: %v)",
            followerId,
            followerState.NextExpectedSequenceNumber,
            mutationCount,
            CommittedState_);

        BatchSummarySize_.Record(mutationCount);

        if (mutationCount > 0) {
            auto startIndex = followerState.NextExpectedSequenceNumber - MutationQueue_.front()->SequenceNumber;
            for (int i = startIndex; i < startIndex + mutationCount; ++i) {
                YT_VERIFY(i < std::ssize(MutationQueue_));
                const auto& mutation = MutationQueue_[i];
                request->Attachments().push_back(mutation->SerializedMutation);
            }
        }

        request->Invoke().Apply(
            BIND(&TLeaderCommitter::OnRemoteFlush, MakeStrong(this), followerId)
                .AsyncVia(EpochContext_->EpochControlInvoker));
    }
}

void TLeaderCommitter::OnSnapshotReply(int peerId)
{
    if (LastSnapshotInfo_->HasReply[peerId]) {
        return;
    }

    YT_LOG_INFO("Received a new snapshot reply (PeerId: %v, SnaphotId: %v)",
        peerId,
        LastSnapshotInfo_->SnapshotId);

    LastSnapshotInfo_->HasReply[peerId] = true;
    ++LastSnapshotInfo_->ReplyCount;
    if (LastSnapshotInfo_->ReplyCount == std::ssize(LastSnapshotInfo_->HasReply)) {
        OnSnapshotsComplete();
    }
}

void TLeaderCommitter::OnRemoteFlush(
    int followerId,
    const TInternalHydraServiceProxy::TErrorOrRspAcceptMutationsPtr& rspOrError)
{
    if (!rspOrError.IsOK()) {
        YT_LOG_INFO(rspOrError, "Error logging mutations at follower (FollowerId: %v)",
            followerId);

        // TODO: This might be an old reply.
        if (LastSnapshotInfo_ && LastSnapshotInfo_->SequenceNumber != -1) {
            OnSnapshotReply(followerId);
        }
    }

    auto& peerState = PeerStates_[followerId];

    const auto& rsp = rspOrError.ValueOrThrow();

    if (rsp->has_snapshot_response()) {
        const auto& snapshotResult = rsp->snapshot_response();

        auto snapshotId = snapshotResult.snapshot_id();
        auto checksum = snapshotResult.checksum();

        YT_LOG_DEBUG("Snaphot reply received (SnapshotId: %v, FollowerId: %v)",
            snapshotId,
            followerId);

        // We could have received an unsuccessfull reply before, so we can mark it as success now, but we
        // won't count it again (because of HasReply).
        if (LastSnapshotInfo_ && LastSnapshotInfo_->SnapshotId == snapshotId && !LastSnapshotInfo_->Checksums[followerId]) {
            LastSnapshotInfo_->Checksums[followerId] = checksum;
            OnSnapshotReply(followerId);
        }
    }

    auto loggedSequenceNumber = rsp->logged_sequence_number();
    YT_VERIFY(peerState.LastLoggedSequenceNumber <= loggedSequenceNumber);
    peerState.LastLoggedSequenceNumber = loggedSequenceNumber;

    auto nextExpectedSequenceNumber = rsp->expected_sequence_number();
    // Rollback here seems possible and ok?
    peerState.NextExpectedSequenceNumber = nextExpectedSequenceNumber;

    YT_LOG_DEBUG("Mutations are flushed by follower (FollowerId: %v, NextExpectedSequenceNumber: %v, LoggedSequenceNumber: %v)",
        followerId,
        nextExpectedSequenceNumber,
        loggedSequenceNumber);

    MaybePromoteCommitedSequenceNumber();
}

void TLeaderCommitter::MaybePromoteCommitedSequenceNumber()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<i64> loggedNumbers;
    for (int i = 0; i < CellManager_->GetTotalPeerCount(); ++i) {
        auto voting = CellManager_->GetPeerConfig(i).Voting;
        if (voting) {
            loggedNumbers.push_back(PeerStates_[i].LastLoggedSequenceNumber);
        }
    }
    YT_VERIFY(std::ssize(loggedNumbers) == CellManager_->GetVotingPeerCount());

    std::sort(loggedNumbers.begin(), loggedNumbers.end(), std::greater<>());

    auto committedSequenceNumber = loggedNumbers[CellManager_->GetQuorumPeerCount() - 1];

    YT_LOG_DEBUG("Trying to promote committed sequence number (NewCommittedSequenceNumber: %v)", committedSequenceNumber);
    if (committedSequenceNumber == -1 || CommittedState_.SequenceNumber == committedSequenceNumber) {
        return;
    }

    YT_VERIFY(!MutationQueue_.empty());
    auto start = MutationQueue_.front()->SequenceNumber;
    YT_VERIFY(committedSequenceNumber >= start);
    auto index = committedSequenceNumber - start;
    YT_VERIFY(index < std::ssize(MutationQueue_));
    auto segmentId = MutationQueue_[index]->Version.SegmentId;

    YT_VERIFY(committedSequenceNumber >= CommittedState_.SequenceNumber);

    TReachableState committedState(segmentId, committedSequenceNumber);
    YT_LOG_DEBUG("Committed sequence number promoted (Previous: %v, Current: %v)",
        CommittedState_,
        committedState);
    CommittedState_ = committedState;

    OnCommittedSequenceNumberUpdated();
}

void TLeaderCommitter::MaybeSendBatch()
{
    if (MutationQueue_.empty()) {
        return;
    }

    // TODO(aleksandra-zh): Some peers may have larger batches. Consider looking at each separately.
    auto batchSize = MutationQueue_.back()->SequenceNumber - CommittedState_.SequenceNumber;
    if (batchSize >= Config_->MaxCommitBatchRecordCount) {
        Flush();
    }

    DrainQueue();
}

void TLeaderCommitter::DrainQueue()
{
    auto popMutationQueue = [&] () {
        const auto& mutation = MutationQueue_.front();
        MutationQueueDataSize_ += sizeof(mutation) + mutation->SerializedMutation.Size();
        MutationQueue_.pop_front();
    };

    while (std::ssize(MutationQueue_) > Config_->MaxQueueMutationCount) {
        const auto& mutation = MutationQueue_.front();
        if (mutation->SequenceNumber > CommittedState_.SequenceNumber) {
            LoggingFailed_.Fire(TError("Mutation queue mutation count limit exceeded, but the first mutation in queue is still uncommitted")
                << TErrorAttribute("mutation_count", MutationQueue_.size())
                << TErrorAttribute("mutation_sequence_number", mutation->SequenceNumber));
        }
        popMutationQueue();
    }

    while (MutationQueueDataSize_ > Config_->MaxQueueMutationDataSize) {
        const auto& mutation = MutationQueue_.front();
        if (mutation->SequenceNumber > CommittedState_.SequenceNumber) {
            LoggingFailed_.Fire(TError("Mutation queue data size limit exceeded, but the first mutation in queue is still uncommitted")
                << TErrorAttribute("queue_data_size", MutationQueueDataSize_)
                << TErrorAttribute("mutation_sequence_number", mutation->SequenceNumber));
        }
        popMutationQueue();
    }

    auto it = std::min_element(PeerStates_.begin(), PeerStates_.end(), [] (const TPeerState& lhs, const TPeerState& rhs) {
        return lhs.LastLoggedSequenceNumber < rhs.LastLoggedSequenceNumber;
    });
    auto minLoggedSequenceNumber = it->LastLoggedSequenceNumber;

    while (!MutationQueue_.empty() && MutationQueue_.front()->SequenceNumber < minLoggedSequenceNumber) {
        popMutationQueue();
    }
}

void TLeaderCommitter::MaybeCheckpoint()
{
    if (AqcuiringChangelog_ || LastSnapshotInfo_) {
        return;
    }

    if (LoggedVersion_.RecordId >= Config_->MaxChangelogRecordCount) {
        YT_LOG_INFO("Requesting checkpoint due to record count limit (RecordCountSinceLastCheckpoint: %v, MaxChangelogRecordCount: %v)",
            LoggedVersion_.RecordId,
            Config_->MaxChangelogRecordCount);
    } else if (Changelog_->GetDataSize() >= Config_->MaxChangelogDataSize)  {
        YT_LOG_INFO("Requesting checkpoint due to data size limit (DataSizeSinceLastCheckpoint: %v, MaxChangelogDataSize: %v)",
            Changelog_->GetDataSize(),
            Config_->MaxChangelogDataSize);
    } else {
        return;
    }

    Checkpoint();
}

void TLeaderCommitter::Checkpoint()
{
    YT_VERIFY(!AqcuiringChangelog_);

    AqcuiringChangelog_ = true;
    RunChangelogAcquisition(Config_, EpochContext_, LoggedVersion_.SegmentId + 1, std::nullopt).Apply(
        BIND(&TLeaderCommitter::OnChangelogAcquired, MakeStrong(this))
            .AsyncVia(EpochContext_->EpochControlInvoker));
}

void TLeaderCommitter::OnSnapshotsComplete()
{
    YT_VERIFY(LastSnapshotInfo_);

    int successCount = 0;
    bool checksumMismatch = false;
    std::optional<TChecksum> canonicalChecksum;
    for (auto id = 0; id < std::ssize(LastSnapshotInfo_->Checksums); ++id) {
        auto checksum = LastSnapshotInfo_->Checksums[id];
        if (checksum) {
            ++successCount;
            if (canonicalChecksum) {
                checksumMismatch |= (*canonicalChecksum != *checksum);
            } else {
                canonicalChecksum = checksum;
            }
        }
    }

    YT_LOG_INFO("Distributed snapshot creation finished (SnapshotId: %v, SuccessCount: %v)",
        LastSnapshotInfo_->SnapshotId,
        successCount);

    if (checksumMismatch) {
        for (auto id = 0; id < std::ssize(LastSnapshotInfo_->Checksums); ++id) {
            auto checksum = LastSnapshotInfo_->Checksums[id];
            if (checksum) {
                YT_LOG_ERROR("Snapshot checksum mismatch (SnapshotId: %v, PeerId: %v, Checksum: %llx)",
                    LastSnapshotInfo_->SnapshotId,
                    id,
                    *checksum);
            }
        }
    }

    LastSnapshotInfo_ = std::nullopt;
}

bool TLeaderCommitter::CanBuildSnapshot() const
{
    // We can be acquiring changelog, it is ok.
    return !LastSnapshotInfo_;
}

TFuture<int> TLeaderCommitter::BuildSnapshot(bool waitForCompletion)
{
    YT_VERIFY(!LastSnapshotInfo_);

    LastSnapshotInfo_ = TShapshotInfo{
        .SnapshotId = LoggedVersion_.SegmentId + 1
    };

    auto result = waitForCompletion ? LastSnapshotInfo_->Promise : MakeFuture(LastSnapshotInfo_->SnapshotId);
    if (!AqcuiringChangelog_) {
        Checkpoint();
    }
    return result;
}

void TLeaderCommitter::OnLocalSnapshotBuilt(int snapshotId, const TErrorOr<TRemoteSnapshotParams>& rspOrError)
{
    YT_LOG_INFO("Local snapshot built (SnapshotId: %v)", snapshotId);

    if (!LastSnapshotInfo_ || LastSnapshotInfo_->SnapshotId > snapshotId) {
        YT_LOG_INFO("Stale local snapshot built, ignoring (SnapshotId: %v)", snapshotId);
        return;
    }
    YT_VERIFY(LastSnapshotInfo_->SnapshotId == snapshotId);

    auto id = CellManager_->GetSelfPeerId();
    YT_VERIFY(!LastSnapshotInfo_->HasReply[id]);

    if (rspOrError.IsOK()) {
        auto id = CellManager_->GetSelfPeerId();
        YT_VERIFY(!LastSnapshotInfo_->Checksums[id]);
        auto value = rspOrError.Value();
        YT_VERIFY(value.SnapshotId == snapshotId);
        LastSnapshotInfo_->Checksums[id] = value.Checksum;
        LastSnapshotInfo_->Promise.Set(value.SnapshotId);
    } else {
        LastSnapshotInfo_->Promise.Set(rspOrError);
    }

    OnSnapshotReply(id);
}

void TLeaderCommitter::OnChangelogAcquired(const TError& result)
{
    AqcuiringChangelog_ = false;
    if (!result.IsOK()) {
        if (LastSnapshotInfo_) {
            LastSnapshotInfo_->Promise.TrySet(result);
            LastSnapshotInfo_ = std::nullopt;
        }
        // restart or retry
        YT_LOG_ERROR(result);
        return;
    }

    auto changelogId = LoggedVersion_.SegmentId + 1;
    YT_VERIFY(changelogId == ChangelogId_ + 1);

    auto changelog = WaitFor(EpochContext_->ChangelogStore->OpenChangelog(changelogId))
        .ValueOrThrow();

    YT_LOG_INFO("Started building snapshot (SnapshotId: %v)", changelogId);

    if (!LastSnapshotInfo_) {
        LastSnapshotInfo_ = TShapshotInfo{
            .SnapshotId = changelogId
        };
    } else {
        YT_VERIFY(LastSnapshotInfo_->SequenceNumber == -1);
        YT_VERIFY(LastSnapshotInfo_->SnapshotId == changelogId);
    }

    const auto& selfState = PeerStates_[CellManager_->GetSelfPeerId()];
    LastSnapshotInfo_->SequenceNumber = selfState.LastLoggedSequenceNumber;
    LastSnapshotInfo_->Checksums.resize(CellManager_->GetTotalPeerCount());
    LastSnapshotInfo_->HasReply.resize(CellManager_->GetTotalPeerCount());

    LoggedVersion_ = LoggedVersion_.Rotate();
    ChangelogId_ = changelogId;
    Changelog_ = changelog;
    YT_VERIFY(Changelog_->GetRecordCount() == 0);

    BIND(&TDecoratedAutomaton::BuildSnapshot, DecoratedAutomaton_)
        .AsyncVia(EpochContext_->EpochUserAutomatonInvoker)
        .Run(ChangelogId_, selfState.LastLoggedSequenceNumber)
        .Apply(
            BIND(&TLeaderCommitter::OnLocalSnapshotBuilt, MakeStrong(this), ChangelogId_)
            .AsyncVia(EpochContext_->EpochControlInvoker));
}

void TLeaderCommitter::LogMutation(TMutationDraft&& mutationDraft)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TCurrentTraceContextGuard traceContextGuard(mutationDraft.Request.TraceContext);

    auto& selfState = PeerStates_[CellManager_->GetSelfPeerId()];

    auto timestamp = GetInstant();
    auto randomSeed = RandomNumber<ui64>();
    ++selfState.LastLoggedSequenceNumber;
    MutationHeader_.Clear(); // don't forget to cleanup the pooled instance
    MutationHeader_.set_reign(mutationDraft.Request.Reign);
    MutationHeader_.set_mutation_type(mutationDraft.Request.Type);
    MutationHeader_.set_timestamp(timestamp.GetValue());
    MutationHeader_.set_random_seed(randomSeed);
    MutationHeader_.set_segment_id(LoggedVersion_.SegmentId);
    MutationHeader_.set_record_id(LoggedVersion_.RecordId);
    MutationHeader_.set_prev_random_seed(LastRandomSeed_);
    MutationHeader_.set_sequence_number(selfState.LastLoggedSequenceNumber);
    if (mutationDraft.Request.MutationId) {
        ToProto(MutationHeader_.mutable_mutation_id(), mutationDraft.Request.MutationId);
    }

    auto recordData = SerializeMutationRecord(MutationHeader_, mutationDraft.Request.Data);
    Changelog_->Append({recordData});

    YT_VERIFY(mutationDraft.Promise);
    auto mutation = New<TPendingMutation>(
        LoggedVersion_,
        std::move(mutationDraft.Request),
        timestamp,
        randomSeed,
        LastRandomSeed_,
        selfState.LastLoggedSequenceNumber,
        recordData,
        std::move(mutationDraft.Promise));

    LastRandomSeed_ = randomSeed;
    LoggedVersion_ = LoggedVersion_.Advance();

    if (!MutationQueue_.empty()) {
        YT_VERIFY(MutationQueue_.back()->SequenceNumber + 1 == mutation->SequenceNumber);
    }

    YT_LOG_DEBUG("Mutation logged (SequenceNumber: %v, Version: %v, RandSeed: %llx)",
        mutation->SequenceNumber,
        mutation->Version,
        mutation->RandomSeed);

    MutationQueueDataSize_ += sizeof(mutation) + mutation->SerializedMutation.Size();
    MutationQueue_.push_back(std::move(mutation));

    MaybeCheckpoint();
    MaybePromoteCommitedSequenceNumber();
}

void TLeaderCommitter::OnCommittedSequenceNumberUpdated()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto automatonSequenceNumber = DecoratedAutomaton_->GetSequenceNumber();
    YT_VERIFY(LastOffloadedSequenceNumber_ >= automatonSequenceNumber);
    YT_VERIFY(CommittedState_.SequenceNumber >= LastOffloadedSequenceNumber_);

    if (CommittedState_.SequenceNumber == LastOffloadedSequenceNumber_) {
        return;
    }

    auto queueStartSequenceNumber = MutationQueue_.front()->SequenceNumber;
    std::vector<TPendingMutationPtr> mutations;
    for (auto i = LastOffloadedSequenceNumber_ + 1; i <= CommittedState_.SequenceNumber; ++i) {
        auto queueIndex = i - queueStartSequenceNumber;
        // restart instead of crash
        YT_VERIFY(queueIndex >= 0 && queueIndex < std::ssize(MutationQueue_));
        YT_VERIFY(MutationQueue_[queueIndex]->LocalCommitPromise);
        YT_VERIFY(MutationQueue_[queueIndex]->SequenceNumber == i);
        mutations.push_back(MutationQueue_[queueIndex]);
    }

    YT_VERIFY(LastOffloadedSequenceNumber_ + std::ssize(mutations) == CommittedState_.SequenceNumber);
    LastOffloadedSequenceNumber_ = CommittedState_.SequenceNumber;
    DoCommitMutations(std::move(mutations));
}

TReachableState TLeaderCommitter::GetCommittedState() const
{
    return CommittedState_;
}

TVersion TLeaderCommitter::GetLoggedVersion() const
{
    return LoggedVersion_;
}

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContext* epochContext,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : TCommitterBase(
        std::move(config),
        options,
        std::move(decoratedAutomaton),
        epochContext,
        std::move(logger),
        profiler)
{ }

i64 TFollowerCommitter::GetLoggedSequenceNumber() const
{
    return LoggedSequenceNumber_;
}

void TFollowerCommitter::SetLoggedSequenceNumber(i64 number)
{
    LoggedSequenceNumber_ = number;
    YT_VERIFY(LoggedMutations_.empty());

    AcceptedSequenceNumber_ = number;
    YT_VERIFY(AcceptedMutations_.empty());
}

void TFollowerCommitter::AcceptMutations(
    i64 startSequenceNumber,
    const std::vector<TSharedRef>& recordsData)
{
    auto expectedSequenceNumber = GetExpectedSequenceNumber();
    YT_LOG_DEBUG("Trying to accept mutations (ExpectedSequenceNumber: %v, StartSequenceNumber: %v, MutationCount: %v)",
        expectedSequenceNumber,
        startSequenceNumber,
        recordsData.size());

    if (expectedSequenceNumber < startSequenceNumber) {
        return;
    }

    auto startMutationIndes = expectedSequenceNumber - startSequenceNumber;
    auto mutationIndex = startMutationIndes;
    for (; mutationIndex < std::ssize(recordsData); ++mutationIndex) {
        DoAcceptMutation(recordsData[mutationIndex]);
    }

    YT_LOG_DEBUG("Mutations accepted (StartMutationIndex: %v, LastMutationIndex: %v)",
        startMutationIndes,
        mutationIndex);
}

void TFollowerCommitter::DoAcceptMutation(const TSharedRef& recordData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TSharedRef mutationData;
    DeserializeMutationRecord(recordData, &MutationHeader_, &mutationData);

    TMutationRequest request;
    request.Reign = MutationHeader_.reign();
    request.Type = std::move(*MutationHeader_.mutable_mutation_type());
    request.Data = std::move(mutationData);
    request.MutationId = FromProto<TMutationId>(MutationHeader_.mutation_id());

    AcceptedMutations_.push(
        New<TPendingMutation>(
            TVersion(MutationHeader_.segment_id(), MutationHeader_.record_id()),
            std::move(request),
            FromProto<TInstant>(MutationHeader_.timestamp()),
            MutationHeader_.random_seed(),
            MutationHeader_.prev_random_seed(),
            MutationHeader_.sequence_number(),
            recordData));

    ++AcceptedSequenceNumber_;
    YT_VERIFY(AcceptedSequenceNumber_ == MutationHeader_.sequence_number());
}

i64 TFollowerCommitter::GetExpectedSequenceNumber() const
{
    return AcceptedSequenceNumber_ + 1;
}

void TFollowerCommitter::RegisterNextChangelog(int id, IChangelogPtr changelog)
{
    InsertOrCrash(NextChangelogs_, std::make_pair(id, changelog));
    YT_LOG_INFO("Changelog registered (ChangelogId: %v)", id);
}

void TFollowerCommitter::PrepareNextChangelog(TVersion version)
{
    YT_LOG_INFO("Preparing changelog (Version: %v)", version);

    auto changelogId = version.SegmentId;
    YT_VERIFY(ChangelogId_ < changelogId);

    if (Changelog_) {
        // We should somehow make sure that we start a new changelog with (N, 0).
        // However we might be writing to an existing changelog (when follower joins a working quorum).
        YT_VERIFY(version.RecordId == 0);
    }

    // TODO: WriteChangelogsAtFollowers.
    while (!NextChangelogs_.empty() && NextChangelogs_.begin()->first < changelogId) {
        NextChangelogs_.erase(NextChangelogs_.begin());
    }

    auto it = NextChangelogs_.find(changelogId);
    if (it != NextChangelogs_.end()) {
        ChangelogId_ = it->first;
        Changelog_ = it->second;
        YT_LOG_INFO("Changelog found in next changelogs (Version: %v)", version);
        NextChangelogs_.erase(it);
        return;
    }

    YT_LOG_INFO("Cannot find changelog in next changelogs, creating (Version: %v, Term: %v)",
        version,
        EpochContext_->Term);

    NHydra::NProto::TChangelogMeta meta;
    meta.set_term(EpochContext_->Term);
    auto future = WaitFor(EpochContext_->ChangelogStore->CreateChangelog(changelogId, meta));
    if (!future.IsOK()) {
        LoggingFailed_.Fire(TError("Error creating changelog")
            << TErrorAttribute("changelog_id", changelogId)
            << future);
        future.ThrowOnError();
    }
    ChangelogId_ = changelogId;
    Changelog_ = future.Value();
}

TFuture<void> TFollowerCommitter::GetLastLoggedMutationFuture()
{
    return LastLoggedMutationFuture_;
}

TFuture<void> TFollowerCommitter::LogMutations()
{
    auto localFlushFuture = VoidFuture;

    // Logging more than one batch at a time makes it difficult to promote LoggedSequenceNumber_ correctly.
    // (And creates other weird problems.)
    if (LoggingMutations_) {
        return localFlushFuture;
    }

    LoggingMutations_ = true;

    i64 lastMutationSequenceNumber = 0;
    int loggedCount = 0;
    while (loggedCount < Config_->MaxLoggedMutationsPerRequest && !AcceptedMutations_.empty()) {
        auto mutation = std::move(AcceptedMutations_.front());
        AcceptedMutations_.pop();

        auto version = mutation->Version;
        if (version.SegmentId != ChangelogId_) {
            PrepareNextChangelog(version);
        }

        localFlushFuture = LogMutation(mutation);
        lastMutationSequenceNumber = mutation->SequenceNumber;
        LoggedMutations_.push(std::move(mutation));
        ++loggedCount;

    }

    if (loggedCount == 0) {
        LoggingMutations_ = false;
        return localFlushFuture;
    }

    LastLoggedMutationFuture_ = localFlushFuture.Apply(
        BIND(&TFollowerCommitter::OnMutationsLogged, MakeStrong(this), loggedCount, lastMutationSequenceNumber)
            .AsyncVia(EpochContext_->EpochControlInvoker));
    return LastLoggedMutationFuture_;
}

void TFollowerCommitter::OnMutationsLogged(int loggedCount, i64 lastMutationSequenceNumber)
{
    YT_VERIFY(LoggedSequenceNumber_ + loggedCount == lastMutationSequenceNumber);
    LoggedSequenceNumber_ = lastMutationSequenceNumber;
    YT_LOG_DEBUG("Logged mutations at follower (LoggedSequenceNumber: %v)", LoggedSequenceNumber_);
    LoggingMutations_ = false;
}

TFuture<void> TFollowerCommitter::LogMutation(const TPendingMutationPtr& mutation)
{
    if (!Changelog_) {
        return VoidFuture;
    }

    return Changelog_->Append({mutation->SerializedMutation});
}

void TFollowerCommitter::CommitMutations(i64 committedSequenceNumber)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (committedSequenceNumber <= SelfCommittedSequenceNumber_) {
        return;
    }

    YT_LOG_DEBUG("Committing mutations at follower (ReceivedCommittedSequenceNumber: %v, SelfCommittedSequenceNumber: %v)",
        committedSequenceNumber,
        SelfCommittedSequenceNumber_);

    SelfCommittedSequenceNumber_ = committedSequenceNumber;

    auto automatonSequenceNumber = DecoratedAutomaton_->GetSequenceNumber();
    YT_VERIFY(SelfCommittedSequenceNumber_ >= automatonSequenceNumber);
    if (SelfCommittedSequenceNumber_ == automatonSequenceNumber) {
        return;
    }

    std::vector<TPendingMutationPtr> mutations;
    while (!LoggedMutations_.empty()) {
        auto&& mutation = LoggedMutations_.front();
        if (mutation->SequenceNumber > SelfCommittedSequenceNumber_) {
            break;
        }

        YT_VERIFY(!mutation->LocalCommitPromise);
        mutations.push_back(std::move(mutation));
        LoggedMutations_.pop();
    }

    auto mutationCount = std::ssize(mutations);
    DoCommitMutations(std::move(mutations));

    YT_LOG_DEBUG("Mutations committed at follower (MutationCount: %v)",
        mutationCount);
}

void TFollowerCommitter::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
