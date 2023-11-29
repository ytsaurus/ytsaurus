#include "mutation_committer.h"

#include "private.h"
#include "decorated_automaton.h"
#include "changelog_acquisition.h"
#include "lease_tracker.h"
#include "helpers.h"
#include "changelog.h"
#include "config.h"
#include "mutation_context.h"
#include "serialize.h"
#include "snapshot.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/utilex/random.h>

#include <utility>

namespace NYT::NHydra {

using namespace NElection;
using namespace NYTree;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NTracing;
using namespace NRpc;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

constexpr auto FollowerCatchupLoggingPeriod = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

namespace {

i64 GetMutationDataSize(const TPendingMutationPtr& mutation)
{
    return sizeof(TPendingMutation) + mutation->RecordData.Size() + mutation->Request.Data.Size();
}

TError MakeStoppedError()
{
    return TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCommitterBase::TCommitterBase(
    TConfigWrapperPtr config,
    const TDistributedHydraManagerOptions& options,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContext* epochContext,
    TLogger logger,
    TProfiler /*profiler*/,
    IChangelogPtr changelog)
    : Config_(std::move(config))
    , Options_(options)
    , DecoratedAutomaton_(std::move(decoratedAutomaton))
    , EpochContext_(epochContext)
    , Logger(std::move(logger))
    , CellManager_(EpochContext_->CellManager)
    , Changelog_(std::move(changelog))
{
    YT_VERIFY(Config_);
    YT_VERIFY(DecoratedAutomaton_);
    YT_VERIFY(EpochContext_);

    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);
}

TFuture<void> TCommitterBase::ScheduleApplyMutations(std::vector<TPendingMutationPtr> mutations)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LastOffloadedMutationsFuture_ = BIND(&TDecoratedAutomaton::ApplyMutations, DecoratedAutomaton_)
        .AsyncViaGuarded(
            EpochContext_->EpochUserAutomatonInvoker,
            TError("Error applying mutations"))
        .Run(std::move(mutations));
    return LastOffloadedMutationsFuture_;
}

TFuture<void> TCommitterBase::GetLastOffloadedMutationsFuture()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return LastOffloadedMutationsFuture_;
}

TFuture<void> TCommitterBase::GetLastLoggedMutationFuture()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return LastLoggedMutationFuture_;
}

TErrorOr<IChangelogPtr> TCommitterBase::ExtractNextChangelog(TVersion version)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto changelogId = version.SegmentId;

    while (!NextChangelogs_.empty() && NextChangelogs_.begin()->first < changelogId) {
        NextChangelogs_.erase(NextChangelogs_.begin());
    }

    auto it = NextChangelogs_.find(changelogId);
    if (it != NextChangelogs_.end()) {
        auto changelog = it->second;
        YT_LOG_INFO("Changelog found in next changelogs (Version: %v)", version);
        NextChangelogs_.erase(it);
        return changelog;
    }

    YT_LOG_INFO("Cannot find changelog in next changelogs, creating (Version: %v, Term: %v)",
        version,
        EpochContext_->Term);

    auto openResult = WaitFor(EpochContext_->ChangelogStore->TryOpenChangelog(changelogId));
    if (!openResult.IsOK()) {
        LoggingFailed_.Fire(TError("Error opening changelog")
            << TErrorAttribute("changelog_id", changelogId)
            << openResult);
        return openResult;
    }

    if (auto changelog = openResult.Value()) {
        if (Changelog_) {
            YT_LOG_INFO("Changelog opened, but it should not exist (OldChangelogId: %v, ChangelogId: %v)",
                Changelog_->GetId(),
                changelogId);
            // There is a verify above that checks that mutation has version N:0 if it is not the first changelog,
            // so this should be valid as well.
            YT_VERIFY(changelog->GetRecordCount() == 0);
        }
        return changelog;
    }

    YT_LOG_INFO("Cannot open changelog, creating (ChangelogId: %v, Term: %v)",
        changelogId,
        EpochContext_->Term);

    auto createResultOrError = WaitFor(EpochContext_->ChangelogStore->CreateChangelog(changelogId, /*meta*/ {}, {.CreateWriterEagerly = true}));
    if (!createResultOrError.IsOK()) {
        LoggingFailed_.Fire(TError("Error creating changelog")
            << TErrorAttribute("changelog_id", changelogId)
            << createResultOrError);
        return createResultOrError;
    }

    return createResultOrError.Value();
}

TError TCommitterBase::PrepareNextChangelog(TVersion version)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_LOG_INFO("Preparing changelog (Version: %v)", version);

    auto changelogId = version.SegmentId;
    if (Changelog_) {
        YT_VERIFY(Changelog_->GetId() < changelogId);

        // We should somehow make sure that we start a new changelog with (N, 0).
        // However we might be writing to an existing changelog (when committer joins a working quorum).
        YT_VERIFY(version.RecordId == 0);
    }

    auto nextChangelogOrError = ExtractNextChangelog(version);
    if (!nextChangelogOrError.IsOK()) {
        return nextChangelogOrError;
    }
    if (Changelog_) {
        CloseChangelog(Changelog_);
    }
    Changelog_ = nextChangelogOrError.Value();
    return {};
}

void TCommitterBase::RegisterNextChangelog(int id, IChangelogPtr changelog)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EmplaceOrCrash(NextChangelogs_, id, changelog);
    YT_LOG_INFO("Changelog registered (ChangelogId: %v)", id);
}

void TCommitterBase::CloseChangelog(const IChangelogPtr& changelog)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Config_->Get()->CloseChangelogs || !changelog) {
        return;
    }

    // NB: Changelog is captured into a closure to prevent
    // its destruction before closing.
    changelog->Close()
        .Subscribe(BIND([this, this_ = MakeStrong(this), changelog = changelog] (const TError& error) {
            if (error.IsOK()) {
                YT_LOG_DEBUG("Changelog closed successfully (ChangelogId: %v)",
                    changelog->GetId());
            } else {
                YT_LOG_WARNING(error, "Failed to close changelog (ChangelogId: %v)",
                    changelog->GetId());
            }
        }));
}

////////////////////////////////////////////////////////////////////////////////

TLeaderCommitter::TLeaderCommitter(
    TConfigWrapperPtr config,
    const TDistributedHydraManagerOptions& options,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TLeaderLeasePtr leaderLease,
    TMutationDraftQueuePtr mutationDraftQueue,
    IChangelogPtr changelog,
    TReachableState reachableState,
    TEpochContext* epochContext,
    TLogger logger,
    TProfiler profiler)
    : TCommitterBase(
        config,
        options,
        std::move(decoratedAutomaton),
        epochContext,
        std::move(logger),
        std::move(profiler),
        std::move(changelog))
    , MutationDraftQueue_(std::move(mutationDraftQueue))
    , LeaderLease_(std::move(leaderLease))
    , FlushMutationsExecutor_(New<TPeriodicExecutor>(
        EpochContext_->EpochControlInvoker,
        BIND(&TLeaderCommitter::FlushMutations, MakeWeak(this)),
        Config_->Get()->MutationFlushPeriod))
    , SerializeMutationsExecutor_(New<TPeriodicExecutor>(
        EpochContext_->EpochControlInvoker,
        BIND(&TLeaderCommitter::SerializeMutations, MakeWeak(this)),
        Config_->Get()->MutationSerializationPeriod))
    , CheckpointCheckExecutor_(New<TPeriodicExecutor>(
        EpochContext_->EpochControlInvoker,
        BIND(&TLeaderCommitter::MaybeCheckpoint, MakeWeak(this)),
        Config_->Get()->CheckpointCheckPeriod))
    , InitialState_(reachableState)
    , CommittedState_(std::move(reachableState))
    , BatchSizeSummary_(profiler.Summary("/mutation_batch_size"))
    , MutationQueueSizeSummary_(profiler.Summary("/mutation_queue_size"))
    , MutationQueueDataSizeSummary_(profiler.Summary("/mutation_queue_data_size"))
{
    PeerStates_.assign(CellManager_->GetTotalPeerCount(), {-1, -1});

    RegisterNextChangelog(Changelog_->GetId(), Changelog_);

    auto selfId = CellManager_->GetSelfPeerId();
    auto& selfState = PeerStates_[selfId];
    selfState.NextExpectedSequenceNumber = CommittedState_.SequenceNumber + 1;
    selfState.LastLoggedSequenceNumber = CommittedState_.SequenceNumber;

    LastOffloadedSequenceNumber_ = CommittedState_.SequenceNumber;
    NextLoggedSequenceNumber_ = CommittedState_.SequenceNumber + 1;

    FlushMutationsExecutor_->Start();
}

TFuture<void> TLeaderCommitter::GetLastMutationFuture()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (MutationQueue_.empty()) {
        return VoidFuture;
    }

    return MutationQueue_.back()->LocalCommitPromise
        .ToFuture()
        .AsVoid();
}

void TLeaderCommitter::SerializeMutations()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (RotatingChangelog_) {
        YT_LOG_DEBUG("Skip serializing mutations as changelog is being rotated");
        return;
    }

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

    auto config = Config_->Get();

    NTracing::TNullTraceContextGuard traceContextGuard;

    auto readOnly = EpochContext_->ReadOnly.load();

    std::vector<TMutationDraft> mutationDrafts;
    while (std::ssize(mutationDrafts) < config->MaxCommitBatchRecordCount) {
        TMutationDraft mutationDraft;
        if (!MutationDraftQueue_->TryDequeue(&mutationDraft)) {
            break;
        }

        if (mutationDraft.Request.Type == EnterReadOnlyMutationType) {
            EpochContext_->ReadOnly = readOnly = true;
        }

        if (readOnly && !IsSystemMutationType(mutationDraft.Request.Type)) {
            auto error = TError(
                NHydra::EErrorCode::ReadOnly,
                "Read-only mode is active");
            mutationDraft.Promise.Set(TError(
                NRpc::EErrorCode::Unavailable,
                "Cannot commit a mutation at the moment")
                << error);
            if (Options_.ResponseKeeper && mutationDraft.Request.MutationId) {
                if (auto setResponseKeeperPromise = Options_.ResponseKeeper->EndRequest(
                    mutationDraft.Request.MutationId,
                    error,
                    /*remember*/ false))
                {
                    setResponseKeeperPromise();
                }
            }
            continue;
        }

        auto epochId = mutationDraft.Request.EpochId;
        auto currentEpochId = *CurrentEpochId;
        if (epochId && epochId != currentEpochId) {
            mutationDraft.Promise.Set(TError(
                NRpc::EErrorCode::Unavailable,
                "Mutation has invalid epoch: expected %v, actual %v",
                currentEpochId,
                epochId));
            continue;
        }

        mutationDrafts.push_back(std::move(mutationDraft));
    }

    if (!mutationDrafts.empty()) {
        LogMutations(std::move(mutationDrafts));
    }

    MaybeFlushMutations();
    DrainQueue();
}

void TLeaderCommitter::BuildMonitoring(TFluentMap fluent)
{
    fluent
        .Item("next_logged_version").Value(ToString(NextLoggedVersion_))
        .Item("next_logged_sequence_number").Value(NextLoggedSequenceNumber_)
        .Item("last_offloaded_sequence_number").Value(LastOffloadedSequenceNumber_)
        .Item("last_random_seed").Value(LastRandomSeed_)
        .Item("committed_sequence_number").Value(CommittedState_.SequenceNumber)
        .Item("committed_seqment_id").Value(CommittedState_.SegmentId)
        .Item("peer_states").DoListFor(
            PeerStates_,
            [&] (TFluentList fluent, const auto& peer) {
                fluent.Item().BeginMap()
                    .Item("next_expected_sequence_number").Value(peer.NextExpectedSequenceNumber)
                    .Item("last_logged_sequence_number").Value(peer.LastLoggedSequenceNumber)
                    .Item("in_flight_request_count").Value(peer.InFlightRequestCount)
                    .Item("in_flight_mutation_count").Value(peer.InFlightMutationCount)
                    .Item("in_flight_mutation_data_size").Value(peer.InFlightMutationDataSize)
                    .Item("accept_mutations_mode").Value(peer.Mode)
                .EndMap();
            });
}

void TLeaderCommitter::Reconfigure()
{
    auto config = Config_->Get();
    FlushMutationsExecutor_->SetPeriod(config->MutationFlushPeriod);
    SerializeMutationsExecutor_->SetPeriod(config->MutationSerializationPeriod);
    CheckpointCheckExecutor_->SetPeriod(config->CheckpointCheckPeriod);
}

void TLeaderCommitter::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LastRandomSeed_ = DecoratedAutomaton_->GetRandomSeed();
    NextLoggedVersion_ = {Changelog_->GetId(), 0};

    auto sequenceNumber = DecoratedAutomaton_->GetSequenceNumber();
    YT_VERIFY(CommittedState_.SequenceNumber == sequenceNumber);

    YT_LOG_INFO("Leader committer started (LastRandomSeed: %x, LoggedVersion: %v)",
        LastRandomSeed_,
        NextLoggedVersion_);

    UpdateSnapshotBuildDeadline();

    SerializeMutationsExecutor_->Start();
    CheckpointCheckExecutor_->Start();
}

void TLeaderCommitter::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_UNUSED_FUTURE(SerializeMutationsExecutor_->Stop());
    YT_UNUSED_FUTURE(FlushMutationsExecutor_->Stop());

    // YT-16687: We do not want to apply mutation after its promise is set.
    Y_UNUSED(WaitFor(LastOffloadedMutationsFuture_));

    auto error = MakeStoppedError();
    for (const auto& mutation : MutationQueue_) {
        mutation->LocalCommitPromise.TrySet(error);
    }

    TMutationDraft mutationDraft;
    while (MutationDraftQueue_->TryDequeue(&mutationDraft)) {
        mutationDraft.Promise.TrySet(error);
    }

    for (const auto& [id, changelog] : NextChangelogs_) {
        CloseChangelog(changelog);
    }
    CloseChangelog(Changelog_);

    MutationQueue_.clear();
    MutationQueueSizeSummary_.Record(0);
    MutationQueueDataSize_ = 0;
    MutationQueueDataSizeSummary_.Record(0);

    if (LastSnapshotInfo_) {
        LastSnapshotInfo_->Promise.TrySet(error);
        LastSnapshotInfo_ = std::nullopt;
    }

    PeerStates_.clear();
}

void TLeaderCommitter::FlushMutations()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_LOG_DEBUG("Started flushing mutations");

    for (auto followerId = 0; followerId < CellManager_->GetTotalPeerCount(); ++followerId) {
        if (followerId == CellManager_->GetSelfPeerId()) {
            continue;
        }

        auto channel = CellManager_->GetPeerChannel(followerId);
        if (!channel) {
            continue;
        }

        auto& followerState = PeerStates_[followerId];
        if (followerState.Mode == EAcceptMutationsMode::Slow && followerState.InFlightRequestCount > 0) {
            YT_LOG_DEBUG("Skipping sending mutations to follower since there are in-flight requests (FollowerId: %v, InFlightRequestCount: %v)",
                followerId,
                followerState.InFlightRequestCount);
            continue;
        }

        auto config = Config_->Get();
        if (followerState.Mode == EAcceptMutationsMode::Fast &&
            (followerState.InFlightRequestCount > config->MaxInFlightAcceptMutationsRequestCount ||
            followerState.InFlightMutationCount > config->MaxInFlightMutationCount ||
            followerState.InFlightMutationDataSize > config->MaxInFlightMutationDataSize))
        {
            YT_LOG_DEBUG("Skipping sending mutations to follower since in-flight limits are violated (FollowerId: %v, "
                "InFlightRequestCount: %v, InFlightMutationCount: %v, InFlightMutationDataSize: %v)",
                followerId,
                followerState.InFlightRequestCount,
                followerState.InFlightMutationCount,
                followerState.InFlightMutationDataSize);
            continue;
        }

        if (!MutationQueue_.empty() && followerState.NextExpectedSequenceNumber < MutationQueue_.front()->SequenceNumber) {
            if (followerState.NextExpectedSequenceNumber == -1) {
                // This is ok, it actually means that follower hasn't received initial ping (and hasn't recovered) yet,
                // Lets just wait for him to recover.

                // Something useful might or might not happen here.
            } else {
                TError error("Follower %v needs a mutation %v that was already lost",
                    followerId,
                    followerState.NextExpectedSequenceNumber);

                YT_LOG_ERROR(error, "Requesting follower restart (FollowerId: %v)",
                    followerId);

                THydraServiceProxy proxy(channel);
                auto req = proxy.ForceRestart();
                ToProto(req->mutable_reason(), error);

                YT_UNUSED_FUTURE(req->Invoke());

                followerState.NextExpectedSequenceNumber = -1;
                followerState.LastLoggedSequenceNumber = -1;
                continue;
            }
        }

        TInternalHydraServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(config->CommitFlushRpcTimeout);

        auto mutationCount = [&] () -> i64 {
            if (MutationQueue_.empty() || followerState.NextExpectedSequenceNumber == -1) {
                return 0;
            }
            return std::min<i64>(
                config->MaxCommitBatchRecordCount,
                MutationQueue_.back()->SequenceNumber - followerState.NextExpectedSequenceNumber + 1);
        }();

        auto request = proxy.AcceptMutations();
        ToProto(request->mutable_epoch_id(), EpochContext_->EpochId);
        request->set_start_sequence_number(followerState.NextExpectedSequenceNumber);

        // We do not want followers to apply mutations before leader, as im might brake multicell:
        // during multicell sync, a cell can get an outdated seqno from leader.
        // This (probably) can be fixed by adding local syncs to committed version
        // before replying to multicell sync, but it sounds like a long and
        // dangerous journey.
        TReachableState automatonState(
            DecoratedAutomaton_->GetAutomatonVersion().SegmentId,
            DecoratedAutomaton_->GetSequenceNumber());
        // We might not yet recovered to this state, but we want followers to recover to it.
        const auto& stateToSend = std::max(automatonState, InitialState_);
        request->set_committed_sequence_number(stateToSend.SequenceNumber);
        request->set_committed_segment_id(stateToSend.SegmentId);

        request->set_term(EpochContext_->Term);

        if (LastSnapshotInfo_ && LastSnapshotInfo_->SequenceNumber != -1) {
            auto* snapshotRequest = request->mutable_snapshot_request();
            snapshotRequest->set_snapshot_id(LastSnapshotInfo_->SnapshotId);
            snapshotRequest->set_sequence_number(LastSnapshotInfo_->SequenceNumber);
            snapshotRequest->set_read_only(LastSnapshotInfo_->ReadOnly);
        }

        YT_LOG_DEBUG("Sending mutations to follower (PeerId: %v, NextExpectedSequenceNumber: %v, MutationCount: %v, CommittedState: %v)",
            followerId,
            followerState.NextExpectedSequenceNumber,
            mutationCount,
            CommittedState_);

        BatchSizeSummary_.Record(mutationCount);

        i64 mutationDataSize = 0;
        if (mutationCount > 0) {
            auto startIndex = followerState.NextExpectedSequenceNumber - MutationQueue_.front()->SequenceNumber;
            for (int i = startIndex; i < startIndex + mutationCount; ++i) {
                YT_VERIFY(i < std::ssize(MutationQueue_));
                const auto& mutation = MutationQueue_[i];
                mutationDataSize += GetMutationDataSize(mutation);
                request->Attachments().push_back(mutation->RecordData);
            }
        }

        if (followerState.Mode == EAcceptMutationsMode::Fast) {
            followerState.NextExpectedSequenceNumber += mutationCount;
        }

        ++followerState.InFlightRequestCount;
        followerState.InFlightMutationCount += mutationCount;
        followerState.InFlightMutationDataSize += mutationDataSize;

        request->Invoke().Subscribe(
            BIND(&TLeaderCommitter::OnMutationsAcceptedByFollower,
                MakeStrong(this),
                followerId,
                mutationCount,
                mutationDataSize)
                .Via(EpochContext_->EpochControlInvoker));
    }
}

void TLeaderCommitter::OnSnapshotReply(int peerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (LastSnapshotInfo_->HasReply[peerId]) {
        return;
    }

    YT_LOG_INFO("Received a new snapshot reply (PeerId: %v, SnapshotId: %v)",
        peerId,
        LastSnapshotInfo_->SnapshotId);

    LastSnapshotInfo_->HasReply[peerId] = true;
    ++LastSnapshotInfo_->ReplyCount;
    if (LastSnapshotInfo_->ReplyCount == std::ssize(LastSnapshotInfo_->HasReply)) {
        OnSnapshotsComplete();
    }
}

void TLeaderCommitter::OnMutationsAcceptedByFollower(
    int followerId,
    int mutationCount,
    i64 mutationDataSize,
    const TInternalHydraServiceProxy::TErrorOrRspAcceptMutationsPtr& rspOrError)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto& peerState = PeerStates_[followerId];

    --peerState.InFlightRequestCount;
    YT_VERIFY(peerState.InFlightRequestCount >= 0);
    peerState.InFlightMutationCount -= mutationCount;
    YT_VERIFY(peerState.InFlightMutationCount >= 0);
    peerState.InFlightMutationDataSize -= mutationDataSize;
    YT_VERIFY(peerState.InFlightMutationDataSize >= 0);

    if (!rspOrError.IsOK()) {
        YT_LOG_EVENT(
            Logger,
            IsChannelFailureError(rspOrError) ? ELogLevel::Debug : ELogLevel::Warning,
            rspOrError,
            "Error logging mutations at follower (FollowerId: %v)",
            followerId);

        // TODO(aleksandra-zh): This might be an old reply.
        if (LastSnapshotInfo_ && LastSnapshotInfo_->SequenceNumber != -1) {
            OnSnapshotReply(followerId);
        }

        if (peerState.Mode == EAcceptMutationsMode::Fast) {
            YT_LOG_DEBUG("Accept mutations mode is set to slow (FollowerId: %v)",
                followerId);
        }
        peerState.Mode = EAcceptMutationsMode::Slow;

        return;
    }

    const auto& rsp = rspOrError.Value();

    if (rsp->has_snapshot_response()) {
        const auto& snapshotResult = rsp->snapshot_response();
        auto snapshotId = snapshotResult.snapshot_id();
        if (!snapshotResult.has_error()) {
            auto checksum = snapshotResult.checksum();
            if (LastSnapshotInfo_ && LastSnapshotInfo_->SnapshotId == snapshotId) {
                auto& currentChecksum = LastSnapshotInfo_->Checksums[followerId];
                if (currentChecksum) {
                    YT_VERIFY(currentChecksum == checksum);
                } else {
                    currentChecksum = checksum;
                    YT_LOG_INFO("Built snapshot at follower (SnapshotId: %v, FollowerId: %v, Checksum: %x)",
                        snapshotId,
                        followerId,
                        checksum);
                }
            }
        } else if (LastSnapshotInfo_ && LastSnapshotInfo_->SnapshotId == snapshotId && !LastSnapshotInfo_->HasReply[followerId]) {
            auto snapshotError = FromProto<TError>(snapshotResult.error());
            YT_LOG_WARNING(snapshotError, "Error building snapshot at follower (SnapshotId: %v, FollowerId: %v)",
                snapshotId,
                followerId);
        }

        if (LastSnapshotInfo_ && LastSnapshotInfo_->SnapshotId == snapshotId) {
            OnSnapshotReply(followerId);
        }
    }

    auto loggedSequenceNumber = rsp->logged_sequence_number();
    // Take max here in case we receive out of order reply.
    peerState.LastLoggedSequenceNumber = std::max(loggedSequenceNumber, peerState.LastLoggedSequenceNumber);

    auto mutationsAccepted = rsp->mutations_accepted();
    auto nextExpectedSequenceNumber = rsp->expected_sequence_number();

    // This does not depend on mode, do that anyway.
    if (!mutationsAccepted) {
        if (peerState.Mode == EAcceptMutationsMode::Fast) {
            YT_LOG_DEBUG("Accept mutations mode is set to slow (FollowerId: %v)",
                followerId);
        }
        peerState.Mode = EAcceptMutationsMode::Slow;
        peerState.NextExpectedSequenceNumber = nextExpectedSequenceNumber;
        YT_LOG_DEBUG("Mutations were not accepted by follower (FollowerId: %v, NextExpectedSequenceNumber: %v, LoggedSequenceNumber: %v)",
            followerId,
            nextExpectedSequenceNumber,
            loggedSequenceNumber);
    } else {
        YT_LOG_DEBUG("Mutations are flushed by follower (FollowerId: %v, Mode: %v, NextExpectedSequenceNumber: %v, LoggedSequenceNumber: %v)",
            followerId,
            peerState.Mode,
            nextExpectedSequenceNumber,
            loggedSequenceNumber);
        if (peerState.Mode == EAcceptMutationsMode::Slow) {
            YT_LOG_DEBUG("Accept mutations mode is set to fast (FollowerId: %v)",
                followerId);
            // Rollback here seems possible and ok (if follower restarts).
            peerState.NextExpectedSequenceNumber = nextExpectedSequenceNumber;
        }
        peerState.Mode = EAcceptMutationsMode::Fast;
    }

    MaybePromoteCommittedSequenceNumber();
}

void TLeaderCommitter::MaybePromoteCommittedSequenceNumber()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<i64> loggedNumbers;
    for (int i = 0; i < CellManager_->GetTotalPeerCount(); ++i) {
        if (CellManager_->GetPeerConfig(i)->Voting) {
            loggedNumbers.push_back(PeerStates_[i].LastLoggedSequenceNumber);
        }
    }
    YT_VERIFY(std::ssize(loggedNumbers) == CellManager_->GetVotingPeerCount());

    std::sort(loggedNumbers.begin(), loggedNumbers.end(), std::greater<>());

    auto committedSequenceNumber = loggedNumbers[CellManager_->GetQuorumPeerCount() - 1];

    YT_LOG_DEBUG("Trying to promote committed sequence number (NewCommittedSequenceNumber: %v, OldCommittedSequenceNumber: %v)",
        committedSequenceNumber,
        CommittedState_.SequenceNumber);
    if (committedSequenceNumber == -1 || CommittedState_.SequenceNumber >= committedSequenceNumber) {
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

void TLeaderCommitter::MaybeFlushMutations()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (MutationQueue_.empty()) {
        return;
    }

    // TODO(aleksandra-zh): Some peers may have larger batches. Consider looking at each separately.
    auto batchSize = MutationQueue_.back()->SequenceNumber - CommittedState_.SequenceNumber;
    if (batchSize >= Config_->Get()->MaxCommitBatchRecordCount) {
        FlushMutations();
    }
}

void TLeaderCommitter::DrainQueue()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto popMutationQueue = [&] () {
        const auto& mutation = MutationQueue_.front();
        MutationQueueDataSize_ -= GetMutationDataSize(mutation);
        MutationQueue_.pop_front();

        MutationQueueSizeSummary_.Record(MutationQueue_.size());
        MutationQueueDataSizeSummary_.Record(MutationQueueDataSize_);
    };

    auto config = Config_->Get();
    while (std::ssize(MutationQueue_) > config->MaxQueuedMutationCount) {
        const auto& mutation = MutationQueue_.front();
        if (mutation->SequenceNumber > CommittedState_.SequenceNumber) {
            LoggingFailed_.Fire(TError("Mutation queue mutation count limit exceeded, but the first mutation in queue is still uncommitted")
                << TErrorAttribute("mutation_count", MutationQueue_.size())
                << TErrorAttribute("mutation_sequence_number", mutation->SequenceNumber));
            return;
        }
        popMutationQueue();
    }

    while (MutationQueueDataSize_ > config->MaxQueuedMutationDataSize) {
        const auto& mutation = MutationQueue_.front();
        if (mutation->SequenceNumber > CommittedState_.SequenceNumber) {
            LoggingFailed_.Fire(TError("Mutation queue data size limit exceeded, but the first mutation in queue is still uncommitted")
                << TErrorAttribute("queue_data_size", MutationQueueDataSize_)
                << TErrorAttribute("mutation_sequence_number", mutation->SequenceNumber));
            return;
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
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (AcquiringChangelog_ || LastSnapshotInfo_) {
        return;
    }

    auto config = Config_->Get();
    if (NextLoggedVersion_.RecordId >= config->MaxChangelogRecordCount) {
        YT_LOG_INFO("Requesting checkpoint due to record count limit (RecordCountSinceLastCheckpoint: %v, MaxChangelogRecordCount: %v)",
            NextLoggedVersion_.RecordId,
            config->MaxChangelogRecordCount);
    } else if (Changelog_->GetDataSize() >= config->MaxChangelogDataSize)  {
        YT_LOG_INFO("Requesting checkpoint due to data size limit (DataSizeSinceLastCheckpoint: %v, MaxChangelogDataSize: %v)",
            Changelog_->GetDataSize(),
            config->MaxChangelogDataSize);
    } else if (!EpochContext_->ReadOnly && TInstant::Now() > SnapshotBuildDeadline_) {
        YT_LOG_INFO("Requesting periodic snapshot (SnapshotBuildPeriod: %v, SnapshotBuildSplay: %v)",
            config->SnapshotBuildPeriod,
            config->SnapshotBuildSplay);
    } else {
        return;
    }

    Checkpoint();
}

void TLeaderCommitter::UpdateSnapshotBuildDeadline()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto config = Config_->Get();
    SnapshotBuildDeadline_ =
        TInstant::Now() +
        config->SnapshotBuildPeriod +
        RandomDuration(config->SnapshotBuildSplay);
}

void TLeaderCommitter::Checkpoint()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_VERIFY(!AcquiringChangelog_);

    AcquiringChangelog_ = true;

    RunChangelogAcquisition(Config_->Get(), EpochContext_, NextLoggedVersion_.SegmentId + 1, std::nullopt, Logger).Subscribe(
        BIND(&TLeaderCommitter::OnChangelogAcquired, MakeStrong(this))
            .Via(EpochContext_->EpochControlInvoker));
}

void TLeaderCommitter::OnSnapshotsComplete()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_VERIFY(LastSnapshotInfo_);

    int successCount = 0;
    bool checksumMismatch = false;
    std::optional<TChecksum> canonicalChecksum;
    for (auto id = 0; id < std::ssize(LastSnapshotInfo_->Checksums); ++id) {
        if (auto checksum = LastSnapshotInfo_->Checksums[id]) {
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

    // TODO(aleksandra-zh): remove when we stop building snapshots on all peers.
    if (Config_->Get()->AlertOnSnapshotFailure && successCount == 0) {
        YT_LOG_ALERT("Not enough successful snapshots built (SnapshotId: %v, SuccessCount: %v)",
            LastSnapshotInfo_->SnapshotId,
            successCount);
    }

    if (checksumMismatch) {
        for (auto id = 0; id < std::ssize(LastSnapshotInfo_->Checksums); ++id) {
            auto checksum = LastSnapshotInfo_->Checksums[id];
            if (checksum) {
                YT_LOG_ERROR("Snapshot checksum mismatch (SnapshotId: %v, PeerId: %v, Checksum: %x)",
                    LastSnapshotInfo_->SnapshotId,
                    id,
                    *checksum);
            }
        }
    }

    if (successCount == 0) {
        LastSnapshotInfo_->Promise.TrySet(TError("Error building snapshot")
            << TErrorAttribute("snapshot_id", LastSnapshotInfo_->SnapshotId));
    } else {
        LastSnapshotInfo_->Promise.TrySet(LastSnapshotInfo_->SnapshotId);
    }

    LastSnapshotInfo_ = std::nullopt;
}

bool TLeaderCommitter::CanBuildSnapshot() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // We can be acquiring changelog, it is ok.
    return !LastSnapshotInfo_;
}

TFuture<int> TLeaderCommitter::BuildSnapshot(bool waitForCompletion, bool readOnly)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_VERIFY(!LastSnapshotInfo_);
    LastSnapshotInfo_ = TShapshotInfo{
        .SnapshotId = NextLoggedVersion_.SegmentId + 1,
        .ReadOnly = readOnly
    };

    auto result = waitForCompletion
        ? LastSnapshotInfo_->Promise.ToFuture()
        : MakeFuture(LastSnapshotInfo_->SnapshotId);

    if (!AcquiringChangelog_) {
        Checkpoint();
    }

    return result;
}

std::optional<TFuture<int>> TLeaderCommitter::GetLastSnapshotFuture(bool waitForCompletion, bool readOnly)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!LastSnapshotInfo_) {
        return std::nullopt;
    }

    if (LastSnapshotInfo_->ReadOnly != readOnly) {
        return std::nullopt;
    }

    return waitForCompletion
        ? LastSnapshotInfo_->Promise.ToFuture()
        : MakeFuture(LastSnapshotInfo_->SnapshotId);
}

void TLeaderCommitter::OnLocalSnapshotBuilt(int snapshotId, const TErrorOr<TRemoteSnapshotParams>& rspOrError)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!LastSnapshotInfo_ || LastSnapshotInfo_->SnapshotId > snapshotId) {
        YT_LOG_INFO("Stale local snapshot built, ignoring (SnapshotId: %v)", snapshotId);
        return;
    }

    YT_LOG_INFO("Local snapshot built (SnapshotId: %v)", snapshotId);

    auto selfId = CellManager_->GetSelfPeerId();

    YT_VERIFY(LastSnapshotInfo_->SnapshotId == snapshotId);
    YT_VERIFY(!LastSnapshotInfo_->HasReply[selfId]);

    if (rspOrError.IsOK()) {
        const auto& snapshotParams = rspOrError.Value();
        YT_VERIFY(!LastSnapshotInfo_->Checksums[selfId]);
        YT_VERIFY(snapshotParams.SnapshotId == snapshotId);
        LastSnapshotInfo_->Checksums[selfId] = snapshotParams.Checksum;
    } else {
        YT_LOG_WARNING(rspOrError, "Error building snapshot locally (SnapshotId: %v)",
            snapshotId);
    }

    OnSnapshotReply(selfId);
}

void TLeaderCommitter::OnChangelogAcquired(const TErrorOr<IChangelogPtr>& changelogsOrError)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!changelogsOrError.IsOK()) {
        if (LastSnapshotInfo_) {
            LastSnapshotInfo_->Promise.TrySet(changelogsOrError);
            LastSnapshotInfo_ = std::nullopt;
        }
        YT_LOG_ERROR(changelogsOrError);
        LoggingFailed_.Fire(TError("Error acquiring changelog")
            << changelogsOrError);
        AcquiringChangelog_ = false;
        return;
    }
    auto changelog = changelogsOrError.Value();
    RegisterNextChangelog(changelog->GetId(), changelog);

    auto changelogId = NextLoggedVersion_.SegmentId + 1;
    YT_VERIFY(Changelog_);
    YT_VERIFY(changelogId == Changelog_->GetId() + 1);

    auto oldChangelog = Changelog_;

    auto newNextLoggedVersion = NextLoggedVersion_.Rotate();
    auto preparationResult = PrepareNextChangelog(newNextLoggedVersion);
    if (!preparationResult.IsOK()) {
        LoggingFailed_.Fire(TError("Error opening changelog")
            << TErrorAttribute("changelog_id", changelogId)
            << preparationResult);
        AcquiringChangelog_ = false;
        return;
    }

    if (!LastSnapshotInfo_) {
        LastSnapshotInfo_ = TShapshotInfo{
            .SnapshotId = changelogId,
            .ReadOnly = EpochContext_->ReadOnly
        };
    } else {
        YT_VERIFY(LastSnapshotInfo_->SequenceNumber == -1);
        YT_VERIFY(LastSnapshotInfo_->SnapshotId == changelogId);
    }

    auto snapshotSequenceNumber = NextLoggedSequenceNumber_ - 1;
    YT_LOG_INFO("Started building snapshot (SnapshotId: %v, SequenceNumber: %v)",
        changelogId,
        snapshotSequenceNumber);

    UpdateSnapshotBuildDeadline();

    LastSnapshotInfo_->SequenceNumber = snapshotSequenceNumber;
    LastSnapshotInfo_->Checksums.resize(CellManager_->GetTotalPeerCount());
    LastSnapshotInfo_->HasReply.resize(CellManager_->GetTotalPeerCount());

    NextLoggedVersion_ = newNextLoggedVersion;

    YT_VERIFY(Changelog_->GetRecordCount() == 0);

    AcquiringChangelog_ = false;

    RotatingChangelog_ = true;
    auto result = WaitFor(LastLoggedMutationFuture_);
    RotatingChangelog_ = false;

    if (!result.IsOK()) {
        LoggingFailed_.Fire(TError("Error logging mutations")
            << TErrorAttribute("changelog_id", oldChangelog->GetId())
            << result);
        THROW_ERROR(result);
    }

    BIND(&TDecoratedAutomaton::BuildSnapshot, DecoratedAutomaton_)
        .AsyncVia(EpochContext_->EpochUserAutomatonInvoker)
        .Run(Changelog_->GetId(), snapshotSequenceNumber, EpochContext_->ReadOnly)
        .Subscribe(
            BIND(&TLeaderCommitter::OnLocalSnapshotBuilt, MakeStrong(this), Changelog_->GetId())
                .Via(EpochContext_->EpochControlInvoker));
}

void TLeaderCommitter::LogMutations(std::vector<TMutationDraft> mutationDrafts)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TSharedRef> recordsData;
    recordsData.reserve(mutationDrafts.size());

    auto timestamp = GetInstant();

    auto firstSequenceNumber = NextLoggedSequenceNumber_;
    for (int index = 0; index < std::ssize(mutationDrafts); ++index) {
        auto& mutationDraft = mutationDrafts[index];

        auto randomSeed = mutationDraft.RandomSeed;

        MutationHeader_.Clear(); // don't forget to cleanup the pooled instance
        MutationHeader_.set_reign(mutationDraft.Request.Reign);
        MutationHeader_.set_mutation_type(mutationDraft.Request.Type);
        MutationHeader_.set_timestamp(timestamp.GetValue());
        MutationHeader_.set_random_seed(randomSeed);
        MutationHeader_.set_segment_id(NextLoggedVersion_.SegmentId);
        MutationHeader_.set_record_id(NextLoggedVersion_.RecordId);
        MutationHeader_.set_prev_random_seed(LastRandomSeed_);
        MutationHeader_.set_sequence_number(NextLoggedSequenceNumber_);
        MutationHeader_.set_term(EpochContext_->Term);
        if (mutationDraft.Request.MutationId) {
            ToProto(MutationHeader_.mutable_mutation_id(), mutationDraft.Request.MutationId);
        }

        auto recordData = SerializeMutationRecord(MutationHeader_, mutationDraft.Request.Data);
        recordsData.push_back(recordData);

        YT_VERIFY(mutationDraft.Promise);
        auto mutation = New<TPendingMutation>(
            NextLoggedVersion_,
            std::move(mutationDraft.Request),
            timestamp,
            randomSeed,
            LastRandomSeed_,
            NextLoggedSequenceNumber_,
            EpochContext_->Term,
            std::move(recordData),
            std::move(mutationDraft.Promise));

        LastRandomSeed_ = randomSeed;
        NextLoggedVersion_ = NextLoggedVersion_.Advance();
        ++NextLoggedSequenceNumber_;

        YT_LOG_DEBUG("Logging mutation at leader (SequenceNumber: %v, Version: %v, RandomSeed: %x, MutationType: %v, MutationId: %v)",
            mutation->SequenceNumber,
            mutation->Version,
            mutation->RandomSeed,
            mutation->Request.Type,
            mutation->Request.MutationId);

        if (!MutationQueue_.empty()) {
            YT_VERIFY(MutationQueue_.back()->SequenceNumber + 1 == mutation->SequenceNumber);
        }

        MutationQueueDataSize_ += GetMutationDataSize(mutation);
        MutationQueue_.push_back(std::move(mutation));
    }
    auto lastSequenceNumber = NextLoggedSequenceNumber_ - 1;

    MutationQueueSizeSummary_.Record(MutationQueue_.size());
    MutationQueueDataSizeSummary_.Record(MutationQueueDataSize_);

    auto future = Changelog_->Append(std::move(recordsData));
    LastLoggedMutationFuture_ = future.Apply(
        BIND(&TLeaderCommitter::OnMutationsLogged, MakeStrong(this), firstSequenceNumber, lastSequenceNumber)
            .AsyncVia(EpochContext_->EpochControlInvoker));

    MaybeCheckpoint();

    if (Config_->Get()->MinimizeCommitLatency) {
        FlushMutations();
    }
}

void TLeaderCommitter::OnMutationsLogged(
    i64 firstSequenceNumber,
    i64 lastSequenceNumber,
    const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!error.IsOK()) {
        LoggingFailed_.Fire(TError("Error logging mutations")
            << error);
        return;
    }

    YT_LOG_DEBUG("Mutations logged at leader (SequenceNumbers: %v-%v)",
        firstSequenceNumber,
        lastSequenceNumber);

    auto& selfState = PeerStates_[CellManager_->GetSelfPeerId()];
    selfState.LastLoggedSequenceNumber = std::max(selfState.LastLoggedSequenceNumber, lastSequenceNumber);

    MaybePromoteCommittedSequenceNumber();
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
    for (auto sequenceNumber = LastOffloadedSequenceNumber_ + 1; sequenceNumber <= CommittedState_.SequenceNumber; ++sequenceNumber) {
        auto queueIndex = sequenceNumber - queueStartSequenceNumber;
        if (queueIndex < 0 || queueIndex >= std::ssize(MutationQueue_)) {
            YT_LOG_ALERT("Mutation is lost (SequenceNumber: %v, QueueIndex: %v, MutationQueueSize: %v)",
                sequenceNumber,
                queueIndex,
                std::ssize(MutationQueue_));
            LoggingFailed_.Fire(TError("Mutation is lost")
                << TErrorAttribute("sequence_number", sequenceNumber));
            return;
        }
        YT_VERIFY(MutationQueue_[queueIndex]->LocalCommitPromise);
        YT_VERIFY(MutationQueue_[queueIndex]->SequenceNumber == sequenceNumber);
        mutations.push_back(MutationQueue_[queueIndex]);
    }

    YT_VERIFY(LastOffloadedSequenceNumber_ + std::ssize(mutations) == CommittedState_.SequenceNumber);
    LastOffloadedSequenceNumber_ = CommittedState_.SequenceNumber;
    YT_UNUSED_FUTURE(ScheduleApplyMutations(std::move(mutations)));
}

TVersion TLeaderCommitter::GetNextLoggedVersion() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return NextLoggedVersion_;
}

i64 TLeaderCommitter::GetLoggedSequenceNumber() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return PeerStates_[CellManager_->GetSelfPeerId()].LastLoggedSequenceNumber;
}

i64 TLeaderCommitter::GetLastOffloadedSequenceNumber() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return LastOffloadedSequenceNumber_;
}

////////////////////////////////////////////////////////////////////////////////

TFollowerCommitter::TFollowerCommitter(
    TConfigWrapperPtr config,
    const TDistributedHydraManagerOptions& options,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContext* epochContext,
    TLogger logger,
    TProfiler profiler)
    : TCommitterBase(
        config,
        options,
        std::move(decoratedAutomaton),
        epochContext,
        std::move(logger),
        std::move(profiler),
        nullptr)
{ }

i64 TFollowerCommitter::GetLoggedSequenceNumber() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return LastLoggedSequenceNumber_;
}

void TFollowerCommitter::SetSequenceNumber(i64 number)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_VERIFY(LoggedMutations_.empty());
    LastLoggedSequenceNumber_ = number;

    YT_VERIFY(AcceptedMutations_.empty());
    LastAcceptedSequenceNumber_ = number;

    YT_VERIFY(CommittedSequenceNumber_ == -1);
    CommittedSequenceNumber_ = number;
}

void TFollowerCommitter::BuildMonitoring(TFluentMap fluent)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    fluent
        .Item("last _logged_sequence_number").Value(LastLoggedSequenceNumber_)
        .Item("last_accepted_sequence_number").Value(LastAcceptedSequenceNumber_)
        .Item("committed_sequence_number").Value(CommittedSequenceNumber_)
        .Item("follower_recovery_complete").Value(RecoveryComplete_);
}

void TFollowerCommitter::CatchUp()
{
    RecoveryComplete_ = true;

    while (true) {
        Y_UNUSED(WaitFor(CaughtUpPromise_.ToFuture().ToUncancelable().WithTimeout(FollowerCatchupLoggingPeriod)));
        if (CaughtUpPromise_.IsSet()) {
            break;
        }
        // NB: Keep this diagnostics in sync with #CheckIfCaughtUp.
        YT_LOG_INFO("Follower is still catching up (AcceptedMutationCount: %v, LoggedMutationCount: %v, CommittedSequenceNumberLag: %v)",
            ssize(AcceptedMutations_),
            ssize(LoggedMutations_),
            CommittedSequenceNumber_ - DecoratedAutomaton_->GetSequenceNumber());
    }

    // Promise must be set by now.
    CaughtUpPromise_
        .Get()
        .ThrowOnError();
}

void TFollowerCommitter::CheckIfCaughtUp()
{
    if (CaughtUpPromise_.IsSet()) {
        return;
    }

    if (!RecoveryComplete_) {
        return;
    }

    auto config = Config_->Get();
    if (ssize(AcceptedMutations_) > config->MaxCatchUpAcceptedMutationCount) {
        return;
    }

    if (ssize(LoggedMutations_) > config->MaxCatchUpLoggedMutationCount) {
        return;
    }

    if (CommittedSequenceNumber_ - DecoratedAutomaton_->GetSequenceNumber() > config->MaxCatchUpSequenceNumberGap) {
        return;
    }

    CaughtUpPromise_.TrySet();
}

bool TFollowerCommitter::AcceptMutations(
    i64 startSequenceNumber,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto expectedSequenceNumber = GetExpectedSequenceNumber();
    YT_LOG_DEBUG("Trying to accept mutations (ExpectedSequenceNumber: %v, StartSequenceNumber: %v, MutationCount: %v)",
        expectedSequenceNumber,
        startSequenceNumber,
        recordsData.size());

    if (expectedSequenceNumber < startSequenceNumber) {
        return false;
    }

    auto startIndex = expectedSequenceNumber - startSequenceNumber;
    int acceptedCount = 0;
    for (auto index = startIndex; index < std::ssize(recordsData); ++index, ++acceptedCount) {
        DoAcceptMutation(recordsData[index]);
    }

    YT_LOG_DEBUG_IF(
        acceptedCount > 0,
        "Mutations accepted (SequenceNumbers: %v-%v)",
        expectedSequenceNumber,
        expectedSequenceNumber + acceptedCount - 1);

    CheckIfCaughtUp();

    return true;
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

    YT_VERIFY(!EpochContext_->Discombobulated || IsSystemMutationType(request.Type));

    AcceptedMutations_.push(
        New<TPendingMutation>(
            TVersion(MutationHeader_.segment_id(), MutationHeader_.record_id()),
            std::move(request),
            FromProto<TInstant>(MutationHeader_.timestamp()),
            MutationHeader_.random_seed(),
            MutationHeader_.prev_random_seed(),
            MutationHeader_.sequence_number(),
            MutationHeader_.term(),
            recordData));

    ++LastAcceptedSequenceNumber_;
    YT_VERIFY(LastAcceptedSequenceNumber_ == MutationHeader_.sequence_number());
}

i64 TFollowerCommitter::GetExpectedSequenceNumber() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return LastAcceptedSequenceNumber_ + 1;
}

void TFollowerCommitter::LogMutations()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Logging more than one batch at a time makes it difficult to promote LoggedSequenceNumber_ correctly.
    // (And creates other weird problems.)
    // We should at least have a barrier around PrepareNextChangelog.
    if (LoggingMutations_) {
        return;
    }
    LoggingMutations_ = true;

    i64 firstSequenceNumber = -1;
    i64 lastSequenceNumber = -1;

    std::vector<TSharedRef> recordsData;
    recordsData.reserve(AcceptedMutations_.size());

    auto isPersistenceEnabled = IsPersistenceEnabled(EpochContext_->CellManager, Options_);
    while (!AcceptedMutations_.empty()) {
        auto version = AcceptedMutations_.front()->Version;

        if ((!Changelog_ || version.SegmentId != Changelog_->GetId()) && isPersistenceEnabled) {
            if (!recordsData.empty()) {
                break;
            }

            auto preparationResult = PrepareNextChangelog(version);
            if (!preparationResult.IsOK()) {
                LoggingFailed_.Fire(TError("Error opening changelog")
                    << TErrorAttribute("version", ToString(version))
                    << preparationResult);
            }
            preparationResult.ThrowOnError();
        }

        auto mutation = std::move(AcceptedMutations_.front());
        AcceptedMutations_.pop();

        if (firstSequenceNumber < 0) {
            firstSequenceNumber = mutation->SequenceNumber;
        } else {
            YT_VERIFY(mutation->SequenceNumber == firstSequenceNumber + std::ssize(recordsData));
        }
        lastSequenceNumber = mutation->SequenceNumber;

        recordsData.push_back(mutation->RecordData);
        LoggedMutations_.push(std::move(mutation));
    }

    if (recordsData.empty()) {
        LoggingMutations_ = false;
        return;
    }

    if (!Changelog_) {
        YT_VERIFY(!isPersistenceEnabled);
        LastLoggedSequenceNumber_ = lastSequenceNumber;
        LoggingMutations_ = false;
        return;
    }

    YT_LOG_DEBUG("Logging mutations at follower (SequenceNumbers: %v-%v)",
        firstSequenceNumber,
        lastSequenceNumber);

    CheckIfCaughtUp();

    auto future = Changelog_->Append(std::move(recordsData));
    LastLoggedMutationFuture_ = future.Apply(
        BIND(&TFollowerCommitter::OnMutationsLogged, MakeStrong(this), firstSequenceNumber, lastSequenceNumber)
            .AsyncVia(EpochContext_->EpochControlInvoker));
}

void TFollowerCommitter::OnMutationsLogged(
    i64 firstSequenceNumber,
    i64 lastSequenceNumber,
    const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!error.IsOK()) {
        LoggingFailed_.Fire(TError("Error logging mutations at follower")
            << error);
        return;
    }

    LastLoggedSequenceNumber_ = std::max(LastLoggedSequenceNumber_, lastSequenceNumber);

    YT_LOG_DEBUG("Mutations logged at follower (SequenceNumbers: %v-%v, LoggedSequenceNumber: %v)",
        firstSequenceNumber,
        lastSequenceNumber,
        LastLoggedSequenceNumber_);

    LoggingMutations_ = false;
}

TFuture<TFollowerCommitter::TCommitMutationsResult> TFollowerCommitter::CommitMutations(i64 committedSequenceNumber)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (committedSequenceNumber > CommittedSequenceNumber_) {
        YT_LOG_DEBUG("Committed sequence number promoted (CommittedSequenceNumber: %v -> %v)",
            CommittedSequenceNumber_,
            committedSequenceNumber);
        CommittedSequenceNumber_ = committedSequenceNumber;
    }

    std::vector<TPendingMutationPtr> mutations;
    while (!LoggedMutations_.empty()) {
        auto&& mutation = LoggedMutations_.front();
        if (mutation->SequenceNumber > CommittedSequenceNumber_) {
            break;
        }

        YT_VERIFY(!mutation->LocalCommitPromise);
        mutations.push_back(std::move(mutation));

        LoggedMutations_.pop();
    }

    if (mutations.empty()) {
        return {};
    }

    TCommitMutationsResult result{
        .FirstSequenceNumber = mutations.front()->SequenceNumber,
        .LastSequenceNumber = mutations.back()->SequenceNumber
    };

    YT_LOG_DEBUG("Committing mutations at follower (SequenceNumbers: %v-%v)",
        result.FirstSequenceNumber,
        result.LastSequenceNumber);

    CheckIfCaughtUp();

    return ScheduleApplyMutations(std::move(mutations))
        .Apply(BIND([=] { return result; }));
}

void TFollowerCommitter::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (const auto& [id, changelog] : NextChangelogs_) {
        CloseChangelog(changelog);
    }
    CloseChangelog(Changelog_);

    auto error = MakeStoppedError();
    CaughtUpPromise_.TrySet(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
