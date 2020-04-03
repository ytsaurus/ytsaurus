#include "recovery.h"
#include "changelog.h"
#include "changelog_download.h"
#include "config.h"
#include "decorated_automaton.h"
#include "snapshot.h"
#include "changelog_discovery.h"

#include <yt/ytlib/election/cell_manager.h>
#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hydra/proto/hydra_manager.pb.h>
#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/response_keeper.h>

#include <yt/core/misc/variant.h>

namespace NYT::NHydra {

using namespace NRpc;
using namespace NElection;
using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

TRecoveryBase::TRecoveryBase(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    const TDistributedHydraManagerDynamicOptions& dynamicOptions,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext,
    TVersion syncVersion,
    NLogging::TLogger logger)
    : Config_(std::move(config))
    , Options_(options)
    , DynamicOptions_(dynamicOptions)
    , DecoratedAutomaton_(std::move(decoratedAutomaton))
    , ChangelogStore_(std::move(changelogStore))
    , SnapshotStore_(std::move(snapshotStore))
    , ResponseKeeper_(std::move(responseKeeper))
    , EpochContext_(epochContext)
    , SyncVersion_(syncVersion)
    , Logger(std::move(logger))
{
    YT_VERIFY(Config_);
    YT_VERIFY(DecoratedAutomaton_);
    YT_VERIFY(ChangelogStore_);
    YT_VERIFY(SnapshotStore_);
    YT_VERIFY(EpochContext_);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochSystemAutomatonInvoker, AutomatonThread);
}

void TRecoveryBase::RecoverToVersion(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(EpochContext_->ReachableVersion <= targetVersion);

    auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();
    YT_VERIFY(currentVersion <= targetVersion);

    auto reachableVersion = EpochContext_->ReachableVersion;
    YT_VERIFY(reachableVersion <= targetVersion);

    int snapshotId = InvalidSegmentId;
    if (targetVersion.SegmentId > currentVersion.SegmentId) {
        auto snapshotIdOrError = WaitFor(SnapshotStore_->GetLatestSnapshotId(targetVersion.SegmentId));
        THROW_ERROR_EXCEPTION_IF_FAILED(snapshotIdOrError, "Error computing the latest snapshot id");

        snapshotId = snapshotIdOrError.Value();
        YT_VERIFY(snapshotId <= targetVersion.SegmentId);
    }

    YT_LOG_INFO("Recovering from version %v to version %v",
        currentVersion,
        targetVersion);

    TVersion snapshotVersion;
    i64 sequenceNumber;
    ui64 randomSeed;
    ISnapshotReaderPtr snapshotReader;
    if (snapshotId != InvalidSegmentId) {
        snapshotReader = SnapshotStore_->CreateReader(snapshotId);

        WaitFor(snapshotReader->Open())
            .ThrowOnError();

        auto meta = snapshotReader->GetParams().Meta;
        snapshotVersion = TVersion(snapshotId, 0);
        randomSeed = meta.random_seed();
        sequenceNumber = meta.sequence_number();
    }

    int initialChangelogId;
    if (snapshotVersion > currentVersion) {
        // Load the snapshot.
        YT_LOG_INFO("Using snapshot for recovery (SnapshotId: %v, SnapshotVersion: %v, CurrentVersion: %v)",
            snapshotId,
            snapshotVersion,
            currentVersion);

        if (ResponseKeeper_) {
            ResponseKeeper_->Stop();
        }

        DecoratedAutomaton_->LoadSnapshot(snapshotId, snapshotVersion, sequenceNumber, randomSeed, snapshotReader);
        initialChangelogId = snapshotId;
    } else {
        // Recover using changelogs only.
        YT_LOG_INFO("Not using snapshots for recovery (SnapshotVersion: %v, CurrentVersion: %v)",
            snapshotVersion,
            currentVersion);
        initialChangelogId = currentVersion.SegmentId;
    }

    // Shortcut for observer startup.
    if (targetVersion == TVersion() && !IsLeader() && !Options_.WriteChangelogsAtFollowers)
        return;

    YT_LOG_INFO("Replaying changelogs (ChangelogIds: %v-%v, TargetVersion: %v)",
        initialChangelogId,
        targetVersion.SegmentId,
        targetVersion);

    IChangelogPtr targetChangelog;
    int changelogId = initialChangelogId;
    while (changelogId <= targetVersion.SegmentId) {
        YT_LOG_INFO("Opening changelog (ChangelogId: %v)",
            changelogId);

        auto changelog = WaitFor(ChangelogStore_->TryOpenChangelog(changelogId))
            .ValueOrThrow();

        if (changelog) {
            YT_LOG_INFO("Changelog opened (ChangelogId: %v)",
                changelogId);
        } else {
            if (!IsLeader() && !Options_.WriteChangelogsAtFollowers) {
                THROW_ERROR_EXCEPTION("Changelog %v is missing", changelogId);
            }

            auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();
            YT_LOG_INFO("Changelog is missing and will be created (ChangelogId: %v, Version: %v)",
                changelogId,
                currentVersion);

            changelog = WaitFor(ChangelogStore_->CreateChangelog(changelogId))
                .ValueOrThrow();
        }

        if (!IsLeader() && Options_.WriteChangelogsAtFollowers) {
            SyncChangelog(changelog, changelogId);
        }

        int targetRecordId = changelogId == targetVersion.SegmentId
            ? targetVersion.RecordId
            : changelog->GetRecordCount();

        if (!ReplayChangelog(changelog, changelogId, targetRecordId)) {
            TDelayedExecutor::WaitForDuration(Config_->ChangelogRecordCountCheckRetryPeriod);
            continue;
        }

        if (changelogId == targetVersion.SegmentId) {
            targetChangelog = changelog;
        }

        ++changelogId;
    }

    YT_VERIFY(targetChangelog);

    if (IsLeader() || Options_.WriteChangelogsAtFollowers) {
        YT_VERIFY(targetChangelog->GetRecordCount() == targetVersion.RecordId);
        DecoratedAutomaton_->SetChangelog(targetChangelog);
    }

    DecoratedAutomaton_->SetLoggedVersion(targetVersion);
}

void TRecoveryBase::SyncChangelog(IChangelogPtr changelog, int changelogId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto channel = EpochContext_->CellManager->GetPeerChannel(EpochContext_->LeaderId);
    YT_VERIFY(channel);

    THydraServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

    auto req = proxy.LookupChangelog();
    req->set_changelog_id(changelogId);

    auto rspOrError = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting changelog %v info from leader",
        changelogId);
    const auto& rsp = rspOrError.Value();

    int remoteRecordCount = rsp->record_count();
    int localRecordCount = changelog->GetRecordCount();
    // NB: Don't download records past the sync point since they are expected to be postponed.
    int syncRecordCount = changelogId == SyncVersion_.SegmentId
        ? SyncVersion_.RecordId
        : remoteRecordCount;

    YT_LOG_INFO("Syncing changelog %v: local %v, remote %v, sync %v",
        changelogId,
        localRecordCount,
        remoteRecordCount,
        syncRecordCount);

    if (localRecordCount > remoteRecordCount) {
        YT_VERIFY(syncRecordCount == remoteRecordCount);
        auto result = WaitFor(changelog->Truncate(remoteRecordCount));
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error truncating changelog");
    } else if (localRecordCount < syncRecordCount) {
        auto asyncResult = DownloadChangelog(
            Config_,
            EpochContext_->CellManager,
            ChangelogStore_,
            changelogId,
            syncRecordCount);
        auto result = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error downloading changelog records");
    }
}

bool TRecoveryBase::ReplayChangelog(IChangelogPtr changelog, int changelogId, int targetRecordId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();

    YT_LOG_INFO("Replaying changelog %v from version %v to version %v",
        changelogId,
        currentVersion,
        TVersion(changelogId, targetRecordId));

    if (currentVersion.SegmentId != changelogId) {
        YT_VERIFY(currentVersion.SegmentId == changelogId - 1);

        // Prepare to apply mutations at the rotated version.
        DecoratedAutomaton_->RotateAutomatonVersion(changelogId);
    }

    YT_LOG_INFO("Checking changelog record count (ChangelogId: %v)",
        changelogId);

    if (changelog->GetRecordCount() < targetRecordId) {
        YT_LOG_INFO("Changelog record count is too low (ChangelogId: %v, NeededRecordCount: %v, ActualRecordCount: %v)",
            changelogId,
            targetRecordId,
            changelog->GetRecordCount());
        return false;
    }

    YT_LOG_INFO("Changelog record count is OK (ChangelogId: %v, NeededRecordCount: %v, ActualRecordCount: %v)",
        changelogId,
        targetRecordId,
        changelog->GetRecordCount());

    YT_LOG_INFO("Checking changelog quorum record count (ChangelogId: %v)",
        changelogId);

    auto asyncResult = ComputeChangelogQuorumInfo(
        Config_,
        EpochContext_->CellManager,
        changelogId,
        changelog->GetRecordCount());
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    if (result.RecordCountLo < targetRecordId) {
        YT_LOG_INFO("Changelog quorum record count is too low (ChangelogId: %v, NeededRecordCount: %v, ActualRecordCount: %v)",
            changelogId,
            targetRecordId,
            result.RecordCountLo);
        return false;
    }

    YT_LOG_INFO("Changelog quorum record count is OK (ChangelogId: %v, NeededRecordCount: %v, ActualRecordCount: %v)",
        changelogId,
        targetRecordId,
        result.RecordCountLo);

    while (true) {
        int startRecordId = DecoratedAutomaton_->GetAutomatonVersion().RecordId;
        int recordsNeeded = targetRecordId - startRecordId;
        YT_VERIFY(recordsNeeded >= 0);
        if (recordsNeeded == 0) {
            break;
        }

        YT_LOG_INFO("Trying to read records %v-%v from changelog %v",
            startRecordId,
            targetRecordId - 1,
            changelogId);

        auto asyncRecordsData = changelog->Read(
            startRecordId,
            recordsNeeded,
            Config_->MaxChangelogBytesPerRequest);
        auto recordsData = WaitFor(asyncRecordsData)
            .ValueOrThrow();
        int recordsRead = static_cast<int>(recordsData.size());

        YT_LOG_INFO("Finished reading records %v-%v from changelog %v",
            startRecordId,
            startRecordId + recordsRead - 1,
            changelogId);

        YT_LOG_INFO("Applying records %v-%v from changelog %v",
            startRecordId,
            startRecordId + recordsRead - 1,
            changelogId);

        for (const auto& data : recordsData)  {
            DecoratedAutomaton_->ApplyMutationDuringRecovery(data);
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TLeaderRecovery::TLeaderRecovery(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    const TDistributedHydraManagerDynamicOptions& dynamicOptions,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext,
    NLogging::TLogger logger)
    : TRecoveryBase(
        std::move(config),
        options,
        dynamicOptions,
        std::move(decoratedAutomaton),
        std::move(changelogStore),
        std::move(snapshotStore),
        std::move(responseKeeper),
        epochContext,
        epochContext->ReachableVersion,
        std::move(logger))
{ }

TFuture<void> TLeaderRecovery::Run()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND(&TLeaderRecovery::DoRun, MakeStrong(this))
        .AsyncVia(EpochContext_->EpochSystemAutomatonInvoker)
        .Run();
}

void TLeaderRecovery::DoRun()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (EpochContext_->ChangelogStore->IsReadOnly()) {
        THROW_ERROR_EXCEPTION("Cannot recover leader with a read-only changelog store");
    }

    NProfiling::TWallTimer timer;
    RecoverToVersion(EpochContext_->ReachableVersion);

    bool previousLeaderLeaseAbandoned = false;
    if (DynamicOptions_.AbandonLeaderLeaseDuringRecovery) {
        auto currentSegmentId = SyncVersion_.SegmentId;

        YT_LOG_INFO("Trying to abandon old leader lease (CurrentSegmentId: %v)",
            currentSegmentId);

        std::vector<TFuture<THydraServiceProxy::TRspAbandonLeaderLeasePtr>> futures;
        const auto& cellManager = EpochContext_->CellManager;
        for (int peerId = 0; peerId < cellManager->GetTotalPeerCount(); ++peerId) {
            auto peerChannel = cellManager->GetPeerChannel(peerId);
            if (!peerChannel) {
                YT_LOG_INFO("Peer channel is not configured (PeerId: %v)", peerId);
                continue;
            }

            THydraServiceProxy proxy(std::move(peerChannel));

            auto tryAbandonLeaderLeaseRequest = proxy.AbandonLeaderLease();
            tryAbandonLeaderLeaseRequest->SetTimeout(Config_->AbandonLeaderLeaseRequestTimeout);
            tryAbandonLeaderLeaseRequest->set_segment_id(SyncVersion_.SegmentId);
            tryAbandonLeaderLeaseRequest->set_peer_id(cellManager->GetSelfPeerId());
            futures.push_back(tryAbandonLeaderLeaseRequest->Invoke());
        }

        auto rspsOrError = WaitFor(CombineAll(futures));
        if (rspsOrError.IsOK()) {
            const auto& rsps = rspsOrError.Value();
            for (int peerId = 0; peerId < rsps.size(); ++peerId) {
                const auto& rspOrError = rsps[peerId];
                if (rspOrError.IsOK()) {
                    auto peerLastLeadingSegmentId = rspOrError.Value()->last_leading_segment_id();
                    YT_LOG_INFO("Peer response received (PeerId: %v, LastLeadingSegmentId: %v)",
                        peerId,
                        peerLastLeadingSegmentId);

                    if (peerLastLeadingSegmentId + 1 == currentSegmentId) {
                        YT_LOG_INFO("Previous leader lease abandoned (PeerId: %v)", peerId);
                        YT_VERIFY(!previousLeaderLeaseAbandoned);
                        previousLeaderLeaseAbandoned = true;
                        break;
                    }
                } else {
                    YT_LOG_INFO(rspOrError, "Failed to abandon peer leader lease (PeerId: %v)", peerId);
                }
            }
        } else {
            YT_LOG_INFO(rspsOrError, "Failed to abandon leader lease");
        }
    }

    auto elapsedTime = timer.GetElapsedTime();

    if (previousLeaderLeaseAbandoned) {
        YT_LOG_INFO("Ignoring leader lease grace delay");
    } else if (Config_->DisableLeaderLeaseGraceDelay) {
        YT_LOG_WARNING("Leader lease grace delay disabled; cluster can only be used for testing purposes");
    } else if (elapsedTime < Config_->LeaderLeaseGraceDelay) {
        YT_LOG_INFO("Waiting for previous leader lease to expire");
        TDelayedExecutor::WaitForDuration(Config_->LeaderLeaseGraceDelay - elapsedTime);
    } else {
        YT_LOG_INFO("Leader lease grace delay already expired");
    }
}

bool TLeaderRecovery::IsLeader() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TFollowerRecovery::TFollowerRecovery(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    const TDistributedHydraManagerDynamicOptions& dynamicOptions,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext,
    TVersion syncVersion,
    NLogging::TLogger logger)
    : TRecoveryBase(
        std::move(config),
        options,
        dynamicOptions,
        std::move(decoratedAutomaton),
        std::move(changelogStore),
        std::move(snapshotStore),
        std::move(responseKeeper),
        epochContext,
        syncVersion,
        std::move(logger))
    , PostponedVersion_(syncVersion)
    , CommittedVersion_(syncVersion)
{ }

TFuture<void> TFollowerRecovery::Run()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND(&TFollowerRecovery::DoRun, MakeStrong(this))
        .AsyncVia(EpochContext_->EpochSystemAutomatonInvoker)
        .Run();
}

void TFollowerRecovery::DoRun()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    RecoverToVersion(SyncVersion_);

    YT_LOG_INFO("Checkpoint reached; started catching up with leader");

    while (true) {
        TVersion committedVersion;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            committedVersion = CommittedVersion_;
        }

        DecoratedAutomaton_->CommitMutations(committedVersion, false);

        decltype(PostponedActions_) postponedActions;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            postponedActions.swap(PostponedActions_);
            if (postponedActions.empty() && !DecoratedAutomaton_->HasReadyMutations()) {
                YT_LOG_INFO("No more postponed actions accepted");
                NoMorePostponedActions_ = true;
                break;
            }
        }

        YT_LOG_INFO("Logging postponed actions (ActionCount: %v)",
            postponedActions.size());

        for (const auto& action : postponedActions) {
            Visit(action,
                [&] (const TPostponedMutation& mutation) {
                    DecoratedAutomaton_->LogFollowerMutation(mutation.RecordData, nullptr);
                },
                [&] (TPostponedChangelogRotation) {
                    WaitFor(DecoratedAutomaton_->RotateChangelog())
                        .ThrowOnError();
                });
        }
    }

    YT_LOG_INFO("Finished catching up with leader");
}

bool TFollowerRecovery::PostponeChangelogRotation(TVersion version)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);

    if (NoMorePostponedActions_) {
        return false;
    }

    if (PostponedVersion_ > version) {
        YT_LOG_DEBUG("Late changelog rotation received during recovery, ignored: expected %v, received %v",
            PostponedVersion_,
            version);
        return true;
    }

    if (PostponedVersion_ < version) {
        THROW_ERROR_EXCEPTION("Out-of-order changelog rotation received during recovery: expected %v, received %v",
            PostponedVersion_,
            version);
    }

    PostponedActions_.push_back(TPostponedChangelogRotation());

    YT_LOG_INFO("Postponing changelog rotation at version %v",
        PostponedVersion_);

    PostponedVersion_ = PostponedVersion_.Rotate();

    return true;
}

bool TFollowerRecovery::PostponeMutations(
    TVersion version,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);

    if (NoMorePostponedActions_) {
        return false;
    }

    if (PostponedVersion_ > version) {
        YT_LOG_DEBUG("Late mutations received during recovery, ignored: expected %v, received %v",
            PostponedVersion_,
            version);
        return true;
    }

    if (PostponedVersion_ != version) {
        THROW_ERROR_EXCEPTION("Out-of-order mutations received during recovery: expected %v, received %v",
            PostponedVersion_,
            version);
    }

    YT_LOG_DEBUG("Mutations postponed (StartVersion: %v, MutationCount: %v)",
        PostponedVersion_,
        recordsData.size());

    for (const auto& data : recordsData) {
        PostponedActions_.push_back(TPostponedMutation{data});
    }

    PostponedVersion_ = PostponedVersion_.Advance(recordsData.size());

    return true;
}

void TFollowerRecovery::SetCommittedVersion(TVersion version)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);

    CommittedVersion_ = std::max(CommittedVersion_, version);
}

bool TFollowerRecovery::IsLeader() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
