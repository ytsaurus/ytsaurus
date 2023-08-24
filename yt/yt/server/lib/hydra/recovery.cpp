#include "recovery.h"
#include "changelog_discovery.h"
#include "changelog_download.h"
#include "config.h"
#include "decorated_automaton.h"
#include "hydra_service_proxy.h"
#include "snapshot_discovery.h"
#include "snapshot_download.h"

#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/local_snapshot_store.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <library/cpp/yt/misc/variant.h>

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
    IResponseKeeperPtr responseKeeper,
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

    auto epochContext = EpochContext_;

    YT_VERIFY(epochContext->ReachableVersion <= targetVersion);

    auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();
    if (currentVersion > targetVersion) {
        // NB: YT-14934, rollback is possible at observers but not at leaders.
        if (IsLeader()) {
            YT_LOG_FATAL("Current automaton version is greater than target version during leader recovery "
                "(CurrentVersion: %v, TargetVersion: %v)",
                currentVersion,
                targetVersion);
        } else {
            THROW_ERROR_EXCEPTION("Error recovering to version %v: current automaton version %v is greater",
                targetVersion,
                currentVersion);
        }
    }

    auto reachableVersion = epochContext->ReachableVersion;
    YT_VERIFY(reachableVersion <= targetVersion);

    int snapshotId = InvalidSegmentId;
    if (targetVersion.SegmentId > currentVersion.SegmentId) {
        auto snapshotParamsOrError = WaitFor(
            DiscoverLatestSnapshot(Config_, epochContext->CellManager, targetVersion.SegmentId));
        THROW_ERROR_EXCEPTION_IF_FAILED(snapshotParamsOrError, "Error computing the latest snapshot id");
        auto localLatestSnapshotId = WaitFor(SnapshotStore_->GetLatestSnapshotId())
            .ValueOrThrow();
        snapshotId = std::max(snapshotParamsOrError.Value().SnapshotId, localLatestSnapshotId);
    }

    YT_LOG_INFO("Recovering from version %v to version %v",
        currentVersion,
        targetVersion);

    TVersion snapshotVersion;
    if (snapshotId != InvalidSegmentId) {
        snapshotVersion = TVersion(snapshotId, 0);
    }

    int initialChangelogId;
    if (snapshotVersion > currentVersion) {
        // Load the snapshot.
        YT_LOG_INFO("Using snapshot for recovery (SnapshotId: %v, SnapshotVersion: %v, CurrentVersion: %v)",
            snapshotId,
            snapshotVersion,
            currentVersion);

        YT_VERIFY(snapshotId != InvalidSegmentId);

        auto createAndOpenReaderOrThrow = [&] {
            auto reader = SnapshotStore_->CreateReader(snapshotId);
            WaitFor(reader->Open())
                .ThrowOnError();
            return reader;
        };

        ISnapshotReaderPtr snapshotReader;
        try {
            snapshotReader = createAndOpenReaderOrThrow();
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(EErrorCode::NoSuchSnapshot)) {
                if (auto legacySnapshotStore = DynamicPointerCast<ILegacySnapshotStore>(SnapshotStore_)) {
                    YT_LOG_INFO(ex, "Snapshot is missing; attempting to download (SnapshotId: %v)",
                        snapshotId);
                    WaitFor(DownloadSnapshot(
                        Config_,
                        epochContext->CellManager,
                        std::move(legacySnapshotStore),
                        snapshotId,
                        Logger))
                        .ThrowOnError();
                    snapshotReader = createAndOpenReaderOrThrow();
                } else {
                    // Snapshot downloading is not supported by remote snapshot store in old Hydra.
                    YT_LOG_INFO(ex, "Snapshot is missing, and downloading is not supported (SnapshotId: %v)", snapshotId);
                    throw;
                }
            } else {
                YT_LOG_INFO(ex, "Failed to open snapshot (SnapshotId: %v)", snapshotId);
                throw;
            }
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Failed to open snapshot (SnapshotId: %v)", snapshotId);
            throw;
        }

        auto meta = snapshotReader->GetParams().Meta;
        auto randomSeed = meta.random_seed();
        auto sequenceNumber = meta.sequence_number();
        auto stateHash = meta.state_hash();
        auto timestamp = FromProto<TInstant>(meta.timestamp());

        if (ResponseKeeper_) {
            ResponseKeeper_->Stop();
        }

        DecoratedAutomaton_->LoadSnapshot(
            snapshotId,
            snapshotVersion,
            sequenceNumber,
            randomSeed,
            stateHash,
            timestamp,
            snapshotReader);
        initialChangelogId = snapshotId;
    } else {
        // Recover using changelogs only.
        YT_LOG_INFO("Not using snapshots for recovery (SnapshotVersion: %v, CurrentVersion: %v)",
            snapshotVersion,
            currentVersion);
        initialChangelogId = currentVersion.SegmentId;
    }

    // Shortcut for observer startup.
    if (targetVersion == TVersion() && !IsLeader() && !Options_.EnableObserverPersistence)
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
            if (!IsLeader() && !Options_.EnableObserverPersistence) {
                THROW_ERROR_EXCEPTION("Changelog %v is missing", changelogId);
            }

            auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();
            YT_LOG_INFO("Changelog is missing and will be created (ChangelogId: %v, Version: %v)",
                changelogId,
                currentVersion);

            changelog = WaitFor(ChangelogStore_->CreateChangelog(changelogId, /* meta */ {}))
                .ValueOrThrow();
        }

        if (!IsLeader() && Options_.EnableObserverPersistence) {
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

    if (IsLeader() || Options_.EnableObserverPersistence) {
        YT_VERIFY(targetChangelog);

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

    TLegacyHydraServiceProxy proxy(channel);
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
    IResponseKeeperPtr responseKeeper,
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

    RecoverToVersion(EpochContext_->ReachableVersion);
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
    IResponseKeeperPtr responseKeeper,
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
            auto guard = Guard(SpinLock_);
            committedVersion = CommittedVersion_;
        }

        DecoratedAutomaton_->CommitMutations(committedVersion, false);

        decltype(PostponedActions_) postponedActions;
        {
            auto guard = Guard(SpinLock_);
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

    auto guard = Guard(SpinLock_);

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

    auto guard = Guard(SpinLock_);

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

    auto guard = Guard(SpinLock_);

    CommittedVersion_ = std::max(CommittedVersion_, version);
}

bool TFollowerRecovery::IsLeader() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
