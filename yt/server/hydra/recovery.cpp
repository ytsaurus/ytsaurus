#include "recovery.h"
#include "changelog.h"
#include "changelog_download.h"
#include "config.h"
#include "decorated_automaton.h"
#include "snapshot.h"
#include "changelog_discovery.h"

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/hydra/hydra_manager.pb.h>
#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/response_keeper.h>

namespace NYT::NHydra {

using namespace NRpc;
using namespace NElection;
using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

TRecoveryBase::TRecoveryBase(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext,
    TVersion syncVersion)
    : Config_(config)
    , Options_(options)
    , CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , ChangelogStore_(changelogStore)
    , SnapshotStore_(snapshotStore)
    , ResponseKeeper_(responseKeeper)
    , EpochContext_(epochContext)
    , SyncVersion_(syncVersion)
    , Logger(HydraLogger)
{
    YCHECK(Config_);
    YCHECK(CellManager_);
    YCHECK(DecoratedAutomaton_);
    YCHECK(ChangelogStore_);
    YCHECK(SnapshotStore_);
    YCHECK(EpochContext_);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochSystemAutomatonInvoker, AutomatonThread);

    Logger.AddTag("CellId: %v", CellManager_->GetCellId());
}

void TRecoveryBase::RecoverToVersion(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(EpochContext_->ReachableVersion <= targetVersion);

    auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();
    YCHECK(currentVersion <= targetVersion);

    auto reachableVersion = EpochContext_->ReachableVersion;
    YCHECK(reachableVersion <= targetVersion);

    int snapshotId = InvalidSegmentId;
    if (targetVersion.SegmentId > currentVersion.SegmentId) {
        auto snapshotIdOrError = WaitFor(SnapshotStore_->GetLatestSnapshotId(targetVersion.SegmentId));
        THROW_ERROR_EXCEPTION_IF_FAILED(snapshotIdOrError, "Error computing the latest snapshot id");

        snapshotId = snapshotIdOrError.Value();
        YCHECK(snapshotId <= targetVersion.SegmentId);
    }

    YT_LOG_INFO("Recovering from version %v to version %v",
        currentVersion,
        targetVersion);

    TVersion snapshotVersion;
    ISnapshotReaderPtr snapshotReader;
    if (snapshotId != InvalidSegmentId) {
        snapshotReader = SnapshotStore_->CreateReader(snapshotId);

        WaitFor(snapshotReader->Open())
            .ThrowOnError();

        auto meta = snapshotReader->GetParams().Meta;
        snapshotVersion = TVersion(snapshotId - 1, meta.prev_record_count());
    }

    int initialChangelogId;
    if (snapshotVersion > currentVersion) {
        // Load the snapshot.
        YT_LOG_INFO("Using snapshot %v for recovery", snapshotId);

        if (ResponseKeeper_) {
            ResponseKeeper_->Stop();
        }

        DecoratedAutomaton_->LoadSnapshot(snapshotId, snapshotVersion, snapshotReader);
        initialChangelogId = snapshotId;
    } else {
        // Recover using changelogs only.
        YT_LOG_INFO("Not using snapshots for recovery");
        initialChangelogId = currentVersion.SegmentId;
    }

    // Shortcut for observer startup.
    if (targetVersion == TVersion() && !IsLeader() && !Options_.WriteChangelogsAtFollowers)
        return;

    YT_LOG_INFO("Replaying changelogs %v-%v to reach version %v",
        initialChangelogId,
        targetVersion.SegmentId,
        targetVersion);

    IChangelogPtr targetChangelog;
    for (int changelogId = initialChangelogId; changelogId <= targetVersion.SegmentId; ++changelogId) {
        auto changelog = WaitFor(ChangelogStore_->TryOpenChangelog(changelogId))
            .ValueOrThrow();

        if (!changelog) {
            if (!IsLeader() && !Options_.WriteChangelogsAtFollowers) {
                THROW_ERROR_EXCEPTION("Changelog %v is missing", changelogId);
            }

            auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();

            YT_LOG_INFO("Changelog %v is missing and will be created at version %v",
                changelogId,
                currentVersion);

            NProto::TChangelogMeta meta;
            meta.set_prev_record_count(currentVersion.RecordId);
            
            changelog = WaitFor(ChangelogStore_->CreateChangelog(changelogId, meta))
                .ValueOrThrow();
        }

        if (!IsLeader() && Options_.WriteChangelogsAtFollowers) {
            SyncChangelog(changelog, changelogId);
        }

        int targetRecordId = changelogId == targetVersion.SegmentId
            ? targetVersion.RecordId
            : changelog->GetRecordCount();
        ReplayChangelog(changelog, changelogId, targetRecordId);

        if (changelogId == targetVersion.SegmentId) {
            targetChangelog = changelog;
        }
    }

    YCHECK(targetChangelog);

    if (IsLeader() || Options_.WriteChangelogsAtFollowers) {
        YCHECK(targetChangelog->GetRecordCount() == targetVersion.RecordId);
        DecoratedAutomaton_->SetChangelog(targetChangelog);
    }

    DecoratedAutomaton_->SetLoggedVersion(targetVersion);
}

void TRecoveryBase::SyncChangelog(IChangelogPtr changelog, int changelogId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto channel = CellManager_->GetPeerChannel(EpochContext_->LeaderId);
    YCHECK(channel);

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
        YCHECK(syncRecordCount == remoteRecordCount);
        auto result = WaitFor(changelog->Truncate(remoteRecordCount));
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error truncating changelog");
    } else if (localRecordCount < syncRecordCount) {
        auto asyncResult = DownloadChangelog(
            Config_,
            CellManager_,
            ChangelogStore_,
            changelogId,
            syncRecordCount);
        auto result = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error downloading changelog records");
    }
}

void TRecoveryBase::ReplayChangelog(IChangelogPtr changelog, int changelogId, int targetRecordId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();
    YT_LOG_INFO("Replaying changelog %v from version %v to version %v",
        changelogId,
        currentVersion,
        TVersion(changelogId, targetRecordId));

    if (currentVersion.SegmentId != changelogId) {
        YCHECK(currentVersion.SegmentId == changelogId - 1);

        const auto& meta = changelog->GetMeta();
        YCHECK(meta.prev_record_count() == currentVersion.RecordId);

        // Prepare to apply mutations at the rotated version.
        DecoratedAutomaton_->RotateAutomatonVersion(changelogId);
    }

    if (changelog->GetRecordCount() < targetRecordId) {
        THROW_ERROR_EXCEPTION("Not enough records in changelog %v: needed %v, actual %v",
            changelogId,
            targetRecordId,
            changelog->GetRecordCount());
    }

    YT_LOG_INFO("Waiting for quorum record count to become sufficiently high");

    while (true) {
        auto asyncResult = ComputeChangelogQuorumInfo(
            Config_,
            CellManager_,
            changelogId,
            changelog->GetRecordCount());
        auto result = WaitFor(asyncResult)
            .ValueOrThrow();

        if (result.RecordCountLo >= targetRecordId) {
            YT_LOG_INFO("Quorum record count check succeeded");
            break;
        }

        YT_LOG_INFO("Quorum record count check failed; will retry");

        WaitFor(TDelayedExecutor::MakeDelayed(Config_->ChangelogQuorumCheckRetryPeriod))
            .ThrowOnError();
    }

    while (true) {
        int startRecordId = DecoratedAutomaton_->GetAutomatonVersion().RecordId;
        int recordsNeeded = targetRecordId - startRecordId;
        YCHECK(recordsNeeded >= 0);
        if (recordsNeeded == 0)
            break;
    
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
}

////////////////////////////////////////////////////////////////////////////////

TLeaderRecovery::TLeaderRecovery(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext)
    : TRecoveryBase(
        config,
        options,
        cellManager,
        decoratedAutomaton,
        changelogStore,
        snapshotStore,
        responseKeeper,
        epochContext,
        epochContext->ReachableVersion)
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

    NProfiling::TWallTimer timer;
    RecoverToVersion(EpochContext_->ReachableVersion);
    auto elapsedTime = timer.GetElapsedTime();

    if (Config_->DisableLeaderLeaseGraceDelay) {
        YT_LOG_WARNING("Leader lease grace delay disabled; cluster can only be used for testing purposes");
    } else if (elapsedTime < Config_->LeaderLeaseGraceDelay) {
        YT_LOG_INFO("Waiting for previous leader lease to expire");
        WaitFor(TDelayedExecutor::MakeDelayed(Config_->LeaderLeaseGraceDelay - elapsedTime))
            .ThrowOnError();
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
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext,
    TVersion syncVersion)
    : TRecoveryBase(
        config,
        options,
        cellManager,
        decoratedAutomaton,
        changelogStore,
        snapshotStore,
        responseKeeper,
        epochContext,
        syncVersion)
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
            switch (action.Tag()) {
                case TPostponedAction::TagOf<TPostponedMutation>():
                    DecoratedAutomaton_->LogFollowerMutation(action.As<TPostponedMutation>().RecordData, nullptr);
                    break;

                case TPostponedAction::TagOf<TPostponedChangelogRotation>():
                    WaitFor(DecoratedAutomaton_->RotateChangelog())
                        .ThrowOnError();
                    break;

                default:
                    Y_UNREACHABLE();
            }
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
