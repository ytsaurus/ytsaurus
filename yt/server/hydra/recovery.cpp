#include "stdafx.h"
#include "recovery.h"
#include "config.h"
#include "decorated_automaton.h"
#include "changelog.h"
#include "snapshot.h"
#include "changelog_download.h"

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

#include <core/rpc/response_keeper.h>

#include <core/profiling/scoped_timer.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>
#include <ytlib/hydra/hydra_manager.pb.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;
using namespace NElection;
using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

TRecoveryBase::TRecoveryBase(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext)
    : Config_(config)
    , CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , ChangelogStore_(changelogStore)
    , SnapshotStore_(snapshotStore)
    , ResponseKeeper_(responseKeeper)
    , EpochContext_(epochContext)
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

    auto snapshotIdOrError = WaitFor(SnapshotStore_->GetLatestSnapshotId(targetVersion.SegmentId));
    THROW_ERROR_EXCEPTION_IF_FAILED(snapshotIdOrError, "Error computing the latest snapshot id");

    int snapshotId = snapshotIdOrError.Value();
    YCHECK(snapshotId <= targetVersion.SegmentId);

    auto currentVersion = DecoratedAutomaton_->GetCommittedVersion();
    YCHECK(currentVersion <= targetVersion);

    LOG_INFO("Recoverying from version %v to version %v",
        currentVersion,
        targetVersion);

    int initialChangelogId;
    if (snapshotId != InvalidSegmentId && snapshotId > currentVersion.SegmentId) {
        // Load the snapshot.
        LOG_INFO("Using snapshot %v for recovery", snapshotId);

        if (ResponseKeeper_) {
            ResponseKeeper_->Stop();
        }

        auto reader = SnapshotStore_->CreateReader(snapshotId);

        WaitFor(reader->Open())
            .ThrowOnError();

        auto meta = reader->GetParams().Meta;
        auto snapshotVersion = TVersion(snapshotId - 1, meta.prev_record_count());

        DecoratedAutomaton_->LoadSnapshot(snapshotVersion, reader);

        initialChangelogId = snapshotId;
    } else {
        // Recover using changelogs only.
        LOG_INFO("Not using snapshots for recovery");
        initialChangelogId = currentVersion.SegmentId;
    }

    LOG_INFO("Replaying changelogs %v-%v to reach version %v",
        initialChangelogId,
        targetVersion.SegmentId,
        targetVersion);

    for (int changelogId = initialChangelogId; changelogId <= targetVersion.SegmentId; ++changelogId) {
        bool isLastChangelog = (changelogId == targetVersion.SegmentId);
        auto changelog = WaitFor(ChangelogStore_->TryOpenChangelog(changelogId))
            .ValueOrThrow();
        if (!changelog) {
            auto currentVersion = DecoratedAutomaton_->GetCommittedVersion();

            LOG_INFO("Changelog %v is missing and will be created at version %v",
                changelogId,
                currentVersion);

            NProto::TChangelogMeta meta;
            meta.set_prev_record_count(currentVersion.RecordId);
            
            changelog = WaitFor(ChangelogStore_->CreateChangelog(changelogId, meta))
                .ValueOrThrow();

            DecoratedAutomaton_->SetLoggedVersion(std::max(
                DecoratedAutomaton_->GetLoggedVersion(),
                TVersion(changelogId, 0)));
        }

        DecoratedAutomaton_->SetChangelog(changelog);

        if (!IsLeader()) {
            SyncChangelog(changelog, changelogId);
        }

        WaitFor(changelog->Flush())
            .ThrowOnError();

        int targetRecordId = isLastChangelog ? targetVersion.RecordId : changelog->GetRecordCount();
        ReplayChangelog(changelog, changelogId, targetRecordId);
    }
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

    LOG_INFO("Syncing changelog %v: local %v, remote %v, sync %v",
        changelogId,
        localRecordCount,
        remoteRecordCount,
        syncRecordCount);

    if (localRecordCount > remoteRecordCount) {
        YCHECK(syncRecordCount == remoteRecordCount);
        WaitFor(changelog->Truncate(remoteRecordCount))
            .ThrowOnError();

        if (DecoratedAutomaton_->GetLoggedVersion().SegmentId == changelogId) {
            YCHECK(DecoratedAutomaton_->GetLoggedVersion().RecordId >= remoteRecordCount);
            DecoratedAutomaton_->SetLoggedVersion(TVersion(changelogId, remoteRecordCount));
        }
    } else if (localRecordCount < syncRecordCount) {
        auto asyncResult = DownloadChangelog(
            Config_,
            CellManager_,
            ChangelogStore_,
            changelogId,
            syncRecordCount);
        auto result = WaitFor(asyncResult);

        DecoratedAutomaton_->SetLoggedVersion(std::max(
            DecoratedAutomaton_->GetLoggedVersion(),
            TVersion(changelogId, syncRecordCount)));

        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error downloading changelog records");
    }
}

void TRecoveryBase::ReplayChangelog(IChangelogPtr changelog, int changelogId, int targetRecordId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto currentVersion = DecoratedAutomaton_->GetCommittedVersion();
    LOG_INFO("Replaying changelog %v from version %v to version %v",
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
        LOG_FATAL("Not enough records in changelog %v: needed %v, actual %v",
            changelogId,
            targetRecordId,
            changelog->GetRecordCount());
    }

    while (true) {
        int startRecordId = DecoratedAutomaton_->GetCommittedVersion().RecordId;
        int recordsNeeded = targetRecordId - startRecordId;
        YCHECK(recordsNeeded >= 0);
        if (recordsNeeded == 0)
            break;
    
        LOG_INFO("Trying to read records %v-%v from changelog %v",
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

        LOG_INFO("Finished reading records %v-%v from changelog %v",
            startRecordId,
            startRecordId + recordsRead - 1,
            changelogId);

        LOG_INFO("Applying records %v-%v from changelog %v",
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
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext)
    : TRecoveryBase(
        config,
        cellManager,
        decoratedAutomaton,
        changelogStore,
        snapshotStore,
        responseKeeper,
        epochContext)
{ }

TFuture<void> TLeaderRecovery::Run(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY_ANY();
    
    SyncVersion_ = targetVersion;
    return BIND(&TLeaderRecovery::DoRun, MakeStrong(this))
        .AsyncVia(EpochContext_->EpochSystemAutomatonInvoker)
        .Run(targetVersion);
}

void TLeaderRecovery::DoRun(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    NProfiling::TScopedTimer timer;
    RecoverToVersion(targetVersion);
    auto elapsedTime = timer.GetElapsed();

    if (Config_->DisableLeaderLeaseGraceDelay) {
        LOG_WARNING("Leader lease grace delay disabled; cluster can only be used for testing purposes");
    } else if (elapsedTime < Config_->LeaderLeaseGraceDelay) {
        LOG_INFO("Waiting for previous leader lease to expire");
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
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext,
    TVersion syncVersion)
    : TRecoveryBase(
        config,
        cellManager,
        decoratedAutomaton,
        changelogStore,
        snapshotStore,
        responseKeeper,
        epochContext)
{
    SyncVersion_ = PostponedVersion_ = syncVersion;
}

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

    LOG_INFO("Checkpoint reached");

    while (true) {
        TPostponedMutations mutations;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (PostponedMutations_.empty()) {
                break;
            }
            mutations.swap(PostponedMutations_);
        }

        LOG_INFO("Logging %v postponed mutations",
            mutations.size());

        for (const auto& mutation : mutations) {
            switch (mutation.Type) {
                case TPostponedMutation::EType::Mutation:
                    DecoratedAutomaton_->LogFollowerMutation(mutation.RecordData, nullptr);
                    break;

                case TPostponedMutation::EType::ChangelogRotation: {
                    WaitFor(DecoratedAutomaton_->RotateChangelog(EpochContext_))
                        .ThrowOnError();
                    break;
                }

                default:
                    YUNREACHABLE();
            }
        }
    }

    LOG_INFO("Finished logging postponed mutations");
}

void TFollowerRecovery::PostponeChangelogRotation(TVersion version)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);

    if (PostponedVersion_ > version) {
        LOG_DEBUG("Late changelog rotation received during recovery, ignored: expected %v, received %v",
            PostponedVersion_,
            version);
        return;
    }

    if (PostponedVersion_ < version) {
        THROW_ERROR_EXCEPTION("Out-of-order changelog rotation received during recovery: expected %v, received %v",
            PostponedVersion_,
            version);
    }

    PostponedMutations_.push_back(TPostponedMutation::CreateChangelogRotation());

    LOG_INFO("Postponing changelog rotation at version %v",
        PostponedVersion_);

    PostponedVersion_ = PostponedVersion_.Rotate();
}

void TFollowerRecovery::PostponeMutations(
    TVersion version,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);

    if (PostponedVersion_ > version) {
        LOG_WARNING("Late mutations received during recovery, ignored: expected %v, received %v",
            PostponedVersion_,
            version);
        return;
    }

    if (PostponedVersion_ != version) {
        THROW_ERROR_EXCEPTION("Out-of-order mutations received during recovery: expected %v, received %v",
            PostponedVersion_,
            version);
    }

    LOG_DEBUG("Postponing %v mutations at version %v",
        recordsData.size(),
        PostponedVersion_);

    for (const auto& data : recordsData) {
        PostponedMutations_.push_back(TPostponedMutation::CreateMutation(data));
    }

    PostponedVersion_ = PostponedVersion_.Advance(recordsData.size());
}

bool TFollowerRecovery::IsLeader() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
