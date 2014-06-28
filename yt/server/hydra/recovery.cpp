#include "stdafx.h"
#include "recovery.h"
#include "config.h"
#include "decorated_automaton.h"
#include "changelog.h"
#include "snapshot.h"
#include "changelog_download.h"

#include <core/concurrency/scheduler.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>
#include <ytlib/hydra/hydra_manager.pb.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TRecovery::TRecovery(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TEpochContextPtr epochContext)
    : Config_(config)
    , CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , ChangelogStore_(changelogStore)
    , SnapshotStore_(snapshotStore)
    , EpochContext_(epochContext)
    , Logger(HydraLogger)
{
    YCHECK(Config_);
    YCHECK(CellManager_);
    YCHECK(DecoratedAutomaton_);
    YCHECK(ChangelogStore_);
    YCHECK(SnapshotStore_);
    YCHECK(EpochContext_);
    VERIFY_INVOKER_AFFINITY(EpochContext_->EpochSystemAutomatonInvoker, AutomatonThread);

    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(CellManager_->GetCellGuid())));
}

void TRecovery::RecoverToVersion(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto latestSnapshotIdOrError = WaitFor(SnapshotStore_->GetLatestSnapshotId(targetVersion.SegmentId));
    THROW_ERROR_EXCEPTION_IF_FAILED(latestSnapshotIdOrError, "Error computing the latest snapshot id");
    int latestSnapshotId = latestSnapshotIdOrError.Value();

    RecoverToVersionWithSnapshot(targetVersion, latestSnapshotId);
}

void TRecovery::RecoverToVersionWithSnapshot(TVersion targetVersion, int snapshotId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();
    YCHECK(snapshotId <= targetVersion.SegmentId);
    YCHECK(currentVersion <= targetVersion);

    LOG_INFO("Recoverying from %s to %s",
        ~ToString(currentVersion),
        ~ToString(targetVersion));

    if (snapshotId != NonexistingSegmentId && snapshotId > currentVersion.SegmentId) {
        // Load the snapshot.
        LOG_DEBUG("Using snapshot %d for recovery", snapshotId);

        auto readerOrError = WaitFor(SnapshotStore_->CreateReader(snapshotId));
        THROW_ERROR_EXCEPTION_IF_FAILED(readerOrError, "Error creating snapshot reader");
        auto reader = readerOrError.Value();

        DecoratedAutomaton_->LoadSnapshot(snapshotId, reader->GetStream());

        currentVersion = TVersion(snapshotId, 0);
    } else {
        // Recover using changelogs only.
        LOG_INFO("Not using any snapshot for recovery");
    }

    int prevRecordCount = ComputePrevRecordCount(currentVersion.SegmentId);
    ReplayChangelogs(targetVersion, prevRecordCount);
}

void TRecovery::ReplayChangelogs(TVersion targetVersion, int expectedPrevRecordCount)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    LOG_INFO("Replaying changelogs to reach %s",
        ~ToString(targetVersion));

    auto initialVersion = DecoratedAutomaton_->GetAutomatonVersion();
    for (int changelogId = initialVersion.SegmentId;
         changelogId <= targetVersion.SegmentId;
         ++changelogId)
    {
        bool isLast = changelogId == targetVersion.SegmentId;
        auto changelogOrError = WaitFor(ChangelogStore_->TryOpenChangelog(changelogId));
        THROW_ERROR_EXCEPTION_IF_FAILED(changelogOrError);
        auto changelog = changelogOrError.Value();
        if (!changelog) {
            LOG_INFO("Changelog %d is missing and will be created",
                changelogId);

            YCHECK(expectedPrevRecordCount != UnknownPrevRecordCount);

            NProto::TChangelogMeta meta;
            meta.set_prev_record_count(expectedPrevRecordCount);
            
            TSharedRef metaBlob;
            YCHECK(SerializeToProto(meta, &metaBlob));

            auto changelogOrError = WaitFor(ChangelogStore_->CreateChangelog(changelogId, metaBlob));
            THROW_ERROR_EXCEPTION_IF_FAILED(changelogOrError);
            changelog = changelogOrError.Value();

            TVersion newLoggedVersion(changelogId, 0);
            // NB: Equality is only possible when segmentId == 0.
            YCHECK(DecoratedAutomaton_->GetLoggedVersion() <= newLoggedVersion);
            DecoratedAutomaton_->SetLoggedVersion(newLoggedVersion);
        }

        NProto::TChangelogMeta meta;
        YCHECK(DeserializeFromProto(&meta, changelog->GetMeta()));

        LOG_FATAL_IF(
            expectedPrevRecordCount != UnknownPrevRecordCount &&
            meta.prev_record_count() != expectedPrevRecordCount,
            "PrevRecordCount mismatch in changelog %d: expected: %d, found %d",
            changelogId,
            expectedPrevRecordCount,
            meta.prev_record_count());

        if (!IsLeader()) {
            SyncChangelog(changelog, changelogId);
        }

        if (!isLast && !changelog->IsSealed()) {
            WaitFor(changelog->Flush());
            if (changelog->IsSealed()) {
                LOG_WARNING("Changelog %d is already sealed",
                    changelogId);
            } else {
                WaitFor(changelog->Seal(changelog->GetRecordCount()));
            }
        }

        // Check if the current state contains some mutations that are redundant.
        // If so, clear the state and restart recovery.
        auto currentVersion = DecoratedAutomaton_->GetAutomatonVersion();
        YCHECK(currentVersion.SegmentId == changelogId);
        if (currentVersion.RecordId > changelog->GetRecordCount()) {
            DecoratedAutomaton_->Clear();
            THROW_ERROR_EXCEPTION("Current version is %s while only %d mutations are expected in this segment, forcing clean restart",
                ~ToString(currentVersion),
                changelog->GetRecordCount());
        }

        int targetRecordId = isLast ? targetVersion.RecordId : changelog->GetRecordCount();
        ReplayChangelog(changelog, changelogId, targetRecordId);

        if (!isLast) {
            DecoratedAutomaton_->RotateChangelogDuringRecovery();
        }

        expectedPrevRecordCount = changelog->GetRecordCount();
    }

    YCHECK(DecoratedAutomaton_->GetAutomatonVersion() == targetVersion);
}

void TRecovery::SyncChangelog(IChangelogPtr changelog, int changelogId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    THydraServiceProxy proxy(CellManager_->GetPeerChannel(EpochContext_->LeaderId));
    proxy.SetDefaultTimeout(Config_->RpcTimeout);

    auto req = proxy.LookupChangelog();
    req->set_changelog_id(changelogId);

    auto rsp = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting changelog %d info from leader",
        changelogId);

    int remoteRecordCount = rsp->record_count();
    int localRecordCount = changelog->GetRecordCount();
    // NB: Don't download records past the sync point since they are expected to be postponed.
    int syncRecordCount =
        changelogId == SyncVersion_.SegmentId
        ? SyncVersion_.RecordId
        : remoteRecordCount;

    LOG_INFO("Syncing changelog %d: local %d, remote %d, sync %d",
        changelogId,
        localRecordCount,
        remoteRecordCount,
        syncRecordCount);

    if (localRecordCount > remoteRecordCount) {
        YCHECK(syncRecordCount == remoteRecordCount);
        if (changelog->IsSealed()) {
            LOG_FATAL("Cannot truncate a sealed changelog %d",
                changelogId);
        } else {
            WaitFor(changelog->Seal(remoteRecordCount));

            TVersion sealedVersion(changelogId, remoteRecordCount);
            if (DecoratedAutomaton_->GetLoggedVersion().SegmentId == sealedVersion.SegmentId) {
                DecoratedAutomaton_->SetLoggedVersion(sealedVersion);
            }
        }
    } else if (localRecordCount < syncRecordCount) {
        auto asyncResult = DownloadChangelog(
            Config_,
            CellManager_,
            ChangelogStore_,
            changelogId,
            syncRecordCount);
        auto result = WaitFor(asyncResult);

        TVersion downloadedVersion(changelogId, changelog->GetRecordCount());
        DecoratedAutomaton_->SetLoggedVersion(std::max(DecoratedAutomaton_->GetLoggedVersion(), downloadedVersion));

        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error downloading changelog records");
    }
}

void TRecovery::ReplayChangelog(IChangelogPtr changelog, int changelogId, int targetRecordId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(DecoratedAutomaton_->GetAutomatonVersion().SegmentId == changelogId);

    if (changelog->GetRecordCount() < targetRecordId) {
        LOG_FATAL("Not enough records in changelog %d: expected %d, actual %d",
            changelogId,
            targetRecordId,
            changelog->GetRecordCount());
    }

    while (true) {
        int startRecordId = DecoratedAutomaton_->GetAutomatonVersion().RecordId;
        int recordsNeeded = targetRecordId - startRecordId;
        YCHECK(recordsNeeded >= 0);
        if (recordsNeeded == 0)
            break;
    
        LOG_INFO("Trying to read records %d-%d from changelog %d",
            startRecordId,
            targetRecordId - 1,
            changelogId);

        auto recordsData = changelog->Read(
            startRecordId,
            recordsNeeded,
            Config_->MaxChangelogReadSize);
        int recordsRead = static_cast<int>(recordsData.size());

        LOG_INFO("Finished reading records %d-%d from changelog %d",
            startRecordId,
            startRecordId + recordsRead - 1,
            changelogId);

        LOG_INFO("Applying records %d-%d from changelog %d",
            startRecordId,
            startRecordId + recordsRead - 1,
            changelogId);

        for (const auto& data : recordsData)  {
            DecoratedAutomaton_->ApplyMutationDuringRecovery(data);
        }
    }
}

int TRecovery::ComputePrevRecordCount(int segmentId)
{
    YCHECK(segmentId >= 0);

    if (segmentId == 0) {
        return NonexistingSegmentId;
    }

    // Extract from changelog.
    auto changelogOrError = WaitFor(ChangelogStore_->TryOpenChangelog(segmentId - 1));
    THROW_ERROR_EXCEPTION_IF_FAILED(changelogOrError);
    auto changelog = changelogOrError.Value();
    if (changelog) {
        return changelog->GetRecordCount();
    }

    // Extract from snapshot.
    auto snapshotParamsOrError = WaitFor(SnapshotStore_->GetSnapshotParams(segmentId));
    THROW_ERROR_EXCEPTION_IF_FAILED(snapshotParamsOrError, "Error getting snapshot parameters");
    const auto& snapshotParams = snapshotParamsOrError.Value();
    return snapshotParams.PrevRecordCount;
}

////////////////////////////////////////////////////////////////////////////////

TLeaderRecovery::TLeaderRecovery(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    TEpochContextPtr epochContext)
    : TRecovery(
        config,
        cellManager,
        decoratedAutomaton,
        changelogStore,
        snapshotStore,
        epochContext)
{ }

TAsyncError TLeaderRecovery::Run(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY_ANY();
    
    SyncVersion_ = targetVersion;
    return BIND(&TLeaderRecovery::DoRun, MakeStrong(this))
        .Guarded()
        .AsyncVia(EpochContext_->EpochSystemAutomatonInvoker)
        .Run(targetVersion);
}

void TLeaderRecovery::DoRun(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto latestSnapshotIdOrError = WaitFor(SnapshotStore_->GetLatestSnapshotId(targetVersion.SegmentId));
    THROW_ERROR_EXCEPTION_IF_FAILED(latestSnapshotIdOrError, "Error computing the latest snapshot id");
    int latestSnapshotId = latestSnapshotIdOrError.Value();

    RecoverToVersionWithSnapshot(targetVersion, latestSnapshotId);
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
    TEpochContextPtr epochContext,
    TVersion syncVersion)
    : TRecovery(
        config,
        cellManager,
        decoratedAutomaton,
        changelogStore,
        snapshotStore,
        epochContext)
{
    SyncVersion_ = PostponedVersion = syncVersion;
}

TAsyncError TFollowerRecovery::Run()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND(&TFollowerRecovery::DoRun, MakeStrong(this))
        .Guarded()
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
            TGuard<TSpinLock> guard(SpinLock);
            if (PostponedMutations.empty()) {
                break;
            }
            mutations.swap(PostponedMutations);
        }

        LOG_INFO("Logging %" PRISZT " postponed mutations",
            mutations.size());

        for (const auto& mutation : mutations) {
            switch (mutation.Type) {
                case TPostponedMutation::EType::Mutation:
                    DecoratedAutomaton_->LogMutationAtFollower(mutation.RecordData, nullptr);
                    break;

                case TPostponedMutation::EType::ChangelogRotation: {
                    auto result = WaitFor(DecoratedAutomaton_->RotateChangelog(EpochContext_));
                    THROW_ERROR_EXCEPTION_IF_FAILED(result);
                    break;
                }

                default:
                    YUNREACHABLE();
            }
        }
    }

    LOG_INFO("Finished logging postponed mutations");
}

TError TFollowerRecovery::PostponeChangelogRotation(
    TVersion version)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);

    if (PostponedVersion > version) {
        LOG_DEBUG("Late changelog rotation received during recovery, ignored: expected %s, received %s",
            ~ToString(PostponedVersion),
            ~ToString(version));
        return TError();
    }

    if (PostponedVersion < version) {
        return TError("Out-of-order changelog rotation received during recovery: expected %s, received %s",
            ~ToString(PostponedVersion),
            ~ToString(version));
    }

    PostponedMutations.push_back(TPostponedMutation::CreateChangelogRotation());

    LOG_DEBUG("Postponing changelog rotation at version %s",
        ~ToString(PostponedVersion));

    ++PostponedVersion.SegmentId;
    PostponedVersion.RecordId = 0;

    return TError();
}

TError TFollowerRecovery::PostponeMutations(
    TVersion version,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);

    if (PostponedVersion > version) {
        LOG_WARNING("Late mutations received during recovery, ignored: expected %s, received %s",
            ~ToString(PostponedVersion),
            ~ToString(version));
        return TError();
    }

    if (PostponedVersion != version) {
        return TError("Out-of-order mutations received during recovery: expected %s, received %s",
            ~ToString(PostponedVersion),
            ~ToString(version));
    }

    LOG_DEBUG("Postponing %" PRISZT " mutations at version %s",
        recordsData.size(),
        ~ToString(PostponedVersion));

    for (const auto& data : recordsData) {
        PostponedMutations.push_back(TPostponedMutation::CreateMutation(data));
    }

    PostponedVersion.RecordId += recordsData.size();

    return TError();
}

bool TFollowerRecovery::IsLeader() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
