#include "stdafx.h"
#include "recovery.h"
#include "config.h"
#include "decorated_automaton.h"
#include "changelog.h"
#include "snapshot.h"
#include "changelog_download.h"

#include <core/concurrency/fiber.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>

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
    const TEpochId& epochId,
    TPeerId leaderId,
    IInvokerPtr epochAutomatonInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedAutomaton(decoratedAutomaton)
    , ChangelogStore(changelogStore)
    , SnapshotStore(snapshotStore)
    , EpochId(epochId)
    , LeaderId(leaderId)
    , EpochAutomatonInvoker(epochAutomatonInvoker)
    , Logger(HydraLogger)
{
    YCHECK(Config);
    YCHECK(CellManager);
    YCHECK(DecoratedAutomaton);
    YCHECK(ChangelogStore);
    YCHECK(SnapshotStore);
    YCHECK(EpochAutomatonInvoker);
    VERIFY_INVOKER_AFFINITY(epochAutomatonInvoker, AutomatonThread);

    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(CellManager->GetCellGuid())));
}

void TRecovery::RecoverToVersion(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto latestSnapshotIdOrError = WaitFor(SnapshotStore->GetLatestSnapshotId(targetVersion.SegmentId));
    THROW_ERROR_EXCEPTION_IF_FAILED(latestSnapshotIdOrError, "Error computing the latest snapshot id");
    int latestSnapshotId = latestSnapshotIdOrError.Value();

    RecoverToVersionWithSnapshot(targetVersion, latestSnapshotId);
}

void TRecovery::RecoverToVersionWithSnapshot(TVersion targetVersion, int snapshotId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto currentVersion = DecoratedAutomaton->GetAutomatonVersion();
    YCHECK(snapshotId <= targetVersion.SegmentId);
    YCHECK(currentVersion <= targetVersion);

    LOG_INFO("Recoverying from %s to %s",
        ~ToString(currentVersion),
        ~ToString(targetVersion));

    if (snapshotId != NonexistingSegmentId && snapshotId > currentVersion.SegmentId) {
        // Load the snapshot.
        LOG_DEBUG("Using snapshot %d for recovery", snapshotId);

        auto readerOrError = WaitFor(SnapshotStore->CreateReader(snapshotId));
        THROW_ERROR_EXCEPTION_IF_FAILED(readerOrError, "Error creating snapshot reader");
        auto reader = readerOrError.Value();

        DecoratedAutomaton->LoadSnapshot(snapshotId, reader->GetStream());

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

    auto initialVersion = DecoratedAutomaton->GetAutomatonVersion();
    for (int segmentId = initialVersion.SegmentId;
         segmentId <= targetVersion.SegmentId;
         ++segmentId)
    {
        bool isLast = segmentId == targetVersion.SegmentId;
        auto changelog = ChangelogStore->TryOpenChangelog(segmentId);
        if (!changelog) {
            LOG_INFO("Changelog %d is missing and will be created",
                segmentId);

            YCHECK(expectedPrevRecordCount != UnknownPrevRecordCount);
            TChangelogCreateParams params;
            params.PrevRecordCount = expectedPrevRecordCount;
            changelog = ChangelogStore->CreateChangelog(segmentId, params);

            TVersion newLoggedVersion(segmentId, 0);
            // NB: Equality is only possible when segmentId == 0.
            YCHECK(DecoratedAutomaton->GetLoggedVersion() <= newLoggedVersion);
            DecoratedAutomaton->SetLoggedVersion(newLoggedVersion);
        }

        LOG_FATAL_IF(
            expectedPrevRecordCount != UnknownPrevRecordCount &&
            changelog->GetPrevRecordCount() != expectedPrevRecordCount,
            "PrevRecordCount mismatch in changelog %d: expected: %d, found %d",
            segmentId,
            expectedPrevRecordCount,
            changelog->GetPrevRecordCount());

        if (!IsLeader()) {
            SyncChangelog(changelog);
        }

        if (!isLast && !changelog->IsSealed()) {
            WaitFor(changelog->Flush());
            if (changelog->IsSealed()) {
                LOG_WARNING("Changelog %d is already sealed",
                    changelog->GetId());
            } else {
                WaitFor(changelog->Seal(changelog->GetRecordCount()));
            }
        }

        // Check if the current state contains some mutations that are redundant.
        // If so, clear the state and restart recovery.
        auto currentVersion = DecoratedAutomaton->GetAutomatonVersion();
        YCHECK(currentVersion.SegmentId == changelog->GetId());
        if (currentVersion.RecordId > changelog->GetRecordCount()) {
            DecoratedAutomaton->Clear();
            THROW_ERROR_EXCEPTION("Current version is %s while only %d mutations are expected in this segment, forcing clean restart",
                ~ToString(currentVersion),
                changelog->GetRecordCount());
        }

        int targetRecordId = isLast ? targetVersion.RecordId : changelog->GetRecordCount();
        ReplayChangelog(changelog, targetRecordId);

        if (!isLast) {
            DecoratedAutomaton->RotateChangelogDuringRecovery();
        }

        expectedPrevRecordCount = changelog->GetRecordCount();
    }

    YCHECK(DecoratedAutomaton->GetAutomatonVersion() == targetVersion);
}

void TRecovery::SyncChangelog(IChangelogPtr changelog)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto changelogId = changelog->GetId();

    THydraServiceProxy proxy(CellManager->GetPeerChannel(LeaderId));
    proxy.SetDefaultTimeout(Config->RpcTimeout);

    auto req = proxy.LookupChangelog();
    req->set_changelog_id(changelogId);

    auto rsp = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting changelog %d info from leader",
        changelogId);

    int remoteRecordCount = rsp->record_count();
    if (changelogId == SyncVersion.SegmentId && remoteRecordCount > SyncVersion.RecordId) {
        THROW_ERROR_EXCEPTION("Follower has more records in changelog %d than the leader: %d > %d",
            changelogId,
            remoteRecordCount,
            SyncVersion.RecordId);
    }

    int localRecordCount = changelog->GetRecordCount();
    // NB: Don't download records past the sync point since they are expected to be postponed.
    int syncRecordCount =
        changelogId == SyncVersion.SegmentId
        ? SyncVersion.RecordId
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
            if (DecoratedAutomaton->GetLoggedVersion().SegmentId == sealedVersion.SegmentId) {
                DecoratedAutomaton->SetLoggedVersion(sealedVersion);
            }
        }
    } else if (localRecordCount < syncRecordCount) {
        auto asyncResult = DownloadChangelog(
            Config,
            CellManager,
            ChangelogStore,
            changelogId,
            syncRecordCount);
        auto result = WaitFor(asyncResult);

        TVersion downloadedVersion(changelogId, changelog->GetRecordCount());
        DecoratedAutomaton->SetLoggedVersion(std::max(DecoratedAutomaton->GetLoggedVersion(), downloadedVersion));

        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error downloading changelog records");
    }
}

void TRecovery::ReplayChangelog(IChangelogPtr changelog, int targetRecordId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(DecoratedAutomaton->GetAutomatonVersion().SegmentId == changelog->GetId());

    if (changelog->GetRecordCount() < targetRecordId) {
        LOG_FATAL("Not enough records in changelog %d: expected %d, actual %d",
            changelog->GetId(),
            targetRecordId,
            changelog->GetRecordCount());
    }

    while (true) {
        int startRecordId = DecoratedAutomaton->GetAutomatonVersion().RecordId;
        int recordsNeeded = targetRecordId - startRecordId;
        YCHECK(recordsNeeded >= 0);
        if (recordsNeeded == 0)
            break;
    
        LOG_INFO("Trying to read records %d-%d from changelog %d",
            startRecordId,
            targetRecordId - 1,
            changelog->GetId());

        auto recordsData = changelog->Read(
            startRecordId,
            recordsNeeded,
            Config->MaxChangelogReadSize);
        int recordsRead = static_cast<int>(recordsData.size());

        LOG_INFO("Finished reading records %d-%d from changelog %d",
            startRecordId,
            startRecordId + recordsRead - 1,
            changelog->GetId());

        LOG_INFO("Applying records %d-%d from changelog %d",
            startRecordId,
            startRecordId + recordsRead - 1,
            changelog->GetId());

        for (const auto& data : recordsData)  {
            DecoratedAutomaton->ApplyMutationDuringRecovery(data);
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
    auto changelog = ChangelogStore->TryOpenChangelog(segmentId - 1);
    if (changelog) {
        return changelog->GetRecordCount();
    }

    // Extract from snapshot.
    auto snapshotParamsOrError = WaitFor(SnapshotStore->GetSnapshotParams(segmentId));
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
    const TEpochId& epochId,
    IInvokerPtr epochAutomatonInvoker)
    : TRecovery(
        config,
        cellManager,
        decoratedAutomaton,
        changelogStore,
        snapshotStore,
        epochId,
        cellManager->GetSelfId(),
        epochAutomatonInvoker)
{ }

TAsyncError TLeaderRecovery::Run(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY_ANY();
    
    SyncVersion = targetVersion;
    return BIND(&TLeaderRecovery::DoRun, MakeStrong(this))
        .Guarded()
        .AsyncVia(EpochAutomatonInvoker)
        .Run(targetVersion);
}

void TLeaderRecovery::DoRun(TVersion targetVersion)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto latestSnapshotIdOrError = WaitFor(SnapshotStore->GetLatestSnapshotId(targetVersion.SegmentId));
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
    const TEpochId& epochId,
    TPeerId leaderId,
    IInvokerPtr epochAutomatonInvoker,
    TVersion syncVersion)
    : TRecovery(
        config,
        cellManager,
        decoratedAutomaton,
        changelogStore,
        snapshotStore,
        epochId,
        leaderId,
        epochAutomatonInvoker)
{
    SyncVersion = PostponedVersion = syncVersion;
}

TAsyncError TFollowerRecovery::Run()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND(&TFollowerRecovery::DoRun, MakeStrong(this))
        .Guarded()
        .AsyncVia(EpochAutomatonInvoker)
        .Run();
}

void TFollowerRecovery::DoRun()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    RecoverToVersion(SyncVersion);

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
                    DecoratedAutomaton->LogMutationAtFollower(mutation.RecordData, nullptr);
                    break;

                case TPostponedMutation::EType::ChangelogRotation:
                    DecoratedAutomaton->RotateChangelog();
                    break;

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
