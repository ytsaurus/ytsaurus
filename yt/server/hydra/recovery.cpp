#include "stdafx.h"
#include "recovery.h"
#include "config.h"
#include "decorated_automaton.h"
#include "changelog.h"
#include "snapshot.h"
#include "snapshot_discovery.h"
#include "snapshot_download.h"
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

    auto asyncSnapshotInfo = DiscoverLatestSnapshot(Config, CellManager, targetVersion.SegmentId);
    auto snapshotInfo = WaitFor(asyncSnapshotInfo);

    RecoverToVersionWithSnapshot(targetVersion, snapshotInfo.SnapshotId);
}

void TRecovery::RecoverToVersionWithSnapshot(TVersion targetVersion, int snapshotId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto currentVersion = DecoratedAutomaton->GetAutomatonVersion();
    YCHECK(snapshotId <= targetVersion.SegmentId);
    YCHECK(currentVersion <= targetVersion);

    LOG_INFO("Recovering from %s to %s",
        ~ToString(currentVersion),
        ~ToString(targetVersion));

    if (snapshotId != NonexistingSegmentId && snapshotId > currentVersion.SegmentId) {
        // Load the snapshot.
        LOG_DEBUG("Using snapshot %d for recovery", snapshotId);

        auto reader = SnapshotStore->TryCreateReader(snapshotId);
        if (!reader) {
            {
                auto asyncResult = DownloadSnapshot(
                    Config,
                    CellManager,
                    SnapshotStore,
                    snapshotId);
                auto result = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            reader = SnapshotStore->TryCreateReader(snapshotId);
            if (!reader) {
                LOG_FATAL("Snapshot is not available after download");
            }
        }

        DecoratedAutomaton->Load(snapshotId, reader->GetStream());

        auto snapshotParams = SnapshotStore->TryGetSnapshotParams(snapshotId);
        if (!snapshotParams) {
            LOG_FATAL("Snapshot is not available after download");
        }

        ReplayChangelogs(targetVersion, snapshotParams->PrevRecordCount);
    } else {
        // Recover using changelogs only.
        LOG_INFO("Not using any snapshot for recovery");

        int prevRecordCount;
        if (currentVersion.SegmentId == 0) {
            prevRecordCount = NonexistingSegmentId;
        } else {
            auto changelog = ChangelogStore->OpenChangelogOrThrow(currentVersion.SegmentId - 1);
            prevRecordCount = changelog->GetRecordCount();
        }

        ReplayChangelogs(targetVersion, prevRecordCount);
    }
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
            THROW_ERROR_EXCEPTION("Current version is %s while only %d mutations are expected, forcing clear restart",
                ~ToString(currentVersion),
                targetVersion.RecordId);
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
    int localRecordCount = changelog->GetRecordCount();

    LOG_INFO("Syncing changelog %d: local %d, remote %d",
        changelogId,
        localRecordCount,
        remoteRecordCount);

    if (localRecordCount > remoteRecordCount) {
        if (changelog->IsSealed()) {
            LOG_FATAL("Cannot truncate a sealed changelog %d",
                changelogId);
        } else {
            WaitFor(changelog->Seal(remoteRecordCount));
        }
    } else if (localRecordCount < remoteRecordCount) {
        auto asyncResult = DownloadChangelog(
            Config,
            CellManager,
            ChangelogStore,
            changelogId,
            remoteRecordCount);
        auto result = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error downloading changelog records");
    }
}

void TRecovery::ReplayChangelog(IChangelogPtr changelog, int targetRecordId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(DecoratedAutomaton->GetAutomatonVersion().SegmentId == changelog->GetId());

    int startRecordId = DecoratedAutomaton->GetAutomatonVersion().RecordId;
    int recordCount = targetRecordId - startRecordId;
    if (recordCount == 0)
        return;

    LOG_INFO("Reading records %d-%d from changelog %d",
        startRecordId,
        targetRecordId - 1,
        changelog->GetId());

    auto recordsData = changelog->Read(startRecordId, recordCount, std::numeric_limits<i64>::max());
    if (recordsData.size() != recordCount) {
        LOG_FATAL("Not enough records in changelog %d: expected %d, found %d",
            changelog->GetId(),
            recordCount,
            static_cast<int>(recordsData.size()));
    }

    LOG_INFO("Applying records %d-%d from changelog %d",
        startRecordId,
        targetRecordId - 1,
        changelog->GetId());

    for (const auto& data : recordsData)  {
        DecoratedAutomaton->ApplyMutationDuringRecovery(data);
    }
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

    int snapshotId = SnapshotStore->GetLatestSnapshotId(targetVersion.SegmentId);
    return BIND(&TLeaderRecovery::DoRun, MakeStrong(this))
        .AsyncVia(EpochAutomatonInvoker)
        .Run(targetVersion, snapshotId);
}

TError TLeaderRecovery::DoRun(TVersion targetVersion, int snapshotId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    try {
        RecoverToVersionWithSnapshot(targetVersion, snapshotId);
        return TError();
    } catch (const std::exception& ex) {
        return ex;
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
    const TEpochId& epochId,
    TPeerId leaderId,
    IInvokerPtr epochAutomatonInvoker)
    : TRecovery(
        config,
        cellManager,
        decoratedAutomaton,
        changelogStore,
        snapshotStore,
        epochId,
        leaderId,
        epochAutomatonInvoker)
{ }

TAsyncError TFollowerRecovery::Run(TVersion syncVersion)
{
    VERIFY_THREAD_AFFINITY_ANY();

    PostponedVersion = syncVersion;
    YCHECK(PostponedMutations.empty());

    return BIND(&TFollowerRecovery::DoRun, MakeStrong(this))
        .AsyncVia(EpochAutomatonInvoker)
        .Run(syncVersion);
}

TError TFollowerRecovery::DoRun(TVersion syncVersion)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    try {
        RecoverToVersion(syncVersion);

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

        return TError();
    } catch (const std::exception& ex) {
        return ex;
    }
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
    PostponedVersion.SegmentId = 0;

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
