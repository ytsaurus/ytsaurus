#include "stdafx.h"
#include "recovery.h"
#include "private.h"
#include "config.h"
#include "snapshot_lookup.h"
#include "snapshot_downloader.h"
#include "change_log_downloader.h"
#include "meta_state.h"
#include "decorated_meta_state.h"
#include "snapshot_store.h"
#include "snapshot.h"
#include "persistent_state_manager.h"
#include "change_log_cache.h"

#include <ytlib/actions/bind.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/misc/fs.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static NProfiling::TProfiler& Profiler = MetaStateProfiler;

////////////////////////////////////////////////////////////////////////////////

TRecovery::TRecovery(
    TPersistentStateManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    TChangeLogCachePtr changeLogCache,
    TSnapshotStorePtr snapshotStore,
    const TEpochId& epochId,
    TPeerId leaderId,
    IInvokerPtr controlInvoker,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochStateInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedState(decoratedState)
    , ChangeLogCache(changeLogCache)
    , SnapshotStore(snapshotStore)
    , EpochId(epochId)
    , LeaderId(leaderId)
    , ControlInvoker(controlInvoker)
    , EpochControlInvoker(epochControlInvoker)
    , EpochStateInvoker(epochStateInvoker)
{
    YCHECK(config);
    YCHECK(cellManager);
    YCHECK(decoratedState);
    YCHECK(changeLogCache);
    YCHECK(snapshotStore);
    YCHECK(epochControlInvoker);
    YCHECK(epochStateInvoker);
    VERIFY_INVOKER_AFFINITY(epochStateInvoker, StateThread);
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);
}

TAsyncError TRecovery::RecoverToState(const TMetaVersion& targetVersion)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TSnapshotLookup snapshotLookup(Config, CellManager);
    int lastestSnapshotId = snapshotLookup.LookupLatestSnapshot(targetVersion.SegmentId);
    YCHECK(lastestSnapshotId <= targetVersion.SegmentId);

    return RecoverToStateWithChangeLog(targetVersion, lastestSnapshotId);
}

TAsyncError TRecovery::RecoverToStateWithChangeLog(
    const TMetaVersion& targetVersion,
    int snapshotId)
{
    auto currentVersion = DecoratedState->GetVersion();
    YCHECK(snapshotId <= targetVersion.SegmentId);

    LOG_INFO("Recovering from %s to %s",
        ~currentVersion.ToString(),
        ~targetVersion.ToString());

    if (snapshotId != NonexistingSnapshotId && snapshotId > currentVersion.SegmentId) {
        // Load the snapshot.
        LOG_DEBUG("Using snapshot %d for recovery", snapshotId);

        auto readerResult = SnapshotStore->GetReader(snapshotId);
        if (!readerResult.IsOK()) {
            if (IsLeader()) {
                LOG_FATAL(readerResult, "Snapshot %d is not available",
                    snapshotId);
            }

            LOG_DEBUG("Snapshot cannot be found locally and will be downloaded");

            TSnapshotDownloader snapshotDownloader(
                Config->SnapshotDownloader,
                CellManager);

            auto fileName = SnapshotStore->GetSnapshotFileName(snapshotId);
            auto tempFileName = fileName + NFS::TempFileSuffix;

            auto error = snapshotDownloader.DownloadSnapshot(snapshotId, tempFileName);
            if (!error.IsOK()) {
                return MakeFuture(error);
            }

            try {
                NFS::Rename(tempFileName, fileName);
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Error renaming temp snapshot file %s",
                    ~fileName);
            }

            SnapshotStore->OnSnapshotAdded(snapshotId);

            readerResult = SnapshotStore->GetReader(snapshotId);
            if (!readerResult.IsOK()) {
                LOG_FATAL(readerResult, "Snapshot is not available");
            }
        }

        auto snapshotReader = readerResult.Value();
        snapshotReader->Open();
        DecoratedState->Load(snapshotId, snapshotReader->GetStream());

        // The reader reference is being held by the closure action.
        return ReplayChangeLogs(    
            targetVersion,
            snapshotReader->GetPrevRecordCount());
    } else {
        // Recover using changelogs only.
        LOG_INFO("No snapshot can be used for recovery");

        int prevRecordCount =
            currentVersion.SegmentId == 0
            ? NonexistingPrevRecordCount
            : UnknownPrevRecordCount;

        return ReplayChangeLogs(targetVersion, prevRecordCount);
    }
}

TAsyncError TRecovery::ReplayChangeLogs(
    const TMetaVersion& targetVersion,
    int expectedPrevRecordCount)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    LOG_INFO("Replaying changelogs to %s", ~targetVersion.ToString());

    // Iterate through the segments and apply the changelogs.
    for (int segmentId = DecoratedState->GetVersion().SegmentId;
         segmentId <= targetVersion.SegmentId;
         ++segmentId)
    {
        bool isLast = segmentId == targetVersion.SegmentId;
        bool mayBeMissing = (isLast && targetVersion.RecordCount == 0) || !IsLeader();

        TCachedAsyncChangeLogPtr changeLog;
        auto changeLogResult = ChangeLogCache->Get(segmentId);
        if (!changeLogResult.IsOK()) {
            LOG_FATAL_IF(
                !mayBeMissing,
                changeLogResult,
                "Changelog %d is not available",
                segmentId);
            LOG_INFO("Changelog %d is missing and will be created",
                segmentId);
            YCHECK(expectedPrevRecordCount != UnknownPrevRecordCount);
            changeLog = ChangeLogCache->Create(segmentId, expectedPrevRecordCount, EpochId);
        } else {
            changeLog = changeLogResult.Value();
        }

        LOG_FATAL_IF(
            expectedPrevRecordCount != UnknownPrevRecordCount &&
            changeLog->GetPrevRecordCount() != expectedPrevRecordCount,
            "PrevRecordCount mismatch in changelog %d: expected: %d, found %d",
            segmentId,
            expectedPrevRecordCount,
            changeLog->GetPrevRecordCount());

        if (!IsLeader()) {
            TProxy proxy(CellManager->GetMasterChannel(LeaderId));
            proxy.SetDefaultTimeout(Config->RpcTimeout);

            auto request = proxy.GetChangeLogInfo();
            request->set_change_log_id(segmentId);

            auto response = request->Invoke().Get();
            if (!response->IsOK()) {
                auto wrappedError = TError("Error getting changelog %d info from leader", segmentId)
                    << response->GetError();
                return MakeFuture(wrappedError);
            }

            int remoteRecordCount = response->record_count();
            int localRecordCount = changeLog->GetRecordCount();
            int targetLocalRecordCount = isLast ? targetVersion.RecordCount : remoteRecordCount;

            LOG_INFO("Syncing changelog %d record count: local %d, remote %d, target local %d",
                segmentId,
                localRecordCount,
                remoteRecordCount,
                targetLocalRecordCount);

            LOG_FATAL_IF(
                remoteRecordCount < targetLocalRecordCount,
                "Remote changelog %d has insufficient records",
                segmentId);

            if (localRecordCount > targetLocalRecordCount) {
                changeLog->Truncate(targetLocalRecordCount);
                changeLog->Finalize();
            }

            if (localRecordCount < targetLocalRecordCount) {
                TChangeLogDownloader changeLogDownloader(
                    Config->ChangeLogDownloader,
                    CellManager,
                    ControlInvoker);
                auto error = changeLogDownloader.Download(TMetaVersion(segmentId, targetLocalRecordCount), ~changeLog);
                if (!error.IsOK()) {
                    return MakeFuture(error);
                }
            }

            YCHECK(changeLog->GetRecordCount() == targetLocalRecordCount);
        }

        if (!isLast && !changeLog->IsFinalized()) {
            changeLog->Finalize();
        }

        // Check if the current state contains some mutations that are redundant.
        // If so, clear the state and restart recovery.
        if (isLast) {
            auto currentVersion = DecoratedState->GetVersion();
            if (currentVersion.RecordCount > targetVersion.RecordCount) {
                DecoratedState->Clear();
                return MakeFuture(TError("Current version is %s while only %d mutations are expected, forcing clear restart",
                    ~currentVersion.ToString(),
                    targetVersion.RecordCount));
            }

            YCHECK(changeLog->GetRecordCount() == targetVersion.RecordCount);
        }

        ReplayChangeLog(*changeLog, changeLog->GetRecordCount());

        if (!isLast) {
            DecoratedState->AdvanceSegment();
        }

        expectedPrevRecordCount = changeLog->GetRecordCount();
    }

    YCHECK(DecoratedState->GetVersion() == targetVersion);

    return MakeFuture(TError());
}

void TRecovery::ReplayChangeLog(
    const TAsyncChangeLog& changeLog,
    int targetRecordCount)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YCHECK(DecoratedState->GetVersion().SegmentId == changeLog.GetId());
    
    int startRecordId = DecoratedState->GetVersion().RecordCount;
    int recordCount = targetRecordCount - startRecordId;

    if (recordCount == 0)
        return;

    LOG_INFO("Replaying records %d-%d from changelog %d",
        startRecordId,
        targetRecordCount - 1, 
        changeLog.GetId());

    std::vector<TSharedRef> records;
    changeLog.Read(startRecordId, recordCount, &records);
    if (records.size() != recordCount) {
        LOG_FATAL("Not enough records in changelog %d: expected %d, found %d (StartRecordId: %d)",
            changeLog.GetId(),
            recordCount,
            static_cast<int>(records.size()),
            startRecordId);
    }

    LOG_INFO("Applying %d mutations to meta state", recordCount);
    Profiler.Enqueue("/replay_change_count", recordCount);

    PROFILE_TIMING ("/replay_time") {
        FOREACH (const auto& recordData, records)  {
            auto version = DecoratedState->GetVersion();
            try {
                DecoratedState->ApplyMutation(recordData);
            } catch (const std::exception& ex) {
                LOG_DEBUG(ex, "Failed to apply the mutation during recovery (Version: %s)",
                    ~version.ToString());
            }
        }
    }

    LOG_INFO("Finished applying mutations");
}

////////////////////////////////////////////////////////////////////////////////

TLeaderRecovery::TLeaderRecovery(
    TPersistentStateManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    TChangeLogCachePtr changeLogCache,
    TSnapshotStorePtr snapshotStore,
    const TEpochId& epochId,
    IInvokerPtr controlInvoker,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochStateInvoker)
    : TRecovery(
        config,
        cellManager,
        decoratedState,
        changeLogCache,
        snapshotStore,
        epochId,
        cellManager->GetSelfId(),
        controlInvoker,
        epochControlInvoker,
        epochStateInvoker)
{ }

TAsyncError TLeaderRecovery::Run()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto version = DecoratedState->GetReachableVersionAsync();
    int maxSnapshotId = SnapshotStore->LookupLatestSnapshot();
    return
        BIND(
            &TRecovery::RecoverToStateWithChangeLog,
            MakeStrong(this),
            version,
            maxSnapshotId)
        .AsyncVia(EpochStateInvoker)
        .Run();
}

bool TLeaderRecovery::IsLeader() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TFollowerRecovery::TFollowerRecovery(
    TPersistentStateManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    TChangeLogCachePtr changeLogCache,
    TSnapshotStorePtr snapshotStore,
    const TEpochId& epochId,
    TPeerId leaderId,
    IInvokerPtr controlInvoker,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochStateInvoker,
    const TMetaVersion& targetVersion)
    : TRecovery(
        config,
        cellManager,
        decoratedState,
        changeLogCache,
        snapshotStore,
        epochId,
        leaderId,
        controlInvoker,
        epochControlInvoker,
        epochStateInvoker)
    , TargetVersion(targetVersion)
{ }

TAsyncError TFollowerRecovery::Run()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    PostponedVersion = TargetVersion;
    YCHECK(PostponedMutations.empty());

    return
        BIND(
            &TRecovery::RecoverToState,
            MakeStrong(this),
            TargetVersion)
        .AsyncVia(EpochStateInvoker)
        .Run()
        .Apply(BIND(
            &TFollowerRecovery::OnSyncReached,
            MakeStrong(this)));
}

TAsyncError TFollowerRecovery::OnSyncReached(TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        return MakeFuture(error);
    }

    LOG_INFO("Sync reached");

    return BIND(&TFollowerRecovery::CapturePostponedMutations, MakeStrong(this))
           .AsyncVia(EpochControlInvoker)
           .Run();
}

TAsyncError TFollowerRecovery::CapturePostponedMutations()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (PostponedMutations.empty()) {
        LOG_INFO("No postponed mutations left");
        return MakeFuture(TError());
    }

    TPostponedMutations mutations;
    mutations.swap(PostponedMutations);

    LOG_INFO("Captured %" PRISZT " postponed mutations", mutations.size());

    return BIND(
               &TFollowerRecovery::ApplyPostponedMutations,
               MakeStrong(this),
               Passed(MoveRV(mutations)))
           .AsyncVia(EpochStateInvoker)
           .Run();
}

TAsyncError TFollowerRecovery::ApplyPostponedMutations(
    TPostponedMutations mutations)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    LOG_INFO("Applying %" PRISZT " postponed mutations", mutations.size());
    
    FOREACH (const auto& mutation, mutations) {
        switch (mutation.Type) {
            case TPostponedMutation::EType::Mutation: {
                auto version = DecoratedState->GetVersion();
                DecoratedState->LogMutation(version, mutation.RecordData);
                try {
                    DecoratedState->ApplyMutation(mutation.RecordData);
                } catch (const std::exception& ex) {
                    LOG_DEBUG(ex, "Failed to apply the mutation during recovery at version %s",
                        ~version.ToString());
                }
                break;
            }

            case TPostponedMutation::EType::SegmentAdvance:
                DecoratedState->RotateChangeLog(EpochId);
                break;

            default:
                YUNREACHABLE();
        }
    }
   
    LOG_INFO("Finished applying postponed mutations");

    return BIND(&TFollowerRecovery::CapturePostponedMutations, MakeStrong(this))
           .AsyncVia(EpochControlInvoker)
           .Run();
}

TError TFollowerRecovery::PostponeSegmentAdvance(
    const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (PostponedVersion > version) {
        LOG_DEBUG("Late segment advance received during recovery, ignored: expected %s, received %s",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return TError();
    }

    if (PostponedVersion < version) {
        TError error("Out-of-order segment advance received during recovery: expected %s, received %s",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return error;
    }

    PostponedMutations.push_back(TPostponedMutation::CreateSegmentAdvance());
    
    LOG_DEBUG("Postponing segment advance at version %s",
        ~PostponedVersion.ToString());

    ++PostponedVersion.SegmentId;
    PostponedVersion.RecordCount = 0;
    
    return TError();
}

TError TFollowerRecovery::PostponeMutations(
    const TMetaVersion& version,
    const std::vector<TSharedRef>& recordsData)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (PostponedVersion > version) {
        LOG_WARNING("Late mutations received during recovery, ignored: expected %s, received %s",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return TError();
    }

    if (PostponedVersion != version) {
        TError error("Out-of-order mutations received during recovery: expected %s, received %s",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return error;
    }

    LOG_DEBUG("Postponing %" PRISZT " mutations at version %s",
        recordsData.size(),
        ~PostponedVersion.ToString());

    FOREACH (const auto& recordData, recordsData) {
        PostponedMutations.push_back(TPostponedMutation::CreateMutation(recordData));
    }
    
    PostponedVersion.RecordCount += recordsData.size();

    return TError();
}

bool TFollowerRecovery::IsLeader() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
