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
    const TEpoch& epoch,
    TPeerId leaderId,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochStateInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedState(decoratedState)
    , ChangeLogCache(changeLogCache)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
    , LeaderId(leaderId)
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
    i32 lastestSnapshotId = snapshotLookup.LookupLatestSnapshot(targetVersion.SegmentId);
    YCHECK(lastestSnapshotId <= targetVersion.SegmentId);

    return RecoverToStateWithChangeLog(targetVersion, lastestSnapshotId);
}

TAsyncError TRecovery::RecoverToStateWithChangeLog(
    const TMetaVersion& targetVersion,
    i32 snapshotId)
{
    auto currentVersion = DecoratedState->GetVersion();
    YCHECK(snapshotId <= targetVersion.SegmentId);

    LOG_INFO("Recovering state from %s to %s",
        ~currentVersion.ToString(),
        ~targetVersion.ToString());

    if (snapshotId != NonexistingSnapshotId && snapshotId > currentVersion.SegmentId) {
        // Load the snapshot.
        LOG_DEBUG("Using snapshot %d for recovery", snapshotId);

        auto readerResult = SnapshotStore->GetReader(snapshotId);
        if (!readerResult.IsOK()) {
            if (IsLeader()) {
                LOG_FATAL("Snapshot %d is not available\n%s",
                    snapshotId,
                    ~readerResult.ToString());
            }

            LOG_DEBUG("Snapshot cannot be found locally and will be downloaded");

            TSnapshotDownloader snapshotDownloader(Config->SnapshotDownloader, CellManager);

            auto fileName = SnapshotStore->GetSnapshotFileName(snapshotId);
            auto tempFileName = fileName + NFS::TempFileSuffix;

            auto downloadResult = snapshotDownloader.DownloadSnapshot(snapshotId, tempFileName);
            if (downloadResult != TSnapshotDownloader::EResult::OK) {
                TError error("Error downloading snapshot %d\n%s",
                    snapshotId,
                    ~downloadResult.ToString());
                return MakeFuture(error);
            }

            try {
                NFS::Rename(tempFileName, fileName);
            } catch (const std::exception& ex) {
                LOG_FATAL("Error renaming temp snapshot file %s\n%s",
                    ~fileName,
                    ex.what());
            }

            SnapshotStore->OnSnapshotAdded(snapshotId);

            readerResult = SnapshotStore->GetReader(snapshotId);
            if (!readerResult.IsOK()) {
                LOG_FATAL("Snapshot is not available\n%s", ~readerResult.ToString());
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

        i32 prevRecordCount =
            currentVersion.SegmentId == 0
            ? NonexistingPrevRecordCount
            : UnknownPrevRecordCount;

        return ReplayChangeLogs(targetVersion, prevRecordCount);
    }
}

TAsyncError TRecovery::ReplayChangeLogs(
    const TMetaVersion& targetVersion,
    i32 expectedPrevRecordCount)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // Iterate through the segments and apply the changelogs.
    for (i32 segmentId = DecoratedState->GetVersion().SegmentId;
         segmentId <= targetVersion.SegmentId;
         ++segmentId)
    {
        bool isLast = segmentId == targetVersion.SegmentId;
        bool mayBeMissing = (isLast && targetVersion.RecordCount == 0) || !IsLeader();

        TCachedAsyncChangeLogPtr changeLog;
        auto changeLogResult = ChangeLogCache->Get(segmentId);
        if (!changeLogResult.IsOK()) {
            if (!mayBeMissing) {
                LOG_FATAL("Changelog %d is not available\n%s",
                    segmentId,
                    ~changeLogResult.ToString());
            }

            LOG_INFO("Changelog %d is missing and will be created", segmentId);
            YCHECK(expectedPrevRecordCount != UnknownPrevRecordCount);
            changeLog = ChangeLogCache->Create(segmentId, expectedPrevRecordCount, Epoch);
        } else {
            changeLog = changeLogResult.Value();
        }

        LOG_DEBUG("Changelog found (ChangeLogId: %d, RecordCount: %d, PrevRecordCount: %d, IsLast: %s)",
            segmentId,
            changeLog->GetRecordCount(),
            changeLog->GetPrevRecordCount(),
            ~FormatBool(isLast));

        if (expectedPrevRecordCount != UnknownPrevRecordCount &&
            changeLog->GetPrevRecordCount() != expectedPrevRecordCount)
        {
            LOG_FATAL("PrevRecordCount mismatch: expected: %d but found %d",
                expectedPrevRecordCount,
                changeLog->GetPrevRecordCount());
        }

        if (!IsLeader()) {
            auto request =
                CellManager->GetMasterProxy<TProxy>(LeaderId)
                ->GetChangeLogInfo()
                ->SetTimeout(Config->RpcTimeout);
            request->set_change_log_id(segmentId);

            auto response = request->Invoke().Get();
            if (!response->IsOK()) {
                TError error("Error getting changelog %d info from leader\n%s",
                    segmentId,
                    ~response->GetError().ToString());
                return MakeFuture(error);
            }

            i32 localRecordCount = changeLog->GetRecordCount();
            i32 remoteRecordCount = response->record_count();

            LOG_INFO("Changelog %d has %d local and %d remote records",
                segmentId,
                localRecordCount,
                remoteRecordCount);

            if (segmentId == targetVersion.SegmentId &&
                remoteRecordCount < targetVersion.RecordCount)
            {
                LOG_FATAL("Remote changelog %d has insufficient records",
                    segmentId);
            }

            if (localRecordCount > remoteRecordCount) {
                changeLog->Truncate(remoteRecordCount);
                changeLog->Finalize();
                LOG_INFO("Local changelog %d has %d records, truncated to %d records",
                    segmentId,
                    localRecordCount,
                    remoteRecordCount);
            }

            auto currentVersion = DecoratedState->GetVersion();
            YCHECK(currentVersion.SegmentId <= segmentId);

            // Check if the current state contains some changes that are not present in the remote changelog.
            // If so, clear the state and restart recovery.
            if (currentVersion.SegmentId == segmentId && currentVersion.RecordCount > remoteRecordCount) {
                TError error("Current version is %s while only %d mutations are expected, forcing clear restart",
                    ~currentVersion.ToString(),
                    remoteRecordCount);
                LOG_INFO("%s", ~error.ToString());
                DecoratedState->Clear();
                return MakeFuture(error);
            }

            // Do not download more than actually needed.
            int desiredRecordCount =
                segmentId == targetVersion.SegmentId
                ? targetVersion.RecordCount
                : remoteRecordCount;
            
            if (localRecordCount < desiredRecordCount) {
                TChangeLogDownloader changeLogDownloader(
                    Config->ChangeLogDownloader,
                    CellManager,
                    EpochControlInvoker);
                auto changeLogResult = changeLogDownloader.Download(
                    TMetaVersion(segmentId, desiredRecordCount),
                    *changeLog);

                if (changeLogResult != TChangeLogDownloader::EResult::OK) {
                    TError error("Error downloading changelog %d\n%s",
                        segmentId,
                        ~changeLogResult.ToString());
                    return MakeFuture(error);
                }
            }
        }

        if (!isLast && !changeLog->IsFinalized()) {
            LOG_INFO("Finalizing an intermediate changelog %d", segmentId);
            changeLog->Finalize();
        }

        if (segmentId == targetVersion.SegmentId) {
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
    TAsyncChangeLog& changeLog,
    i32 targetRecordCount)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YCHECK(DecoratedState->GetVersion().SegmentId == changeLog.GetId());
    
    i32 startRecordId = DecoratedState->GetVersion().RecordCount;
    i32 recordCount = targetRecordCount - startRecordId;

    if (recordCount == 0)
        return;

    LOG_INFO("Reading records %d-%d from changelog %d",
        startRecordId,
        targetRecordCount - 1, 
        changeLog.GetId());

    std::vector<TSharedRef> records;
    changeLog.Read(startRecordId, recordCount, &records);
    if (records.size() != recordCount) {
        LOG_FATAL("Not enough records in changelog %d: expected %d but found %d (StartRecordId: %d)",
            changeLog.GetId(),
            recordCount,
            static_cast<int>(records.size()),
            startRecordId);
    }

    LOG_INFO("Applying %d changes to meta state", recordCount);
    Profiler.Enqueue("/replay_change_count", recordCount);

    PROFILE_TIMING ("/replay_time") {
        FOREACH (const auto& recordData, records)  {
            auto version = DecoratedState->GetVersion();
            try {
                DecoratedState->ApplyMutation(recordData);
            } catch (const std::exception& ex) {
                LOG_DEBUG("Failed to apply the mutation during recovery (Version: %s)\n%s",
                    ~version.ToString(),
                    ex.what());
            }
        }
    }

    LOG_INFO("Finished applying changes");
}

////////////////////////////////////////////////////////////////////////////////

TLeaderRecovery::TLeaderRecovery(
    TPersistentStateManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    TChangeLogCachePtr changeLogCache,
    TSnapshotStorePtr snapshotStore,
    const TEpoch& epoch,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochStateInvoker)
    : TRecovery(
        config,
        cellManager,
        decoratedState,
        changeLogCache,
        snapshotStore,
        epoch,
        cellManager->GetSelfId(),
        epochControlInvoker,
        epochStateInvoker)
{ }

TAsyncError TLeaderRecovery::Run()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto version = DecoratedState->GetReachableVersionAsync();
    i32 maxSnapshotId = SnapshotStore->LookupLatestSnapshot();
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
    const TEpoch& epoch,
    TPeerId leaderId,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochStateInvoker,
    const TMetaVersion& targetVersion)
    : TRecovery(
        config,
        cellManager,
        decoratedState,
        changeLogCache,
        snapshotStore,
        epoch,
        leaderId,
        epochControlInvoker,
        epochStateInvoker)
    , Promise(NewPromise<TError>())
    , TargetVersion(targetVersion)
{ }

TAsyncError TFollowerRecovery::Run()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    PostponedVersion = TargetVersion;
    YCHECK(PostponedMutations.empty());

    auto this_ = MakeStrong(this);
    BIND(
        &TRecovery::RecoverToState,
        MakeStrong(this),
        TargetVersion)
    .AsyncVia(EpochStateInvoker)
    .Run()
    .Apply(BIND(
        &TFollowerRecovery::OnSyncReached,
        MakeStrong(this)))
    // TODO(sandello): Remove a lambda here when listeners accept
    // const-reference.
    .Subscribe(BIND([this, this_] (TError error) mutable {
        Promise.Set(MoveRV(error));
    }));

    return Promise;
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
        LOG_INFO("No postponed changes left");
        return MakeFuture(TError());
    }

    THolder<TPostponedMutations> changes(new TPostponedMutations());
    changes->swap(PostponedMutations);

    LOG_INFO("Captured %" PRISZT " postponed changes", changes->size());

    return BIND(
               &TFollowerRecovery::ApplyPostponedMutations,
               MakeStrong(this),
               Passed(MoveRV(changes)))
           .AsyncVia(EpochStateInvoker)
           .Run();
}

TAsyncError TFollowerRecovery::ApplyPostponedMutations(
    TAutoPtr<TPostponedMutations> mutations)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    LOG_INFO("Applying %" PRISZT " postponed mutations", mutations->size());
    
    FOREACH (const auto& mutation, *mutations) {
        switch (mutation.Type) {
            case TPostponedMutation::EType::Mutation: {
                auto version = DecoratedState->GetVersion();
                DecoratedState->LogMutation(version, mutation.RecordData);
                try {
                    DecoratedState->ApplyMutation(mutation.RecordData);
                } catch (const std::exception& ex) {
                    LOG_DEBUG("Failed to apply the mutation during recovery at version %s\n%s",
                        ~version.ToString(),
                        ex.what());
                }
                break;
            }

            case TPostponedMutation::EType::SegmentAdvance:
                DecoratedState->RotateChangeLog();
                break;

            default:
                YUNREACHABLE();
        }
    }
   
    LOG_INFO("Finished applying postponed mutations");

    return BIND(
               &TFollowerRecovery::CapturePostponedMutations,
               MakeStrong(this))
           .AsyncVia(EpochControlInvoker)
           .Run();
}

TError TFollowerRecovery::PostponeSegmentAdvance(
    const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (PostponedVersion > version) {
        LOG_DEBUG("Late segment advance received during recovery, ignored: expected %s but received %s",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return TError();
    }

    if (PostponedVersion < version) {
        TError error("Out-of-order segment advance received during recovery: expected %s but received %s",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return error;
    }

    PostponedMutations.push_back(TPostponedMutation::CreateSegmentAdvance());
    
    LOG_DEBUG("Postponing segment advance at version %s", ~PostponedVersion.ToString());

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
        LOG_WARNING("Late mutations received during recovery, ignored: expected %s but received %s",
            ~PostponedVersion.ToString(),
            ~version.ToString());
        return TError();
    }

    if (PostponedVersion != version) {
        TError error("Out-of-order mutations received during recovery: expected %s but received %s",
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
