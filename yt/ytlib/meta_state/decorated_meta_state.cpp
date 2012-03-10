#include "stdafx.h"
#include "decorated_meta_state.h"
#include "common.h"
#include "change_log_cache.h"
#include "snapshot_store.h"
#include "meta_state.h"
#include "snapshot.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MetaState");
static NProfiling::TProfiler Profiler("meta_state");

////////////////////////////////////////////////////////////////////////////////

TDecoratedMetaState::TDecoratedMetaState(
    IMetaState* state,
    IInvoker* stateInvoker,
    TSnapshotStore* snapshotStore,
    TChangeLogCache* changeLogCache)
    : State(state)
    , StateInvoker(stateInvoker)
    , SnapshotStore(snapshotStore)
    , ChangeLogCache(changeLogCache)
{
    YASSERT(state);
    YASSERT(stateInvoker);
    YASSERT(snapshotStore);
    YASSERT(changeLogCache);

    VERIFY_INVOKER_AFFINITY(StateInvoker, StateThread);

    ComputeReachableVersion();
}

IInvoker* TDecoratedMetaState::GetStateInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ~StateInvoker;
}

IMetaState* TDecoratedMetaState::GetState() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ~State;
}

void TDecoratedMetaState::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    State->Clear();
    Version = TMetaVersion();
    CurrentChangeLog.Reset();
}

void TDecoratedMetaState::Save(TOutputStream* output)
{
    YASSERT(output);
    VERIFY_THREAD_AFFINITY(StateThread);

    State->Save(output);
}

void TDecoratedMetaState::Load(
    i32 segmentId,
    TInputStream* input)
{
    YASSERT(input);
    VERIFY_THREAD_AFFINITY(StateThread);

    LOG_INFO("Started loading snapshot %d", segmentId);

    CurrentChangeLog.Reset();
    UpdateVersion(TMetaVersion(segmentId, 0));

    PROFILE_TIMING ("snapshot_load_time") {
        State->Load(input);
    }

    LOG_INFO("Finished loading snapshot");
}

void TDecoratedMetaState::ApplyChange(const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    try {
        State->ApplyChange(changeData);
    } catch (const std::exception& ex) {
        LOG_FATAL("Error applying change (Version: %s)\n%s",
            ~Version.ToString(),
            ex.what());
    }

    IncrementRecordCount();
}

void TDecoratedMetaState::ApplyChange(IAction::TPtr changeAction)
{
    YASSERT(changeAction);
    VERIFY_THREAD_AFFINITY(StateThread);

    try {
        changeAction->Do();
    } catch (const std::exception& ex) {
        LOG_FATAL("Error applying change (Version: %s)\n%s",
            ~Version.ToString(),
            ex.what());
    }

    IncrementRecordCount();
}

void TDecoratedMetaState::IncrementRecordCount()
{
    UpdateVersion(TMetaVersion(Version.SegmentId, Version.RecordCount + 1));
}

TCachedAsyncChangeLogPtr TDecoratedMetaState::GetCurrentChangeLog()
{
    if (CurrentChangeLog) {
        return CurrentChangeLog;
    }

    auto result = ChangeLogCache->Get(Version.SegmentId);
    if (!result.IsOK()) {
        LOG_FATAL("Cannot obtain the current changelog\n%s", ~result.ToString());
    }

    CurrentChangeLog = result.Value();
    return CurrentChangeLog;
}

TAsyncChangeLog::TAppendResult::TPtr TDecoratedMetaState::LogChange(
    const TMetaVersion& version,
    const TSharedRef& changeData)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(version.SegmentId == Version.SegmentId);

    auto changeLog = GetCurrentChangeLog();
    return changeLog->Append(version.RecordCount, changeData);
}

void TDecoratedMetaState::AdvanceSegment()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    CurrentChangeLog.Reset();
    UpdateVersion(TMetaVersion(Version.SegmentId + 1, 0));
   
    LOG_INFO("Switched to a new segment %d", Version.SegmentId);
}

void TDecoratedMetaState::RotateChangeLog()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto changeLog = GetCurrentChangeLog();
    changeLog->Finalize();

    AdvanceSegment();

    ChangeLogCache->Create(Version.SegmentId, changeLog->GetRecordCount());
}

void TDecoratedMetaState::ComputeReachableVersion()
{
    i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();
    if (maxSnapshotId == NonexistingSnapshotId) {
        LOG_INFO("No snapshots found");
        // Let's pretend we have snapshot 0.
        maxSnapshotId = 0;
    } else {
        auto snapshotReader = SnapshotStore->GetReader(maxSnapshotId);
        LOG_INFO("Latest snapshot is %d", maxSnapshotId);
    }

    auto currentVersion = TMetaVersion(maxSnapshotId, 0);

    for (i32 segmentId = maxSnapshotId; ; ++segmentId) {
        auto result = ChangeLogCache->Get(segmentId);
        if (!result.IsOK()) {
            ReachableVersion = currentVersion;
            break;
        }

        auto changeLog = result.Value();
        bool isFinal = !ChangeLogCache->Get(segmentId + 1).IsOK();

        LOG_DEBUG("Found changelog (ChangeLogId: %d, RecordCount: %d, PrevRecordCount: %d, IsFinal: %s)",
            segmentId,
            changeLog->GetRecordCount(),
            changeLog->GetPrevRecordCount(),
            ~ToString(isFinal));

        currentVersion = TMetaVersion(segmentId, changeLog->GetRecordCount());
    }

    LOG_INFO("Reachable version is %s", ~ReachableVersion.ToString());
}         

TMetaVersion TDecoratedMetaState::GetVersion() const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // NB: No need to take a spinlock here since both
    // GetVersion and UpdateVersion have same affinity.
    return Version;
}

TMetaVersion TDecoratedMetaState::GetVersionAsync() const
{
	VERIFY_THREAD_AFFINITY_ANY();

	TGuard<TSpinLock> guard(VersionSpinLock);
	return Version;
}

TMetaVersion TDecoratedMetaState::GetReachableVersion() const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return ReachableVersion;
}

TMetaVersion TDecoratedMetaState::GetReachableVersionAsync() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(VersionSpinLock);
    return ReachableVersion;
}

void TDecoratedMetaState::UpdateVersion(const TMetaVersion& newVersion)
{
    TGuard<TSpinLock> guard(VersionSpinLock);
    Version = newVersion;
    ReachableVersion = Max(ReachableVersion, Version);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
