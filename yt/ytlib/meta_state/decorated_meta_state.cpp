#include "stdafx.h"
#include "decorated_meta_state.h"
#include "common.h"
#include "change_log_cache.h"
#include "snapshot_store.h"
#include "meta_state.h"
#include "snapshot.h"

#include <ytlib/actions/callback.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MetaState");
static NProfiling::TProfiler Profiler("/meta_state");

////////////////////////////////////////////////////////////////////////////////

TDecoratedMetaState::TDecoratedMetaState(
    IMetaStatePtr state,
    IInvokerPtr stateInvoker,
    IInvokerPtr controlInvoker,
    TSnapshotStorePtr snapshotStore,
    TChangeLogCachePtr changeLogCache)
    : State(state)
    , StateInvoker(stateInvoker)
    , SnapshotStore(snapshotStore)
    , ChangeLogCache(changeLogCache)
    , Started(false)
{
    YASSERT(state);
    YASSERT(stateInvoker);
    YASSERT(snapshotStore);
    YASSERT(changeLogCache);
    VERIFY_INVOKER_AFFINITY(StateInvoker, StateThread);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
}

void TDecoratedMetaState::Start()
{
    YASSERT(!Started);
    ComputeReachableVersion();
    Started = true;
}

void TDecoratedMetaState::SetEpoch(const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(Started);
    Epoch = epoch;
}

const TEpoch& TDecoratedMetaState::GetEpoch() const
{
    YASSERT(Started);
    return Epoch;
}

void TDecoratedMetaState::ComputeReachableVersion()
{
    i32 maxSnapshotId = SnapshotStore->LookupLatestSnapshot();
    if (maxSnapshotId == NonexistingSnapshotId) {
        LOG_INFO("No snapshots found");
        // Let's pretend we have snapshot 0.
        maxSnapshotId = 0;
    } else {
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
            ~FormatBool(isFinal));

        currentVersion = TMetaVersion(segmentId, changeLog->GetRecordCount());
    }

    LOG_INFO("Reachable version is %s", ~ReachableVersion.ToString());
}

IInvokerPtr TDecoratedMetaState::GetStateInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(Started);

    return ~StateInvoker;
}

IMetaStatePtr TDecoratedMetaState::GetState() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(Started);

    return State;
}

void TDecoratedMetaState::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(Started);

    State->Clear();
    Version = TMetaVersion();
    CurrentChangeLog.Reset();
}

void TDecoratedMetaState::Save(TOutputStream* output)
{
    YASSERT(output);
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    State->Save(output);
}

void TDecoratedMetaState::Load(
    i32 segmentId,
    TInputStream* input)
{
    YASSERT(input);
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    LOG_INFO("Started loading snapshot %d", segmentId);

    CurrentChangeLog.Reset();
    UpdateVersion(TMetaVersion(segmentId, 0));

    PROFILE_TIMING ("/snapshot_load_time") {
        State->Load(input);
    }

    LOG_INFO("Finished loading snapshot");
}

void TDecoratedMetaState::ApplyChange(const TSharedRef& changeData)
{
    YASSERT(Started);
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

void TDecoratedMetaState::ApplyChange(const TClosure& changeAction)
{
    YASSERT(!changeAction.IsNull());
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    try {
        changeAction.Run();
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

TAsyncChangeLog::TAppendResult TDecoratedMetaState::LogChange(
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
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    CurrentChangeLog.Reset();
    UpdateVersion(TMetaVersion(Version.SegmentId + 1, 0));

    LOG_INFO("Switched to a new segment %d", Version.SegmentId);
}

void TDecoratedMetaState::RotateChangeLog()
{
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    auto changeLog = GetCurrentChangeLog();
    changeLog->Finalize();

    AdvanceSegment();

    ChangeLogCache->Create(Version.SegmentId, changeLog->GetRecordCount(), Epoch);
}

TMetaVersion TDecoratedMetaState::GetVersion() const
{
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    // NB: No need to take a spinlock here since both
    // GetVersion and UpdateVersion have same affinity.
    return Version;
}

TMetaVersion TDecoratedMetaState::GetVersionAsync() const
{
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(VersionSpinLock);
    return Version;
}

TMetaVersion TDecoratedMetaState::GetReachableVersionAsync() const
{
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(VersionSpinLock);
    return ReachableVersion;
}

TMetaVersion TDecoratedMetaState::GetPingVersion() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return PingVersion;
}

void TDecoratedMetaState::SetPingVersion(const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    PingVersion = version;
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
