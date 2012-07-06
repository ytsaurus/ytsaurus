#include "stdafx.h"
#include "decorated_meta_state.h"
#include "private.h"
#include "change_log_cache.h"
#include "snapshot_store.h"
#include "meta_state.h"
#include "snapshot.h"
#include "serialize.h"

#include <ytlib/actions/callback.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static NProfiling::TProfiler& Profiler = MetaStateProfiler;

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
    YCHECK(state);
    YCHECK(stateInvoker);
    YCHECK(snapshotStore);
    YCHECK(changeLogCache);
    VERIFY_INVOKER_AFFINITY(StateInvoker, StateThread);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
}

void TDecoratedMetaState::Start()
{
    YCHECK(!Started);
    ComputeReachableVersion();
    Started = true;
}

void TDecoratedMetaState::SetEpoch(const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(Started);
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
        LOG_INFO("Found latest snapshot %d", maxSnapshotId);
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

        LOG_DEBUG("Found changelog %d (RecordCount: %d, PrevRecordCount: %d, IsFinal: %s)",
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

    return StateInvoker;
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
    YCHECK(output);
    YCHECK(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    State->Save(output);
}

void TDecoratedMetaState::Load(
    i32 segmentId,
    TInputStream* input)
{
    YCHECK(input);
    YCHECK(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    LOG_INFO("Started loading snapshot %d", segmentId);

    CurrentChangeLog.Reset();
    UpdateVersion(TMetaVersion(segmentId, 0));

    PROFILE_TIMING ("/snapshot_load_time") {
        State->Load(input);
    }

    LOG_INFO("Finished loading snapshot");
}

void TDecoratedMetaState::ApplyMutation(const TSharedRef& recordData)
{
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    try {
        EnterMutation(recordData);
        State->ApplyMutation(*MutationContext);
        LeaveMutation();
    } catch (const std::exception& ex) {
        LOG_FATAL("Error applying mutation (Version: %s)\n%s",
            ~Version.ToString(),
            ex.what());
    }

    IncrementRecordCount();
}

void TDecoratedMetaState::ApplyMutation(
    const TSharedRef& recordData,
    const TClosure& mutationAction)
{
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    try {
        EnterMutation(recordData);
        mutationAction.Run();
        LeaveMutation();
    } catch (const std::exception& ex) {
        LOG_FATAL("Error applying mutation (Version: %s)\n%s",
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

TFuture<void> TDecoratedMetaState::LogMutation(
    const TMetaVersion& version,
    const TSharedRef& recordData)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(version.SegmentId == Version.SegmentId);

    auto changeLog = GetCurrentChangeLog();
    return changeLog->Append(version.RecordCount, recordData);
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
    YCHECK(Started);
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

TMutationContext* TDecoratedMetaState::GetMutationContext()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return ~MutationContext;
}

void TDecoratedMetaState::EnterMutation(const TSharedRef& recordData)
{
    NProto::TMutationHeader mutationHeader;
    TSharedRef mutationData;
    DeserializeMutationRecord(recordData, &mutationHeader, &mutationData);
    MutationContext.Reset(new TMutationContext(
        mutationHeader.mutation_type(),
        mutationData,
        TInstant(mutationHeader.timestamp()),
        mutationHeader.random_seed()));
}

void TDecoratedMetaState::LeaveMutation()
{
    MutationContext.Destroy();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
