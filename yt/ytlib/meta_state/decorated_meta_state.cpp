#include "stdafx.h"
#include "decorated_meta_state.h"
#include "private.h"
#include "change_log_cache.h"
#include "snapshot_store.h"
#include "meta_state.h"
#include "snapshot.h"
#include "serialize.h"
#include "response_keeper.h"
#include "config.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static NProfiling::TProfiler& Profiler = MetaStateProfiler;

////////////////////////////////////////////////////////////////////////////////

class TDecoratedMetaState::TGuardedUserInvoker
    : public IInvoker
{
public:
    TGuardedUserInvoker(TDecoratedMetaStatePtr metaState, IInvokerPtr underlyingInvoker)
        : MetaState(metaState)
        , UnderlyingInvoker(underlyingInvoker)
    { }

    virtual bool Invoke(const TClosure& action) override
    {
        if (!MetaState->AcquireUserEnqueueLock()) {
            return false;
        }
        if (MetaState->GetStatus() != EPeerStatus::Leading &&
            MetaState->GetStatus() != EPeerStatus::Following)
        {
            MetaState->ReleaseUserEnqueueLock();
            return false;
        }
        bool result = UnderlyingInvoker->Invoke(action);
        MetaState->ReleaseUserEnqueueLock();
        return result;
    }

private:
    TDecoratedMetaStatePtr MetaState;
    IInvokerPtr UnderlyingInvoker;

};

////////////////////////////////////////////////////////////////////////////////

class TDecoratedMetaState::TSystemInvoker
    : public IInvoker
{
public:
    explicit TSystemInvoker(TDecoratedMetaState* metaState)
        : MetaState(metaState)
    { }

    virtual bool Invoke(const TClosure& action) override
    {
        auto metaState = MakeStrong(MetaState);
        metaState->AcquireSystemLock();
        
        bool result = metaState->StateInvoker->Invoke(BIND([=] () {
            action.Run();
            metaState->ReleaseSystemLock();
        }));

        if (!result) {
            metaState->ReleaseSystemLock();
        }

        return result;
    }

private:
    TDecoratedMetaState* MetaState;

};

////////////////////////////////////////////////////////////////////////////////

TDecoratedMetaState::TDecoratedMetaState(
    TPersistentStateManagerConfigPtr config,
    IMetaStatePtr state,
    IInvokerPtr stateInvoker,
    IInvokerPtr controlInvoker,
    TSnapshotStorePtr snapshotStore,
    TChangeLogCachePtr changeLogCache)
    : Status_(EPeerStatus::Stopped)
    , State(state)
    , StateInvoker(stateInvoker)
    , UserEnqueueLock(0)
    , SystemLock(0)
    , SystemInvoker(New<TSystemInvoker>(this))
    , SnapshotStore(snapshotStore)
    , ChangeLogCache(changeLogCache)
    , Started(false)
    , MutationContext(NULL)
{
    YCHECK(config);
    YCHECK(state);
    YCHECK(snapshotStore);
    YCHECK(changeLogCache);

    VERIFY_INVOKER_AFFINITY(StateInvoker, StateThread);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);

    ResponseKeeper = New<TResponseKeeper>(  
        config->ResponseKeeper,
        StateInvoker);
}

void TDecoratedMetaState::Start()
{
    YCHECK(!Started);
    ComputeReachableVersion();
    Started = true;
}

void TDecoratedMetaState::OnStartLeading()
{
    YCHECK(Status_ == EPeerStatus::Stopped);
    Status_ = EPeerStatus::LeaderRecovery;
}

void TDecoratedMetaState::OnLeaderRecoveryComplete()
{
    YCHECK(Status_ == EPeerStatus::LeaderRecovery);
    Status_ = EPeerStatus::Leading;
}

void TDecoratedMetaState::OnStopLeading()
{
    YCHECK(Status_ == EPeerStatus::Leading || Status_ == EPeerStatus::LeaderRecovery);
    Status_ = EPeerStatus::Stopped;
}

void TDecoratedMetaState::OnStartFollowing()
{
    YCHECK(Status_ == EPeerStatus::Stopped);
    Status_ = EPeerStatus::FollowerRecovery;
}

void TDecoratedMetaState::OnFollowerRecoveryComplete()
{
    YCHECK(Status_ == EPeerStatus::FollowerRecovery);
    Status_ = EPeerStatus::Following;
}

void TDecoratedMetaState::OnStopFollowing()
{
    YCHECK(Status_ == EPeerStatus::Following || Status_ == EPeerStatus::FollowerRecovery);
    Status_ = EPeerStatus::Stopped;
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
        bool isLast = !ChangeLogCache->Get(segmentId + 1).IsOK();

        LOG_DEBUG("Changelog found (ChangeLogId: %d, RecordCount: %d, PrevRecordCount: %d, IsLast: %s)",
            segmentId,
            changeLog->GetRecordCount(),
            changeLog->GetPrevRecordCount(),
            ~FormatBool(isLast));

        currentVersion = TMetaVersion(segmentId, changeLog->GetRecordCount());
    }

    LOG_INFO("Reachable version is %s", ~ReachableVersion.ToString());
}

IInvokerPtr TDecoratedMetaState::CreateGuardedUserInvoker(IInvokerPtr underlyingInvoker)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return New<TGuardedUserInvoker>(this, underlyingInvoker);
}

IInvokerPtr TDecoratedMetaState::GetSystemInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SystemInvoker;
}

IMetaStatePtr TDecoratedMetaState::GetState()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return State;
}

void TDecoratedMetaState::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(Started);

    State->Clear();
    ResponseKeeper->Clear();
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

bool TDecoratedMetaState::FindKeptResponse(const TMutationId& id, TSharedRef* data)
{
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    return ResponseKeeper->FindResponse(id, data);
}

void TDecoratedMetaState::ApplyMutation(TMutationContext* context) throw()
{
    YASSERT(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    MutationContext = context;
    const auto& action = context->GetRequestAction();
    if (action.IsNull()) {
        State->ApplyMutation(context);
    } else {
        action.Run();
    }
    MutationContext = NULL;

    if (context->GetId() != NullMutationId) {
        ResponseKeeper->RegisterResponse(context->GetId(), context->GetResponseData());
    }

    IncrementRecordCount();
}

void TDecoratedMetaState::ApplyMutation(const TSharedRef& recordData) throw()
{
    NProto::TMutationHeader header;
    TSharedRef requestData;
    DeserializeMutationRecord(recordData, &header, &requestData);

    TMutationRequest request(
        header.mutation_type(),
        requestData);
    TMutationContext context(
        Version,
        request,
        TInstant(header.timestamp()),
        header.random_seed());
    ApplyMutation(&context);
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
        LOG_FATAL(result, "Cannot obtain the current changelog");
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

void TDecoratedMetaState::RotateChangeLog(const TEpochId& epochId)
{
    YCHECK(Started);
    VERIFY_THREAD_AFFINITY(StateThread);

    auto changeLog = GetCurrentChangeLog();
    changeLog->Finalize();

    AdvanceSegment();

    ChangeLogCache->Create(Version.SegmentId, changeLog->GetRecordCount(), epochId);
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

    return MutationContext;
}

bool TDecoratedMetaState::AcquireUserEnqueueLock()
{
    if (SystemLock != 0) {
        return false;
    }
    AtomicIncrement(UserEnqueueLock);
    if (AtomicGet(SystemLock) != 0) {
        AtomicDecrement(UserEnqueueLock);
        return false;
    }
    return true;
}

void TDecoratedMetaState::ReleaseUserEnqueueLock()
{
    AtomicDecrement(UserEnqueueLock);
}

void TDecoratedMetaState::AcquireSystemLock()
{
    AtomicIncrement(SystemLock);
    while (AtomicGet(UserEnqueueLock) != 0) {
        SpinLockPause();
    }
}

void TDecoratedMetaState::ReleaseSystemLock()
{
    AtomicDecrement(SystemLock);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
