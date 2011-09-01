#include "decorated_meta_state.h"
#include "change_log_cache.h"
#include "snapshot_store.h"

#include "../actions/action_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TDecoratedMetaState::TDecoratedMetaState(
    IMetaState::TPtr state,
    TSnapshotStore::TPtr snapshotStore,
    TChangeLogCache::TPtr changeLogCache)
    : State(state)
    , SnapshotStore(snapshotStore)
    , ChangeLogCache(changeLogCache)
{
    state->Clear();
    ComputeNextVersion();
}

IInvoker::TPtr TDecoratedMetaState::GetInvoker() const
{
    return State->GetInvoker();
}

IMetaState::TPtr TDecoratedMetaState::GetState() const
{
    return State;
}

TVoid TDecoratedMetaState::Clear()
{
    State->Clear();
    Version = TMetaVersion();
    return TVoid();
}

TAsyncResult<TVoid>::TPtr TDecoratedMetaState::Save(TOutputStream* output)
{
    LOG_INFO("Started saving snapshot");

    return State->Save(output)->Apply(FromMethod(
        &TDecoratedMetaState::OnSave,
        TPtr(this),
        TInstant::Now()));
}

TVoid TDecoratedMetaState::OnSave(TVoid, TInstant started)
{
    TInstant finished = TInstant::Now();
    LOG_INFO("Finished saving snapshot (Time: %.3f)", (finished - started).SecondsFloat());
    return TVoid();
}

TAsyncResult<TVoid>::TPtr TDecoratedMetaState::Load(
    i32 segmentId,
    TInputStream* input)
{
    LOG_INFO("Started loading snapshot %d", segmentId);
    UpdateVersion(TMetaVersion(segmentId, 0));
    return State->Load(input)->Apply(FromMethod(
        &TDecoratedMetaState::OnLoad,
        TPtr(this),
        TInstant::Now()));
}

TVoid TDecoratedMetaState::OnLoad(TVoid, TInstant started)
{
    TInstant finished = TInstant::Now();
    LOG_INFO("Finished loading snapshot (Time: %.3f)", (finished - started).SecondsFloat());
    return TVoid();
}

void TDecoratedMetaState::ApplyChange(const TSharedRef& changeData)
{
    State->ApplyChange(changeData);
    IncrementRecordCount();
}

void TDecoratedMetaState::ApplyChange(IAction::TPtr changeAction)
{
    changeAction->Do();
    IncrementRecordCount();
}

void TDecoratedMetaState::IncrementRecordCount()
{
    UpdateVersion(TMetaVersion(Version.SegmentId, Version.RecordCount + 1));
}

TAsyncChangeLog::TAppendResult::TPtr TDecoratedMetaState::LogChange(
    const TSharedRef& changeData)
{
    TCachedAsyncChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(Version.SegmentId);
    if (~cachedChangeLog == NULL) {
        LOG_FATAL("The current changelog %d is missing", Version.SegmentId);
    }

    return cachedChangeLog->Append(
        Version.RecordCount,
        changeData);
}

void TDecoratedMetaState::AdvanceSegment()
{
    UpdateVersion(TMetaVersion(Version.SegmentId + 1, 0));
   
    LOG_INFO("Switched to a new segment %d", Version.SegmentId);
}

void TDecoratedMetaState::RotateChangeLog()
{
    TCachedAsyncChangeLog::TPtr currentChangeLog = ChangeLogCache->Get(Version.SegmentId);
    YASSERT(~currentChangeLog != NULL);

    currentChangeLog->Finalize();

    AdvanceSegment();

    ChangeLogCache->Create(Version.SegmentId, currentChangeLog->GetRecordCount());
}

void TDecoratedMetaState::ComputeNextVersion()
{
    i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();
    if (maxSnapshotId == NonexistingSnapshotId) {
        LOG_INFO("No snapshots found");
        // Let's pretend we have snapshot 0.
        maxSnapshotId = 0;
    } else {
        TSnapshotReader::TPtr snapshotReader = SnapshotStore->GetReader(maxSnapshotId);
        LOG_INFO("Latest snapshot is %d", maxSnapshotId);
    }

    TMetaVersion currentVersion = TMetaVersion(maxSnapshotId, 0);

    for (i32 segmentId = maxSnapshotId; ; ++segmentId) {
        TCachedAsyncChangeLog::TPtr changeLog = ChangeLogCache->Get(segmentId);
        if (~changeLog == NULL) {
            NextVersion = currentVersion;
            break;
        }

        bool isFinal = ~ChangeLogCache->Get(segmentId + 1) == NULL;

        LOG_DEBUG("Found changelog (Id: %d, RecordCount: %d, PrevRecordCount: %d, IsFinal: %s)",
            segmentId,
            changeLog->GetRecordCount(),
            changeLog->GetPrevRecordCount(),
            ~ToString(isFinal));

        currentVersion = TMetaVersion(segmentId, changeLog->GetRecordCount());
    }

    LOG_INFO("Available state is %s", ~NextVersion.ToString());
}         

TMetaVersion TDecoratedMetaState::GetVersion() const
{
    return Version;
}

TMetaVersion TDecoratedMetaState::GetNextVersion() const
{
    return NextVersion;
}

void TDecoratedMetaState::UpdateVersion(const TMetaVersion& newVersion)
{
    Version = newVersion;
    NextVersion = Max(NextVersion, Version);
}

void TDecoratedMetaState::OnStartLeading()
{
    State->OnStartLeading();
}

void TDecoratedMetaState::OnStopLeading()
{
    State->OnStopLeading();
}

void TDecoratedMetaState::OnStartFollowing()
{
    State->OnStartFollowing();
}

void TDecoratedMetaState::OnStopFollowing()
{
    State->OnStopFollowing();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
