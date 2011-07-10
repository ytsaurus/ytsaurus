#include "decorated_master_state.h"
#include "change_log_cache.h"
#include "snapshot_store.h"

#include "../actions/action_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MasterLogger;

////////////////////////////////////////////////////////////////////////////////

TDecoratedMasterState::TDecoratedMasterState(
    IMasterState::TPtr state,
    TSnapshotStore::TPtr snapshotStore,
    TChangeLogCache::TPtr changeLogCache)
    : State(state)
    , SnapshotStore(snapshotStore)
    , ChangeLogCache(changeLogCache)
{
    state->Clear();
    ComputeAvailableStateId();
}

IMasterState::TPtr TDecoratedMasterState::GetState() const
{
    return State;
}

TVoid TDecoratedMasterState::Clear()
{
    State->Clear();
    StateId = TMasterStateId();
    return TVoid();
}

TAsyncResult<TVoid>::TPtr TDecoratedMasterState::Save(TOutputStream& output)
{
    LOG_INFO("Started saving snapshot");

    return State->Save(output)->Apply(FromMethod(
        &TDecoratedMasterState::OnSave,
        TPtr(this),
        TInstant::Now()));
}

TVoid TDecoratedMasterState::OnSave(TVoid, TInstant started)
{
    TInstant finished = TInstant::Now();
    LOG_INFO("Finished saving snapshot (Time: %.3f)", (finished - started).SecondsFloat());
    return TVoid();
}

TAsyncResult<TVoid>::TPtr TDecoratedMasterState::Load(i32 segmentId, TInputStream& input)
{
    LOG_INFO("Started loading snapshot %d", segmentId);
    UpdateStateId(TMasterStateId(segmentId, 0));
    return State->Load(input)->Apply(FromMethod(
        &TDecoratedMasterState::OnLoad,
        TPtr(this),
        TInstant::Now()));
}

TVoid TDecoratedMasterState::OnLoad(TVoid, TInstant started)
{
    TInstant finished = TInstant::Now();
    LOG_INFO("Finished loading snapshot (Time: %.3f)", (finished - started).SecondsFloat());
    return TVoid();
}

void TDecoratedMasterState::ApplyChange(const TSharedRef& changeData)
{
    State->ApplyChange(changeData);
    UpdateStateId(TMasterStateId(StateId.SegmentId, StateId.ChangeCount + 1));
}

TAsyncChangeLog::TAppendResult::TPtr TDecoratedMasterState::LogAndApplyChange(
    const TSharedRef& changeData)
{
    TCachedChangeLog::TPtr changeLog = ChangeLogCache->Get(StateId.SegmentId);
    if (~changeLog == NULL) {
        LOG_FATAL("The current changelog %d is missing", StateId.SegmentId);
    }

    TAsyncChangeLog& asyncChangeLog = changeLog->GetWriter();
    TAsyncChangeLog::TAppendResult::TPtr appendResult = asyncChangeLog.Append(
        StateId.ChangeCount,
        changeData);

    ApplyChange(changeData);

    return appendResult;
}

void TDecoratedMasterState::AdvanceSegment()
{
    UpdateStateId(TMasterStateId(StateId.SegmentId + 1, 0));
   
    LOG_INFO("Switched to a new segment %d", StateId.SegmentId);
}

void TDecoratedMasterState::RotateChangeLog()
{
    TCachedChangeLog::TPtr currentCachedChangeLog = ChangeLogCache->Get(StateId.SegmentId);
    YASSERT(~currentCachedChangeLog != NULL);

    currentCachedChangeLog->GetWriter().Finalize();

    AdvanceSegment();

    TChangeLog::TPtr currentChangeLog = currentCachedChangeLog->GetChangeLog();
    ChangeLogCache->Create(StateId.SegmentId, currentChangeLog->GetRecordCount());
}

void TDecoratedMasterState::ComputeAvailableStateId()
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

    TMasterStateId currentStateId = TMasterStateId(maxSnapshotId, 0);

    for (i32 segmentId = maxSnapshotId; ; ++segmentId) {
        TCachedChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(segmentId);
        if (~cachedChangeLog == NULL) {
            AvailableStateId = currentStateId;
            break;
        }

        TChangeLog::TPtr changeLog = cachedChangeLog->GetChangeLog();
        bool isFinal = ~ChangeLogCache->Get(segmentId + 1) == NULL;

        LOG_DEBUG("Found changelog (Id: %d, RecordCount: %d, PrevRecordCount: %d, IsFinal: %s)",
            segmentId,
            changeLog->GetRecordCount(),
            changeLog->GetPrevRecordCount(),
            ~ToString(isFinal));

        currentStateId = TMasterStateId(segmentId, changeLog->GetRecordCount());
    }

    LOG_INFO("Available state is %s", ~AvailableStateId.ToString());
}         

TMasterStateId TDecoratedMasterState::GetStateId() const
{
    return StateId;
}

TMasterStateId TDecoratedMasterState::GetAvailableStateId() const
{
    return AvailableStateId;
}

void TDecoratedMasterState::UpdateStateId(const TMasterStateId& newStateId)
{
    StateId = newStateId;
    AvailableStateId = Max(AvailableStateId, StateId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
