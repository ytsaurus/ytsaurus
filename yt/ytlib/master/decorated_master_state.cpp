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
    TSnapshotStore* snapshotStore,
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
    LOG_INFO("Finishied saving snapshot, took %.3f s", (finished - started).SecondsFloat());
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
    LOG_INFO("Finished loading snapshot, took %.3f s", (finished - started).SecondsFloat());
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
   
    LOG_INFO("Switched master state to a new segment %d",
        StateId.SegmentId);
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
    if (maxSnapshotId < 0) {
        LOG_INFO("No snapshots found");
        // Let's pretend we have snapshot 0.
        maxSnapshotId = 0;
    } else {
        LOG_INFO("Latest snapshot is %d", maxSnapshotId);
    }

    TMasterStateId currentStateId = TMasterStateId(maxSnapshotId, 0);

    // TODO: check prevchangecount between the snapshot and the first log

    for (i32 segmentId = maxSnapshotId; ; ++segmentId) {
        TCachedChangeLog::TPtr currentCachedChangeLog = ChangeLogCache->Get(segmentId);
        if (~currentCachedChangeLog == NULL) {
            AvailableStateId = currentStateId;
            break;
        }

        currentStateId = TMasterStateId(
            segmentId,
            currentCachedChangeLog->GetChangeLog()->GetRecordCount());
        
        i32 nextSegmentId = segmentId + 1;
        TCachedChangeLog::TPtr nextCachedChangeLog = ChangeLogCache->Get(nextSegmentId);
        if (~nextCachedChangeLog == NULL) {
            AvailableStateId = currentStateId;
            break;
        }

        if (!currentCachedChangeLog->GetChangeLog()->IsFinalized()) {
            LOG_WARNING("Changelog %d was not finalized", segmentId);
            currentCachedChangeLog->GetWriter().Finalize();
        }

        TMasterStateId prevStateId = nextCachedChangeLog->GetChangeLog()->GetPrevStateId();
        if (prevStateId != currentStateId) {
            LOG_FATAL("Previous state is mismatch in changelog %d: expected (%d, %d), found (%d, %d)",
            nextSegmentId,
            currentStateId.SegmentId, currentStateId.ChangeCount,
            prevStateId.SegmentId, prevStateId.ChangeCount);
        }

        if (!currentCachedChangeLog->GetChangeLog()->IsFinalized()) {
            LOG_FATAL("Changelog %d is not finalized", segmentId);
        }
    }

    LOG_INFO("Available state is %s",
        ~AvailableStateId.ToString());
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
