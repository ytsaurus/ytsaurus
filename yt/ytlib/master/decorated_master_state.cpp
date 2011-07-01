#include "decorated_master_state.h"
#include "change_log_cache.h"
#include "snapshot_store.h"

#include "../actions/action_util.h"
#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("DecoratedMasterState");

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

TMasterStateId TDecoratedMasterState::GetStateId() const
{
    return StateId;
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
    LOG_INFO("Saving snapshot");

    return State->Save(output)->Apply(FromMethod(
        &TDecoratedMasterState::OnSave,
        TPtr(this),
        TInstant::Now()));
}

TVoid TDecoratedMasterState::OnSave(TVoid, TInstant savingStarted)
{
    TInstant savingFinished = TInstant::Now();

    LOG_INFO("Snapshot saved in %.3f s", (savingFinished - savingStarted).SecondsFloat());

    return TVoid();
}

void TDecoratedMasterState::Load(i32 segmentId, TInputStream& input)
{
    LOG_INFO("Loading snapshot %d", segmentId);
    
    TInstant loadingStarted = TInstant::Now();
    State->Load(input);
    StateId = TMasterStateId(segmentId, 0);
    TInstant loadingFinished = TInstant::Now();

    LOG_INFO("Snapshot loaded in %.3f s", (loadingFinished - loadingStarted).SecondsFloat());
}

void TDecoratedMasterState::ApplyChange(const TSharedRef& changeData)
{
    State->ApplyChange(changeData);
    ++StateId.ChangeCount;

    AvailableStateId = Max(AvailableStateId, StateId);
}

TChangeLogWriter::TAppendResult::TPtr TDecoratedMasterState::LogAndApplyChange(
    const TSharedRef& changeData)
{
    TCachedChangeLog::TPtr changeLog = ChangeLogCache->Get(StateId.SegmentId);
    if (~changeLog == NULL) {
        LOG_FATAL("The current changelog %d is missing", StateId.SegmentId);
    }

    TChangeLogWriter& writer = changeLog->GetWriter();
    TChangeLogWriter::TAppendResult::TPtr appendResult = writer.Append(
        StateId.ChangeCount,
        changeData);

    ApplyChange(changeData);

    return appendResult;
}

void TDecoratedMasterState::AdvanceSegment()
{
    ++StateId.SegmentId;
    StateId.ChangeCount = 0;

    AvailableStateId = Max(AvailableStateId, StateId);
    
    LOG_INFO("Switched master state to a new segment %d",
        StateId.SegmentId);
}

void TDecoratedMasterState::RotateChangeLog()
{
    TCachedChangeLog::TPtr currentCachedChangeLog = ChangeLogCache->Get(StateId.SegmentId);
    YASSERT(~currentCachedChangeLog != NULL);

    currentCachedChangeLog->GetWriter().Close();

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
            currentCachedChangeLog->GetWriter().Close();
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

TMasterStateId TDecoratedMasterState::GetAvailableStateId() const
{
    return AvailableStateId;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
