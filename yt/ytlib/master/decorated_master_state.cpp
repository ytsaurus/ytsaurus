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

IInvoker::TPtr TDecoratedMasterState::GetInvoker() const
{
    return State->GetInvoker();
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
    AdvanceChangeCount();
}

void TDecoratedMasterState::ApplyChange(IAction::TPtr changeAction)
{
    changeAction->Do();
    AdvanceChangeCount();
}

void TDecoratedMasterState::AdvanceChangeCount()
{
    UpdateStateId(TMasterStateId(StateId.SegmentId, StateId.ChangeCount + 1));
}

TAsyncChangeLog::TAppendResult::TPtr TDecoratedMasterState::LogChange(
    const TSharedRef& changeData)
{
    TCachedAsyncChangeLog::TPtr cachedChangeLog = ChangeLogCache->Get(StateId.SegmentId);
    if (~cachedChangeLog == NULL) {
        LOG_FATAL("The current changelog %d is missing", StateId.SegmentId);
    }

    return cachedChangeLog->Append(
        StateId.ChangeCount,
        changeData);
}

void TDecoratedMasterState::AdvanceSegment()
{
    UpdateStateId(TMasterStateId(StateId.SegmentId + 1, 0));
   
    LOG_INFO("Switched to a new segment %d", StateId.SegmentId);
}

void TDecoratedMasterState::RotateChangeLog()
{
    TCachedAsyncChangeLog::TPtr currentChangeLog = ChangeLogCache->Get(StateId.SegmentId);
    YASSERT(~currentChangeLog != NULL);

    currentChangeLog->Finalize();

    AdvanceSegment();

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
        TCachedAsyncChangeLog::TPtr changeLog = ChangeLogCache->Get(segmentId);
        if (~changeLog == NULL) {
            AvailableStateId = currentStateId;
            break;
        }

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

void TDecoratedMasterState::OnStartLeading()
{
    State->OnStartLeading();
}

void TDecoratedMasterState::OnStopLeading()
{
    State->OnStopLeading();
}

void TDecoratedMasterState::OnStartFollowing()
{
    State->OnStartFollowing();
}

void TDecoratedMasterState::OnStopFollowing()
{
    State->OnStopFollowing();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
