#pragma once

#include "common.h"
#include "master_state.h"
#include "snapshot_store.h"
#include "change_log_cache.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: make refcounted
// TODO: why this is not inherited from IMasterState ?
class TDecoratedMasterState
{
public:
    TDecoratedMasterState(
        IMasterState::TPtr state,
        TSnapshotStore* snapshotStore,
        TChangeLogCache::TPtr changeLogCache);

    TMasterStateId GetStateId() const;
    IMasterState::TPtr GetState() const;
    TVoid Clear();
    TAsyncResult<TVoid>::TPtr Save(TOutputStream& output);
    void Load(i32 segmentId, TInputStream& input);
    void ApplyChange(TRef changeData);
    void NextSegment();
    void NextChangeLog();
    TMasterStateId GetAvailableStateId() const;

private:
    void ComputeAvailableStateId();
    TVoid OnSave(TVoid, TInstant savingStarted);

    IMasterState::TPtr State;
    TSnapshotStore* SnapshotStore;
    TChangeLogCache::TPtr ChangeLogCache;
    TMasterStateId StateId;
    TMasterStateId AvailableStateId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
