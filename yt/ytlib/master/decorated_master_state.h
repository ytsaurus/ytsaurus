#pragma once

#include "common.h"
#include "master_state.h"
#include "snapshot_store.h"
#include "change_log_cache.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TDecoratedMasterState
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TDecoratedMasterState> TPtr;

    TDecoratedMasterState(
        IMasterState::TPtr state,
        TSnapshotStore* snapshotStore,
        TChangeLogCache::TPtr changeLogCache);

    TMasterStateId GetStateId() const;
    TMasterStateId GetAvailableStateId() const;

    IMasterState::TPtr GetState() const;

    TVoid Clear();
    
    TAsyncResult<TVoid>::TPtr Save(TOutputStream& output);
    TAsyncResult<TVoid>::TPtr Load(i32 segmentId, TInputStream& input);
    
    void ApplyChange(const TSharedRef& changeData);
    TChangeLogWriter::TAppendResult::TPtr LogAndApplyChange(const TSharedRef& changeData);
    
    void AdvanceSegment();
    void RotateChangeLog();

private:
    void ComputeAvailableStateId();
    void UpdateStateId(const TMasterStateId& newStateId);
    TVoid OnSave(TVoid, TInstant started);
    TVoid OnLoad(TVoid, TInstant started);

    IMasterState::TPtr State;
    TSnapshotStore* SnapshotStore;
    TChangeLogCache::TPtr ChangeLogCache;

    TMasterStateId StateId;
    TMasterStateId AvailableStateId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
