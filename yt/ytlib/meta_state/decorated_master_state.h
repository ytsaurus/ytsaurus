#pragma once

#include "common.h"
#include "master_state.h"
#include "snapshot_store.h"
#include "change_log_cache.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TDecoratedMetaState
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TDecoratedMetaState> TPtr;

    TDecoratedMetaState(
        IMetaState::TPtr state,
        TSnapshotStore::TPtr snapshotStore,
        TChangeLogCache::TPtr changeLogCache);

    IInvoker::TPtr GetInvoker() const;

    TMetaVersion GetVersion() const;
    TMetaVersion GetNextVersion() const;

    IMetaState::TPtr GetState() const;

    TVoid Clear();
    
    TAsyncResult<TVoid>::TPtr Save(TOutputStream* output);
    TAsyncResult<TVoid>::TPtr Load(i32 segmentId, TInputStream* input);
    
    void ApplyChange(const TSharedRef& changeData);
    void ApplyChange(IAction::TPtr changeAction);
    TAsyncChangeLog::TAppendResult::TPtr LogChange(const TSharedRef& changeData);
    
    void AdvanceSegment();
    void RotateChangeLog();

    void OnStartLeading();
    void OnStopLeading();
    void OnStartFollowing();
    void OnStopFollowing();

private:
    void IncrementRecordCount();
    void ComputeNextVersion();
    void UpdateVersion(const TMetaVersion& newVersion);
    TVoid OnSave(TVoid, TInstant started);
    TVoid OnLoad(TVoid, TInstant started);

    IMetaState::TPtr State;
    TSnapshotStore::TPtr SnapshotStore;
    TChangeLogCache::TPtr ChangeLogCache;

    TMetaVersion Version;
    TMetaVersion NextVersion;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
