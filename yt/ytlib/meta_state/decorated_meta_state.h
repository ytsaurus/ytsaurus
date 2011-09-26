#pragma once

#include "common.h"
#include "meta_state.h"
#include "snapshot_store.h"
#include "change_log_cache.h"

#include "../misc/thread_affinity.h"

namespace NYT {
namespace NMetaState {

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

    /*!
     * \note Thread affinity: any
     */
    IInvoker::TPtr GetInvoker() const;

    /*!
     * \note Thread affinity: StateThread
     */
    TMetaVersion GetVersion() const;
    /*!
     * \note Thread affinity: any
     */
    TMetaVersion GetNextVersion() const;
    /*!
     * \note Thread affinity: any
     */
    IMetaState::TPtr GetState() const;

    /*!
     * \note Thread affinity: StateThread
     */
    void Clear();
    
    /*!
     * \note Thread affinity: StateThread
     */
    TAsyncResult<TVoid>::TPtr Save(TOutputStream* output);
    /*!
     * \note Thread affinity: StateThread
     */
    TAsyncResult<TVoid>::TPtr Load(i32 segmentId, TInputStream* input);
    
    /*!
     * \note Thread affinity: StateThread
     */
    void ApplyChange(const TSharedRef& changeData);

    /*!
     * \note Thread affinity: StateThread
     */
    void ApplyChange(IAction::TPtr changeAction);

    /*!
     * \note Thread affinity: StateThread
     */
    TAsyncChangeLog::TAppendResult::TPtr LogChange(const TSharedRef& changeData);
    
    /*!
     * \note Thread affinity: StateThread
     */
    void AdvanceSegment();

    /*!
     * \note Thread affinity: StateThread
     */
    void RotateChangeLog();

    /*!
     * \note Thread affinity: StateThread
     */
    void OnStartLeading();
    /*!
     * \note Thread affinity: StateThread
     */
    void OnStopLeading();
    /*!
     * \note Thread affinity: StateThread
     */
    void OnStartFollowing();
    /*!
     * \note Thread affinity: StateThread
     */
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

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
