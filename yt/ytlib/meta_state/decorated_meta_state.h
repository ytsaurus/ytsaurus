#pragma once

#include "public.h"
#include "meta_version.h"

// TODO(babenko): get rid of this
#include "async_change_log.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/ref.h>
#include <ytlib/actions/action.h>
#include <ytlib/actions/invoker.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TDecoratedMetaState
    : public TRefCounted
{
public:
    TDecoratedMetaState(
        IMetaState* state,
        IInvoker* stateInvoker,
        TSnapshotStore* snapshotStore,
        TChangeLogCache* changeLogCache);

    //! Initializes the instance.
    void Start();

    //! Returns the invoker used for updating the state.
    /*!
     * \note Thread affinity: any
     */
    IInvoker* GetStateInvoker() const;

    //! Returns the current version of the state.
    /*!
     * \note Thread affinity: StateThread
     */
    TMetaVersion GetVersion() const;

    //! Same as #GetVersion but can be called from an arbitrary thread.
    /*!
     * \note Thread affinity: any
     */
    TMetaVersion GetVersionAsync() const;

    //! Returns the maximum reachable version of the state that
    //! can be obtained by reading the local snapshots, changelogs.
    /*!
     *  It is always no smaller than #GetVersion.
     *  
     *  \note Thread affinity: StateThread
     */
    TMetaVersion GetReachableVersion() const;

    //! Same as #GetReachableVersion but call be called from an arbitrary thread.
    /*!
     *  \note Thread affinity: any
     */
    TMetaVersion GetReachableVersionAsync() const;

    //! Returns the underlying state.
    /*!
     * \note Thread affinity: any
     */
    IMetaState* GetState() const;

    //! Delegates the call to IMetaState::Clear.
    /*!
     * \note Thread affinity: StateThread
     */
    void Clear();
    
    //! Delegates the call to IMetaState::Save.
    /*!
     * \note Thread affinity: StateThread
     */
    void Save(TOutputStream* output);

    //! Delegates the call to IMetaState::Load and updates the version.
    /*!
     * \note Thread affinity: StateThread
     */
    void Load(i32 segmentId, TInputStream* input);
    
    //! Delegates the call to IMetaState::ApplyChange and updates the version.
    /*!
     * \note Thread affinity: StateThread
     */
    void ApplyChange(const TSharedRef& changeData);

    //! Executes a given action and updates the version.
    /*!
     * \note Thread affinity: StateThread
     */
    void ApplyChange(IAction::TPtr changeAction);

    //! Appends a new record into an appropriate changelog.
    /*!
     * \note Thread affinity: StateThread
     */
    TAsyncChangeLog::TAppendResult::TPtr LogChange(
        const TMetaVersion& version,
        const TSharedRef& changeData);
    
    //! Finalizes the current changelog, advances the segment, and creates a new changelog.
    /*!
     * \note Thread affinity: StateThread
     */
    void RotateChangeLog();

    //! Updates the version so as to switch to a new segment.
    /*!
     * \note Thread affinity: StateThread
     */
    void AdvanceSegment();

private:
    IMetaStatePtr State;
    IInvoker::TPtr StateInvoker;
    TSnapshotStorePtr SnapshotStore;
    TChangeLogCachePtr ChangeLogCache;
    bool Started;

    TCachedAsyncChangeLogPtr CurrentChangeLog;

    TSpinLock VersionSpinLock;
    TMetaVersion Version;
    TMetaVersion ReachableVersion;

    void IncrementRecordCount();
    void ComputeReachableVersion();
    void UpdateVersion(const TMetaVersion& newVersion);
    TCachedAsyncChangeLogPtr GetCurrentChangeLog();

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
