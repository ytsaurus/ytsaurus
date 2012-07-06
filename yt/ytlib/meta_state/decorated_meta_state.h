#pragma once

#include "public.h"
#include "meta_version.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/ref.h>
#include <ytlib/actions/callback_forward.h>
#include <ytlib/actions/invoker.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TDecoratedMetaState
    : public TRefCounted
{
public:
    TDecoratedMetaState(
        IMetaStatePtr state,
        IInvokerPtr stateInvoker,
        IInvokerPtr controlInvoker,
        TSnapshotStorePtr snapshotStore,
        TChangeLogCachePtr changeLogCache);

    //! Initializes the instance.
    void Start();

    //! Returns current epoch id.
    /*!
     * \note Thread affinity: any
     */
    const TEpoch& GetEpoch() const;

    //! Set new epoch id.
    /*!
     *  \note Thread affinity: ControlThread
     */
    void SetEpoch(const TEpoch& epoch);

    //! Returns the invoker used for updating the state.
    /*!
     * \note Thread affinity: any
     */
    IInvokerPtr GetStateInvoker() const;

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
     *  \note Thread affinity: any
     */
    TMetaVersion GetReachableVersionAsync() const;

    //! Returns the version that is sent to followers via pings.
    /*!
     *  During recovery this is equal to the reachable version.
     *  After recovery this is equal to the version resulting from applying all
     *  mutations in the latest batch.
     *
     *  \note Thread affinity: ControlThread
     */
    TMetaVersion GetPingVersion() const;

    //! Updates the ping version.
    /*!
     *  \note Thread affinity: ControlThread
     */
    void SetPingVersion(const TMetaVersion& version);

    //! Returns the underlying state.
    /*!
     * \note Thread affinity: any
     */
    IMetaStatePtr GetState() const;

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

    //! Delegates the call to IMetaState::ApplyMutation and updates the version.
    /*!
     * \note Thread affinity: StateThread
     */
    void ApplyMutation(const TSharedRef& recordData);

    //! Executes a given action and updates the version.
    /*!
     * \note Thread affinity: StateThread
     */
    void ApplyMutation(
        const TSharedRef& recordData,
        const TClosure& mutationAction);

    //! Appends a new record into an appropriate changelog.
    /*!
     * \note Thread affinity: StateThread
     */
    TFuture<void> LogMutation(
        const TMetaVersion& version,
        const TSharedRef& recordData);

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

    //! Returns the current mutation context or NULL if no mutation is currently being applied.
    TMutationContext* GetMutationContext();

private:
    IMetaStatePtr State;
    IInvokerPtr StateInvoker;
    TSnapshotStorePtr SnapshotStore;
    TChangeLogCachePtr ChangeLogCache;
    TEpoch Epoch;
    bool Started;

    TCachedAsyncChangeLogPtr CurrentChangeLog;

    TSpinLock VersionSpinLock;
    TMetaVersion Version;
    TMetaVersion ReachableVersion;
    TMetaVersion PingVersion;

    TAutoPtr<TMutationContext> MutationContext;

    void IncrementRecordCount();
    void ComputeReachableVersion();
    void UpdateVersion(const TMetaVersion& newVersion);
    TCachedAsyncChangeLogPtr GetCurrentChangeLog();

    void EnterMutation(const TSharedRef& recordData);
    void LeaveMutation();

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
