#pragma once

#include "private.h"
#include "meta_state_manager_proxy.h"

#include <core/misc/checksum.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/action_queue.h>

#include <ytlib/election/election_manager.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public TExtrinsicRefCounted
{
public:
    struct TResult
    {
        int SnapshotId;
        TChecksum Checksum;
    };

    typedef TErrorOr<TResult> TResultOrError;

    TSnapshotBuilder(
        TSnapshotBuilderConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr decoratedState,
        TSnapshotStorePtr snapshotStore,
        const TEpochId& epochId,
        IInvokerPtr controlInvoker,
        IInvokerPtr stateInvoker);

    /*!
     *  \returns OK if distributed session is started,
     *  AlreadyInProgress if the previous session is not completed yet.
     *
     *  \note Thread affinity: StateThread
     */
    TFuture<TResultOrError> BuildSnapshotDistributed();

    /*!
     *  \note Thread affinity: StateThread
     */
    void RotateChangeLog();

    /*!
     *  \note Thread affinity: StateThread
     */
    TFuture<TResultOrError> BuildSnapshotLocal(const TMetaVersion& version);

    /*!
     *  \note Thread affinity: StateThread
     */
    void WaitUntilFinished();

    /*!
     *  \note Thread affinity: StateThread
     */
    bool IsInProgress() const;

private:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    class TSession;
    typedef TIntrusivePtr<TSession> TSessionPtr;

    typedef TMetaStateManagerProxy TProxy;

    TResult DoCreateLocalSnapshot(const TMetaVersion& version);
    void OnLocalSnapshotCreated(i32 snapshotId, const TResult& result);

    TSnapshotBuilderConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    TDecoratedMetaStatePtr DecoratedState;
    TSnapshotStorePtr SnapshotStore;
    TEpochId EpochId;
    IInvokerPtr ControlInvoker;
    IInvokerPtr StateInvoker;

    TPromise<TResultOrError> LocalPromise;
    bool Canceled;

#if defined(_unix_)
    static void WatchdogFork(
        TWeakPtr<TSnapshotBuilder> weakSnapshotBuilder,
        i32 segmentId,
        pid_t childPid);
    NConcurrency::TActionQueuePtr WatchdogQueue;
#endif
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
