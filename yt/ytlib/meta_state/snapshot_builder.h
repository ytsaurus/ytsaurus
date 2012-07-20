#pragma once

#include "public.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/misc/checksum.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/election/election_manager.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public TExtrinsicRefCounted
{
public:
    DECLARE_ENUM(EResultCode,
        (OK)
        (InvalidVersion)
        (AlreadyInProgress)
        (ForkError)
        (TimeoutExceeded)
    );

    // TODO(babenko): consider replacing with TValueOrError
    struct TLocalResult
    {
        EResultCode ResultCode;
        TChecksum Checksum;

        explicit TLocalResult(
            EResultCode resultCode = EResultCode::OK,
            TChecksum checksum = 0)
            : ResultCode(resultCode)
            , Checksum(checksum)
        { }
    };

    typedef TFuture<TLocalResult> TAsyncLocalResult;
    typedef TPromise<TLocalResult> TAsyncLocalPromise;

    TSnapshotBuilder(
        TSnapshotBuilderConfig* config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr decoratedState,
        TSnapshotStorePtr snapshotStore,
        const TEpoch& epoch,
        IInvokerPtr epochControlInvoker,
        IInvokerPtr epochStateInvoker);

    /*!
     *  \returns OK if distributed session is started,
     *  AlreadyInProgress if the previous session is not completed yet.
     *
     *  \note Thread affinity: StateThread
     */
    void CreateDistributedSnapshot();

    /*!
     *  \note Thread affinity: StateThread
     */
    void RotateChangeLog();

    /*!
     *  \note Thread affinity: StateThread
     */
    TAsyncLocalResult CreateLocalSnapshot(const TMetaVersion& version);

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

    TChecksum DoCreateLocalSnapshot(TMetaVersion version);
    void OnLocalSnapshotCreated(i32 snapshotId, const TChecksum& checksum);

    TSnapshotBuilderConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    TDecoratedMetaStatePtr DecoratedState;
    TSnapshotStorePtr SnapshotStore;
    TEpoch Epoch;
    IInvokerPtr EpochControlInvoker;
    IInvokerPtr EpochStateInvoker;

    TAsyncLocalPromise LocalPromise;

#if defined(_unix_)
    static void WatchdogFork(
        TWeakPtr<TSnapshotBuilder> weakSnapshotBuilder,
        i32 segmentId, 
        pid_t childPid);
    TActionQueuePtr WatchdogQueue;
#endif
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
