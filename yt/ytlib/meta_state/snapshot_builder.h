#pragma once

#include "public.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/misc/checksum.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/election/election_manager.h>
#include <ytlib/rpc/client.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public TExtrinsicRefCounted
{
public:
    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TDuration RemoteTimeout;
        TDuration LocalTimeout;

        TConfig()
        {
            Register("remote_timeout", RemoteTimeout)
                .GreaterThan(TDuration())
                .Default(TDuration::Minutes(1));
            Register("local_timeout", LocalTimeout)
                .GreaterThan(TDuration())
                .Default(TDuration::Minutes(1));
        }
    };

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

    TSnapshotBuilder(
        TConfig* config,
        TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr decoratedState,
        TChangeLogCachePtr changeLogCache,
        TSnapshotStorePtr snapshotStore,
        TEpoch epoch,
        IInvoker::TPtr epochControlInvoker,
        IInvoker::TPtr epochStateInvoker);

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
    TAsyncLocalResult::TPtr CreateLocalSnapshot(const TMetaVersion& version);

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

    TConfig::TPtr Config;
    TCellManagerPtr CellManager;
    TDecoratedMetaStatePtr DecoratedState;
    TSnapshotStorePtr SnapshotStore;
    TChangeLogCachePtr ChangeLogCache;
    TEpoch Epoch;
    IInvoker::TPtr EpochControlInvoker;
    IInvoker::TPtr EpochStateInvoker;

    TAsyncLocalResult::TPtr LocalResult;

#if defined(_unix_)
    static void WatchdogFork(
        TWeakPtr<TSnapshotBuilder> weakSnapshotBuilder,
        i32 segmentId, 
        pid_t childPid);
    TActionQueue::TPtr WatchdogQueue;
#endif
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
