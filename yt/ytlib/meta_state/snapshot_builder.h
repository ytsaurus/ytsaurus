#pragma once

#include "public.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/misc/checksum.h>
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
        TSnapshotBuilderConfig* config,
        TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr metaState,
        TChangeLogCachePtr changeLogCache,
        TSnapshotStorePtr snapshotStore,
        TEpoch epoch,
        IInvoker::TPtr serviceInvoker);

    /*!
     * \returns OK if distributed session is started,
     *          AlreadyInProgress if the previous session is not completed yet.
     *
     * \note Thread affinity: StateThread
     */
    EResultCode CreateDistributed();

    /*!
     * \note Thread affinity: StateThread
     */
    TAsyncLocalResult::TPtr CreateLocal(TMetaVersion version);

    /*!
     * \note Thread affinity: StateThread
     */
    TAsyncLocalResult::TPtr GetLocalResult() const
    {
         VERIFY_THREAD_AFFINITY(StateThread);
        
         return LocalResult;
    }

    /*!
     * \note Thread affinity: StateThread
     */
    bool IsInProgress() const
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    
        TLocalResult fake;
        return !LocalResult->TryGet(&fake);
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    class TSession;
    typedef TIntrusivePtr<TSession> TSessionPtr;

    typedef TMetaStateManagerProxy TProxy;

    TChecksum DoCreateLocal(TMetaVersion version);
    void OnLocalCreated(i32 segmentId, const TChecksum& checksum);

    TSnapshotBuilderConfigPtr Config;
    TCellManagerPtr CellManager;
    TDecoratedMetaStatePtr MetaState;
    TSnapshotStorePtr SnapshotStore;
    TChangeLogCachePtr ChangeLogCache;
    TEpoch Epoch;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr StateInvoker;

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
