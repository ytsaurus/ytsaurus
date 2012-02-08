#pragma once

#include "common.h"
#include "meta_state_manager_proxy.h"
#include "snapshot_store.h"
#include "meta_state.h"
#include "decorated_meta_state.h"
#include "cell_manager.h"
#include "change_log_cache.h"

#include <ytlib/election/election_manager.h>
#include <ytlib/rpc/client.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TSnapshotBuilder> TPtr;

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
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr metaState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
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

    typedef TMetaStateManagerProxy TProxy;

    TChecksum DoCreateLocal(TMetaVersion version);
    void OnLocalCreated(const TChecksum& checksum);

#if defined(_unix_)
    static void* WatchdogThreadFunc(void* param);
    THolder<TThread> WatchdogThread;
    pid_t ChildPid;
#endif

    TConfig::TPtr Config;
    TCellManager::TPtr CellManager;
    TDecoratedMetaState::TPtr MetaState;
    TSnapshotStore::TPtr SnapshotStore;
    TChangeLogCache::TPtr ChangeLogCache;
    TEpoch Epoch;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr StateInvoker;

    TAsyncLocalResult::TPtr LocalResult;
    i32 CurrentSnapshotId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
