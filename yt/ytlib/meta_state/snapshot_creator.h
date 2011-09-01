#pragma once

#include "common.h"
#include "meta_state_manager_rpc.h"
#include "snapshot_store.h"
#include "meta_state.h"
#include "decorated_meta_state.h"
#include "cell_manager.h"
#include "change_log_cache.h"

#include "../election/election_manager.h"
#include "../rpc/client.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotCreator
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSnapshotCreator> TPtr;

    struct TConfig
    {
        TDuration Timeout;

        TConfig()
            : Timeout(TDuration::Minutes(1))
        { }
    };

    DECLARE_ENUM(EResultCode,
        (OK)
        (InvalidVersion)
    );

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

    typedef TAsyncResult<TLocalResult> TAsyncLocalResult;

    TSnapshotCreator(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr metaState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        IInvoker::TPtr serviceInvoker);

    void CreateDistributed(TMetaVersion version);
    TAsyncLocalResult::TPtr CreateLocal(TMetaVersion version);

private:
    class TSession;

    typedef TMetaStateManagerProxy TProxy;

    TConfig Config;
    TCellManager::TPtr CellManager;
    TDecoratedMetaState::TPtr MetaState;
    TSnapshotStore::TPtr SnapshotStore;
    TChangeLogCache::TPtr ChangeLogCache;
    TEpoch Epoch;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr StateInvoker;

    TAsyncLocalResult::TPtr DoCreateLocal(TMetaVersion version);

    static TLocalResult OnSave(
        TVoid /* fake */,
        i32 segmentId,
        TSnapshotWriter::TPtr writer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
