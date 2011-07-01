#pragma once

#include "common.h"
#include "master_state_manager_rpc.h"
#include "election_manager.h"
#include "snapshot_store.h"
#include "master_state.h"
#include "decorated_master_state.h"
#include "cell_manager.h"
#include "change_log_cache.h"

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
        {}
    };

    enum EResult
    {
        OK,
        InvalidStateId
    };

    struct TLocalResult
    {
        EResult ResultCode;
        TChecksum Checksum;

        explicit TLocalResult(EResult resultCode = OK, TChecksum checksum = 0)
            : ResultCode(resultCode)
            , Checksum(checksum)
        { }
    };

    typedef TAsyncResult<TLocalResult> TAsyncLocalResult;

    TSnapshotCreator(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMasterState::TPtr masterState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore* snapshotStore,
        TMasterEpoch epoch,
        IInvoker::TPtr serviceInvoker,
        IInvoker::TPtr workInvoker);

    void CreateDistributed(TMasterStateId stateId);
    TAsyncLocalResult::TPtr CreateLocal(TMasterStateId stateId);

private:
    class TSession;

    typedef TMasterStateManagerProxy TProxy;

    TConfig Config;
    TCellManager::TPtr CellManager;
    TDecoratedMasterState::TPtr MasterState;
    TSnapshotStore* SnapshotStore;
    TChangeLogCache::TPtr ChangeLogCache;
    TMasterEpoch Epoch;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr WorkInvoker;

    TAsyncLocalResult::TPtr DoCreateLocal(TMasterStateId stateId);

    static TLocalResult OnSave(
        TVoid /* fake */,
        i32 segmentId,
        TAutoPtr<TSnapshotWriter> writer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
