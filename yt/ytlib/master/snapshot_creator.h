#pragma once

#include "common.h"
#include "master_state_manager_rpc.h"
#include "snapshot_store.h"
#include "master_state.h"
#include "decorated_master_state.h"
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
        (InvalidStateId)
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
        TDecoratedMasterState::TPtr masterState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
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
    TSnapshotStore::TPtr SnapshotStore;
    TChangeLogCache::TPtr ChangeLogCache;
    TMasterEpoch Epoch;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr WorkInvoker;

    TAsyncLocalResult::TPtr DoCreateLocal(TMasterStateId stateId);

    static TLocalResult OnSave(
        TVoid /* fake */,
        i32 segmentId,
        TSnapshotWriter::TPtr writer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
