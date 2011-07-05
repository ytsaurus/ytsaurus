#pragma once

#include "common.h"
#include "election_manager_rpc.h"

#include "../master/cell_manager.h"
#include "../actions/async_result.h"
#include "../rpc/client.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TLeaderLookup
    : private TNonCopyable
{
public:
    struct TConfig
    {
        TDuration Timeout;

        TConfig()
            : Timeout(TDuration::MilliSeconds(300))
        {}
    };

    struct TResult
    {
        TMasterId LeaderId;
        TGUID Epoch;
    };

    typedef TAsyncResult<TResult> TLookupResult;

    TLeaderLookup(
        const TConfig& config,
        TCellManager::TPtr cellManager);

    TLookupResult::TPtr GetLeader();

private:
    typedef TElectionManagerProxy TProxy;

    TConfig Config;
    TCellManager::TPtr CellManager;
    
    static void OnResponse(
        TProxy::TRspGetStatus::TPtr response,
        TParallelAwaiter::TPtr awaiter,
        TLookupResult::TPtr asyncResult,
        TMasterId masterId);
    static void OnComplete(TLookupResult::TPtr asyncResult);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
