#pragma once

#include "common.h"
#include "election_manager_rpc.h"

#include "../master/cell_manager.h"
#include "../actions/async_result.h"
#include "../actions/parallel_awaiter.h"
#include "../rpc/client.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TLeaderLookup
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TLeaderLookup> TPtr;

    struct TConfig
    {
        yvector<Stroka> MasterAddresses;
        TDuration Timeout;

        TConfig()
            : Timeout(TDuration::MilliSeconds(300))
        { }

        void Read(TJsonObject* json)
        {
            // TODO: read timeout
            NYT::TryRead(json, L"MasterAddresses", &MasterAddresses);
        }
    };

    struct TResult
    {
        Stroka Address;
        TMasterId Id;
        TGuid Epoch;
    };

    typedef TAsyncResult<TResult> TLookupResult;

    TLeaderLookup(const TConfig& config);

    TLookupResult::TPtr GetLeader();

private:
    typedef TElectionManagerProxy TProxy;

    TConfig Config;
    NRpc::TChannelCache ChannelCache;
    
    static void OnResponse(
        TProxy::TRspGetStatus::TPtr response,
        TParallelAwaiter::TPtr awaiter,
        TLookupResult::TPtr asyncResult,
        Stroka address);
    static void OnComplete(TLookupResult::TPtr asyncResult);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
