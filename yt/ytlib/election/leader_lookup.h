#pragma once

#include "common.h"
#include "election_manager_rpc.h"

#include "../actions/async_result.h"
#include "../actions/parallel_awaiter.h"
#include "../rpc/client.h"
#include "../misc/config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Performs parallel and asynchronous leader lookups.
class TLeaderLookup
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TLeaderLookup> TPtr;

    //! Describes a configuration.
    struct TConfig
    {
        //! List of peer addresses.
        yvector<Stroka> Addresses;

        //! Timeout for RPC requests.
        TDuration RpcTimeout;

        TConfig()
            : RpcTimeout(TDuration::MilliSeconds(300))
        { }

        void Read(TJsonObject* json)
        {
            // TODO: read timeout
            NYT::TryRead(json, L"Addresses", &Addresses);
        }
    };

    //! Describes a lookup result.
    struct TResult
    {
        //! Leader id.
        /*!
         *  InvalidPeerId value indicates that no leader is found.
         */
        TPeerId Id;

        //! Leader address.
        Stroka Address;

        //! Leader epoch.
        TGuid Epoch;
    };

    typedef TAsyncResult<TResult> TAsyncResult;

    //! Initializes a new instance.
    TLeaderLookup(const TConfig& config);

    //! Performs an asynchronous lookup.
    /*!
     * \note Thread affinity: any
     */
    TAsyncResult::TPtr GetLeader();

private:
    typedef TElectionManagerProxy TProxy;

    TConfig Config;
    NRpc::TChannelCache ChannelCache;

    //! Protects from simultaneously reporting conflicting results.
    /*! 
     *  We shall reuse the same spinlock for all (possibly concurrent)
     *  #GetLeader requests. This should not harm since the protected region
     *  is quite tiny.
     */
    TSpinLock SpinLock;
    
    void OnResponse(
        TProxy::TRspGetStatus::TPtr response,
        TParallelAwaiter::TPtr awaiter,
        TAsyncResult::TPtr asyncResult,
        Stroka address);
    void OnComplete(TAsyncResult::TPtr asyncResult);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
