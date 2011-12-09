#pragma once

#include "common.h"
#include "election_manager_rpc.h"

#include "../misc/config.h"
#include "../actions/future.h"
#include "../actions/parallel_awaiter.h"
#include "../rpc/client.h"
#include "../rpc/channel_cache.h"
#include "../misc/config.h"

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

//! Performs parallel and asynchronous leader lookups.
/*!
 * \note Thread affinity: any.
 */
class TLeaderLookup
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TLeaderLookup> TPtr;

    //! Lookup configuration.
    struct TConfig
        : TConfigBase
    {
        //! List of peer addresses.
        yvector<Stroka> Addresses;

        //! Timeout for RPC requests.
        TDuration RpcTimeout;

        TConfig()
        {
            Register("addresses", Addresses);
            Register("rpc_timeout", RpcTimeout).Default(TDuration::MilliSeconds(300));

            SetDefaults();
        }

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
         *  #InvalidPeerId value indicates that no leader is found.
         */
        TPeerId Id;

        //! Leader address.
        Stroka Address;

        //! Leader epoch.
        TGuid Epoch;
    };

    //! Initializes a new instance.
    TLeaderLookup(const TConfig& config);

    //! Performs an asynchronous lookup.
    TFuture<TResult>::TPtr GetLeader();

private:
    typedef TElectionManagerProxy TProxy;

    TConfig Config;
    static NRpc::TChannelCache ChannelCache;

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
        TFuture<TResult>::TPtr asyncResult,
        const Stroka& address);
    void OnComplete(TFuture<TResult>::TPtr asyncResult);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
