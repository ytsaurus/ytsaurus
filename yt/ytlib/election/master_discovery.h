#pragma once

#include "common.h"
#include "election_manager_proxy.h"

#include <ytlib/actions/future.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

//! Performs parallel and asynchronous leader lookups.
/*!
 * \note Thread affinity: any.
 */
class TMasterDiscovery
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TMasterDiscovery> TPtr;

    //! Lookup configuration.
    struct TConfig
        : public TYsonSerializable
    {
        //! List of peer addresses.
        std::vector<Stroka> Addresses;

        //! Timeout for RPC requests to masters.
        TDuration RpcTimeout;

        //! Master connection priority. 
        int ConnectionPriority;

        TConfig()
        {
            Register("addresses", Addresses)
                .NonEmpty();
            Register("rpc_timeout", RpcTimeout)
                .Default(TDuration::Seconds(5));
            Register("connection_priority", ConnectionPriority)
                .InRange(0, 6)
                .Default(6);
        }

    };

    typedef TIntrusivePtr<TConfig> TConfigPtr;

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

    typedef TFuture<TResult> TAsyncResult;

    //! Initializes a new instance.
    TMasterDiscovery(TConfigPtr config);

    //! Performs an asynchronous lookup.
    TAsyncResult GetLeader();

private:
    typedef TElectionManagerProxy TProxy;

    TConfigPtr Config;

    //! Protects from simultaneously reporting conflicting results.
    /*! 
     *  We shall reuse the same spinlock for all (possibly concurrent)
     *  #GetLeader requests. This should not harm since the protected region
     *  is quite tiny.
     */
    TSpinLock SpinLock;
    
    void OnResponse(
        TParallelAwaiterPtr awaiter,
        TPromise<TResult> promise,
        const Stroka& address,
        TProxy::TRspGetStatusPtr response);
    void OnComplete(TPromise<TResult> promise);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
