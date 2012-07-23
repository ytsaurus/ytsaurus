#pragma once

#include "meta_state_manager_proxy.h"

#include <ytlib/actions/future.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Performs parallel and asynchronous leader lookups.
/*!
 * \note Thread affinity: any.
 */
class TMasterDiscovery
    : public TRefCounted
{
public:
    //! Describes a lookup result.
    struct TResult
    {
        //! Peer address.
        /*!
         *  Empty address represents a failed request.
         */
        TNullable<Stroka> Address;

        //! Quorum epoch.
        TGuid Epoch;
    };

    typedef TFuture<TResult> TAsyncResult;

    //! Initializes a new instance.
    TMasterDiscovery(TMasterDiscoveryConfigPtr config);

    //! Performs an asynchronous lookup of a master.
    /*!
     * The returned master is uniformly chosen among alive quorum participants.
     */
    TAsyncResult GetMaster();

    //! Performs an asynchronous lookup of a leader.
    TAsyncResult GetLeader();

    //! Performs an asynchronous lookup of a follower.
    /*!
     * The returned follower is uniformly chosen among alive followers.
     */
    TAsyncResult GetFollower();

private:
    typedef TMetaStateManagerProxy TProxy;

    TMasterDiscoveryConfigPtr Config;

    //! Protects from simultaneously reporting conflicting results.
    /*! 
     *  We shall reuse the same spinlock for all (possibly concurrent)
     *  #GetLeader requests. This should not harm since the protected region
     *  is quite tiny.
     */
    TSpinLock SpinLock;
    
    TFuture<TProxy::TRspGetQuorumPtr> GetQuorum();

    void OnResponse(
        TParallelAwaiterPtr awaiter,
        TPromise<TProxy::TRspGetQuorumPtr> promise,
        const Stroka& address,
        TProxy::TRspGetQuorumPtr response);
    void OnComplete(TPromise<TProxy::TRspGetQuorumPtr> promise);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
