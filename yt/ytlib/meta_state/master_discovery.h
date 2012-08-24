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
        TGuid EpochId;
    };

    typedef TFuture<TResult> TAsyncResult;

    //! Initializes a new instance.
    explicit TMasterDiscovery(TMasterDiscoveryConfigPtr config);

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
    class TQuorumRequester;

    TMasterDiscoveryConfigPtr Config;
  
    TFuture<TProxy::TRspGetQuorumPtr> GetQuorum();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
