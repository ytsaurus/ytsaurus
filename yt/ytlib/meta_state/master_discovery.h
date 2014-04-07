#pragma once

#include "meta_state_manager_proxy.h"

#include <core/actions/future.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/ytree/yson_serializable.h>

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

    //! Performs an asynchronous lookup of a leader.
    TAsyncResult GetLeader();

private:
    typedef TMetaStateManagerProxy TProxy;
    class TQuorumRequester;

    TMasterDiscoveryConfigPtr Config;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
