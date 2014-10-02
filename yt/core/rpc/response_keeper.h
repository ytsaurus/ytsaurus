#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Provides a standard interface for remembering previously
//! served requests thus enabling client-side retries even for non-idempotent actions.
/*!
 *  Clients assign a unique (random) mutation id to every retry session.
 *  Servers ignore requests whose mutation id is already known.
 *
 *  After a sufficiently long period of time, a remembered response might
 *  get evicted.
 *
 *  \note
 *  Thread affinity: any
 */
struct IResponseKeeper
    : public virtual TRefCounted
{
    //! Called upon receiving a request with a given mutation #id.
    //! Either returns a valid future for the response (which can either be unset
    //! if the request is still being served or set if it is already completed) or
    //! a null future if #id is not known. In the latter case subsequent
    //! calls to #TryBeginRequest will be returning the same future over and
    //! over again.
    virtual TFuture<TSharedRefArray> TryBeginRequest(const TMutationId& id) = 0;

    //! Called when a request with a given mutation #id is finished and a #response is ready.
    //! The latter #response is pushed to every subscriber waiting for the future
    //! previously returned by #TryBeginRequest. Additionally, the #response
    //! may be remembered and returned by future calls to #TryBeginRequest.
    virtual void EndRequest(const TMutationId& id, TSharedRefArray response) = 0;
};

DEFINE_REFCOUNTED_TYPE(IResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
