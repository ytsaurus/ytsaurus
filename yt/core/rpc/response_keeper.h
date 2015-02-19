#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/actions/future.h>

#include <core/logging/public.h>

#include <core/profiling/public.h>

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
class TResponseKeeper
    : public TRefCounted
{
public:
    TResponseKeeper(
        TResponseKeeperConfigPtr config,
        const NLog::TLogger& logger,
        const NProfiling::TProfiler& profiler);
    ~TResponseKeeper();

    void Start();
    void Stop();

    //! Called upon receiving a request with a given mutation #id.
    //! Either returns a valid future for the response (which can either be unset
    //! if the request is still being served or set if it is already completed) or
    //! a null future if #id is not known. In the latter case subsequent
    //! calls to #TryBeginRequest will be returning the same future over and
    //! over again.
    TFuture<TSharedRefArray> TryBeginRequest(const TMutationId& id, bool isRetry);

    //! Called when a request with a given mutation #id is finished and a #response is ready.
    //! The latter #response is pushed to every subscriber waiting for the future
    //! previously returned by #TryBeginRequest. Additionally, the #response
    //! may be remembered and returned by future calls to #TryBeginRequest.
    void EndRequest(const TMutationId& id, TSharedRefArray response);

    void CancelRequest(const TMutationId& id);

    bool TryReplyFrom(IServiceContextPtr context);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
