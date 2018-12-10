#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/logging/public.h>

#include <yt/core/misc/ref.h>

#include <yt/core/profiling/public.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Helps to remeber previously served requests thus enabling client-side retries
//! even for non-idempotent actions.
/*!
 *  Clients assign a unique (random) mutation id to every retry session.
 *  Servers ignore requests whose mutation ids are already known.
 *
 *  After a sufficiently long period of time, a remembered response gets evicted.
 *
 *  The keeper is initially inactive.
 *
 *  \note
 *  Thread affinity: single-threaded
 */
class TResponseKeeper
    : public TRefCounted
{
public:
    TResponseKeeper(
        TResponseKeeperConfigPtr config,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler);
    ~TResponseKeeper();

    //! Activates the keeper.
    /*!
     *  If the keeper is already started, the call does nothing.
     *  The keeper will start remembering responses.
     *  During the warm-up period, however, it cannot detect duplicate requests
     *  reliably and thus #TryBeginRequest may throw.
     */
    void Start();

    //! Deactivates the keeper.
    /*!
     *  If the keeper is already stopped, the call does nothing.
     *  Calling #TryBeginRequest for an inactive keeper will lead to an exception.
     */
    void Stop();

    //! Called upon receiving a request with a given mutation #id.
    /*!
     *  Either returns a valid future for the response (which can either be unset
     *  if the request is still being served or set if it is already completed) or
     *  a null future if #id is not known. In the latter case subsequent
     *  calls to #TryBeginRequest will be returning the same future over and
     *  over again.
     *
     *  The call throws if the keeper is not active or if #isRetry is |true| and
     *  the keeper is warming up.
     */
    TFuture<TSharedRefArray> TryBeginRequest(const TMutationId& id, bool isRetry);

    //! Called when a request with a given mutation #id is finished and a #response is ready.
    /*
     *  The latter #response is pushed to every subscriber waiting for the future
     *  previously returned by #TryBeginRequest. Additionally, the #response
     *  is remembered and returned by future calls to #TryBeginRequest.
     */
    void EndRequest(const TMutationId& id, TSharedRefArray response);

    //! Forgets the request, which was previously registered via #TryBeginRequest.
    void CancelRequest(const TMutationId& id, const TError& error);

    //! Combines #TryBeginRequest and #EndBeginRequest.
    /*!
     *  If |true| is returned then the request (given by #context) has mutation id assigned and
     *  a previously-remembered response is known. In this case #TryReplyFrom replies #context;
     *  no further server-side processing is needed.
     *
     *  If |false| is returned then either the request has no mutation id assigned or
     *  this id hasn't been seen before. In both cases the server must proceed with serving the request.
     *  Also, if the request has mutation id assigned the response will be automatically remembered
     *  when #context is replied.
     */
    bool TryReplyFrom(IServiceContextPtr context);

    //! Returns |true| if the keeper is still warming up.
    bool IsWarmingUp() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
