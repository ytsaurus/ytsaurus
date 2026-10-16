#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Process-wide facilities of a companion server, shared by every hosted job;
//! the companion analogue of the worker's #TJobTrackerContext.
struct TCompanionServerContext
    : public TRefCounted
{
    //! Fiber-capable invoker of the companion thread pool.
    IInvokerPtr Invoker;
    NConcurrency::IPollerPtr HttpPoller;
    NHttp::IClientPtr HttpClient;
    NHttp::IClientPtr HttpsClient;
};

DEFINE_REFCOUNTED_TYPE(TCompanionServerContext)

//! Builds the context from the companion config: an HTTP poller with the
//! configured thread count and the HTTP and HTTPS clients on it.
TCompanionServerContextPtr CreateCompanionServerContext(
    const NCompanion::TCompanionExecutionConfigPtr& config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
