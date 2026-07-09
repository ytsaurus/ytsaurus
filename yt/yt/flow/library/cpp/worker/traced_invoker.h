#pragma once

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/tracing/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

IInvokerPtr CreateTracedInvoker(
    IInvokerPtr underlyingInvoker,
    NTracing::TTraceContextPtr traceContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
