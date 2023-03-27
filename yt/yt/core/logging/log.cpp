#include "log.h"
#include "log_manager.h"

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <util/system/thread.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

TLoggingContext GetLoggingContext()
{
    auto* traceContext = NTracing::TryGetCurrentTraceContext();

    return TLoggingContext{
        .Instant = GetCpuInstant(),
        .ThreadId = TThread::CurrentThreadId(),
        .ThreadName = GetLoggingThreadName(),
        .FiberId = NConcurrency::GetCurrentFiberId(),
        .TraceId = traceContext ? traceContext->GetTraceId() : TTraceId{},
        .RequestId = traceContext ? traceContext->GetRequestId() : NTracing::TRequestId(),
        .TraceLoggingTag = traceContext ? traceContext->GetLoggingTag() : TStringBuf(),
    };
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
