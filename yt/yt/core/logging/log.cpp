#include "log.h"
#include "log_manager.h"

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <util/system/thread.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

namespace {

thread_local TLoggingThreadName CachedLoggingThreadName;

void CacheThreadName()
{
    if (auto name = TThread::CurrentThreadName()) {
        auto length = std::min(TLoggingThreadName::BufferCapacity - 1, static_cast<int>(name.length()));
        CachedLoggingThreadName.Length = length;
        ::memcpy(CachedLoggingThreadName.Buffer.data(), name.data(), length);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLoggingContext GetLoggingContext()
{
    if (CachedLoggingThreadName.Length == 0) {
        CacheThreadName();
    }

    auto* traceContext = NTracing::GetCurrentTraceContext();

    return TLoggingContext{
        .Instant = GetCpuInstant(),
        .ThreadId = TThread::CurrentThreadId(),
        .ThreadName = CachedLoggingThreadName,
        .FiberId = NConcurrency::GetCurrentFiberId(),
        .TraceId = traceContext ? traceContext->GetTraceId() : TTraceId{},
        .RequestId = traceContext ? traceContext->GetRequestId() : NTracing::TRequestId(),
        .TraceLoggingTag = traceContext ? traceContext->GetLoggingTag() : TStringBuf(),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
