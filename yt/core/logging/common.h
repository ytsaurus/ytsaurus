#pragma once

#include "public.h"

#include <core/concurrency/thread.h>

#include <core/concurrency/public.h>

#include <core/tracing/public.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

// Any changes to this enum must be also propagated to FormatLevel.
DEFINE_ENUM(ELogLevel,
    (Minimum)
    (Trace)
    (Debug)
    (Info)
    (Warning)
    (Error)
    (Fatal)
    (Maximum)
);

struct TLogEvent
{
    static const int InvalidLine = -1;

    Stroka Category;
    ELogLevel Level;
    Stroka Message;
    TInstant DateTime;
    const char* FileName = nullptr;
    int Line = InvalidLine;
    NConcurrency::TThreadId ThreadId = NConcurrency::InvalidThreadId;
    NConcurrency::TFiberId FiberId = NConcurrency::InvalidFiberId;
    NTracing::TTraceId TraceId = NTracing::InvalidTraceId;
    const char* Function = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
