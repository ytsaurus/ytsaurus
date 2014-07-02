#pragma once

#include "public.h"

#include <core/concurrency/thread.h>

#include <core/concurrency/public.h>

#include <core/tracing/public.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

// Any changes to this enum must be also propagated to FormatLevel.
DECLARE_ENUM(ELogLevel,
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

    TLogEvent()
        : DateTime(TInstant::Now())
        , FileName(nullptr)
        , Line(InvalidLine)
        , ThreadId(NConcurrency::InvalidThreadId)
        , FiberId(NConcurrency::InvalidFiberId)
        , TraceId(NTracing::InvalidTraceId)
        , Function(nullptr)
    { }

    TLogEvent(const Stroka& category, ELogLevel level, const Stroka& message)
        : Category(category)
        , Level(level)
        , Message(message)
        , DateTime(TInstant::Now())
        , FileName(nullptr)
        , Line(InvalidLine)
        , ThreadId(NConcurrency::InvalidThreadId)
        , FiberId(NConcurrency::InvalidFiberId)
        , TraceId(NTracing::InvalidTraceId)
        , Function(nullptr)
    { }

    Stroka Category;
    ELogLevel Level;
    Stroka Message;
    TInstant DateTime;
    const char* FileName;
    i32 Line;
    NConcurrency::TThreadId ThreadId;
    NConcurrency::TFiberId FiberId;
    NTracing::TTraceId TraceId;
    const char* Function;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
