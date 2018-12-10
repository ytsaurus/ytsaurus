#pragma once
#ifndef LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include log.h"
// For the sake of sane code completion.
#include "log.h"
#endif
#undef LOG_INL_H_

#include <yt/core/profiling/timing.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
TLogger& TLogger::AddTag(const char* format, const TArgs&... args)
{
    return AddRawTag(Format(format, args...));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class... TArgs>
TString FormatLogMessage(const char* format, const TArgs&... args)
{
    return Format(format, args...);
}

template <class... TArgs>
TString FormatLogMessage(const TError& error, const char* format, const TArgs&... args)
{
    TStringBuilder builder;
    Format(&builder, format, args...);
    builder.AppendChar('\n');
    builder.AppendString(ToString(error));
    return builder.Flush();
}

template <class T>
TString FormatLogMessage(const T& obj)
{
    return ToString(obj);
}

inline TString FormatLogMessage(TString&& message)
{
    return std::move(message);
}

inline TLogEvent CreateLogEvent(const TLogger& logger, ELogLevel level)
{
    TLogEvent event;
    event.Instant = NProfiling::GetCpuInstant();
    event.Category = logger.GetCategory();
    event.Level = level;
    event.ThreadId = TThread::CurrentThreadId();
    event.FiberId = NConcurrency::GetCurrentFiberId();
    event.TraceId = NTracing::GetCurrentTraceContext().GetTraceId();
    return event;
}

inline void LogEventImpl(
    const TLogger& logger,
    ELogLevel level,
    TString message)
{
    TLogEvent event = CreateLogEvent(logger, level);
    event.Message = std::move(message);
    event.MessageFormat = ELogMessageFormat::PlainText;
    logger.Write(std::move(event));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

inline void LogStructuredEvent(
    const TLogger& logger,
    NYson::TYsonString message,
    ELogLevel level)
{
    YCHECK(message.GetType() == NYson::EYsonType::MapFragment);
    TLogEvent event = NDetail::CreateLogEvent(logger, level);
    event.StructuredMessage = std::move(message);
    event.MessageFormat = ELogMessageFormat::Structured;
    logger.Write(std::move(event));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
