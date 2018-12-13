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
TLogger& TLogger::AddTag(const char* format, TArgs&&... args)
{
    return AddRawTag(Format(format, std::forward<TArgs>(args)...));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class... TArgs>
void AppendLogMessage(TStringBuilder* builder, TStringBuf context, TStringBuf message)
{
    if (context) {
        if (message.length() >= 1 && message[message.length() - 1] == ')') {
            builder->AppendString(TStringBuf(message.data(), message.length() - 1));
            builder->AppendString(AsStringBuf(", "));
            builder->AppendString(context);
            builder->AppendChar(')');
        } else {
            builder->AppendString(message);
            builder->AppendString(AsStringBuf(" ("));
            builder->AppendString(context);
            builder->AppendChar(')');
        }
    } else {
        builder->AppendString(message);
    }
}

template <class... TArgs, size_t FormatLength>
void AppendLogMessageWithFormat(TStringBuilder* builder, TStringBuf context, const char (&format)[FormatLength], TArgs&&... args)
{
    if (context) {
        if (FormatLength >= 2 && format[FormatLength - 2] == ')') {
            builder->AppendFormat(TStringBuf(format, FormatLength - 2), std::forward<TArgs>(args)...);
            builder->AppendString(AsStringBuf(", "));
            builder->AppendString(context);
            builder->AppendChar(')');
        } else {
            builder->AppendFormat(format, std::forward<TArgs>(args)...);
            builder->AppendString(AsStringBuf(" ("));
            builder->AppendString(context);
            builder->AppendChar(')');
        }
    } else {
        builder->AppendFormat(format, std::forward<TArgs>(args)...);
    }
}

template <class... TArgs, size_t FormatLength>
TString BuildLogMessage(TStringBuf context, const char (&format)[FormatLength], TArgs&&... args)
{
    TStringBuilder builder;
    AppendLogMessageWithFormat(&builder, context, format, std::forward<TArgs>(args)...);
    return builder.Flush();
}

template <class... TArgs, size_t FormatLength>
TString BuildLogMessage(TStringBuf context, const TError& error, const char (&format)[FormatLength], TArgs&&... args)
{
    TStringBuilder builder;
    AppendLogMessageWithFormat(&builder, context, format, std::forward<TArgs>(args)...);
    builder.AppendChar('\n');
    FormatValue(&builder, error, TStringBuf());
    return builder.Flush();
}

template <class T>
TString BuildLogMessage(TStringBuf context, const T& obj)
{
    TStringBuilder builder;
    FormatValue(&builder, obj, TStringBuf());
    if (context) {
        builder.AppendString(AsStringBuf(" ("));
        builder.AppendString(context);
        builder.AppendChar(')');
    }
    return builder.Flush();
}

inline TString BuildLogMessage(TStringBuf context, TString&& message)
{
    if (context) {
        TStringBuilder builder;
        AppendLogMessage(&builder, context, message);
        return builder.Flush();
    } else {
        return std::move(message);
    }
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
