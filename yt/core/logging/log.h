#pragma once

#include "common.h"

#include <core/misc/error.h>
#include <core/misc/format.h>

#include <core/concurrency/scheduler.h>

#include <core/tracing/trace_context.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TLogManager;

class TLogger
{
public:
    explicit TLogger(const Stroka& category = "");
    TLogger(const TLogger& other);

    const Stroka& GetCategory() const;
    bool IsEnabled(ELogLevel level) const;
    void Write(TLogEvent&& event) const;

    void AddRawTag(const Stroka& tag);

    template <class... TArgs>
    void AddTag(const char* format, const TArgs&... args)
    {
        AddRawTag(Format(format, args...));
    }

private:
    TLogManager* GetLogManager() const;
    void Update();
    static Stroka GetMessageWithContext(const Stroka& originalMessage, const Stroka& context);

    Stroka Category_;
    Stroka Context_;
    int Version_ = -1;
    mutable TLogManager* LogManager_ = nullptr;
    ELogLevel MinLevel_;

};

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_TRACE_LOGGING
#define LOG_TRACE(...)                      LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Trace, __VA_ARGS__)
#define LOG_TRACE_IF(condition, ...)        if (condition) LOG_TRACE(__VA_ARGS__)
#define LOG_TRACE_UNLESS(condition, ...)    if (!condition) LOG_TRACE(__VA_ARGS__)
#else
#define LOG_UNUSED(...)                     if (true) { } else { LOG_DEBUG(__VA_ARGS__); }
#define LOG_TRACE(...)                      LOG_UNUSED(__VA_ARGS__)
#define LOG_TRACE_IF(condition, ...)        LOG_UNUSED(__VA_ARGS__)
#define LOG_TRACE_UNLESS(condition, ...)    LOG_UNUSED(__VA_ARGS__)
#endif

#define LOG_DEBUG(...)                      LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Debug, __VA_ARGS__)
#define LOG_DEBUG_IF(condition, ...)        if (condition) LOG_DEBUG(__VA_ARGS__)
#define LOG_DEBUG_UNLESS(condition, ...)    if (!condition) LOG_DEBUG(__VA_ARGS__)

#define LOG_INFO(...)                       LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Info, __VA_ARGS__)
#define LOG_INFO_IF(condition, ...)         if (condition) LOG_INFO(__VA_ARGS__)
#define LOG_INFO_UNLESS(condition, ...)     if (!condition) LOG_INFO(__VA_ARGS__)

#define LOG_WARNING(...)                    LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Warning, __VA_ARGS__)
#define LOG_WARNING_IF(condition, ...)      if (condition) LOG_WARNING(__VA_ARGS__)
#define LOG_WARNING_UNLESS(condition, ...)  if (!condition) LOG_WARNING(__VA_ARGS__)

#define LOG_ERROR(...)                      LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Error, __VA_ARGS__)
#define LOG_ERROR_IF(condition, ...)        if (condition) LOG_ERROR(__VA_ARGS__)
#define LOG_ERROR_UNLESS(condition, ...)    if (!condition) LOG_ERROR(__VA_ARGS__)

#define LOG_FATAL(...)                      LOG_EVENT(Logger, ::NYT::NLog::ELogLevel::Fatal, __VA_ARGS__)
#define LOG_FATAL_IF(condition, ...)        if ( UNLIKELY(condition)) LOG_FATAL(__VA_ARGS__)
#define LOG_FATAL_UNLESS(condition, ...)    if ( ! LIKELY(condition) ) LOG_FATAL(__VA_ARGS__)

#define LOG_EVENT(logger, level, ...) \
    do { \
        if (logger.IsEnabled(level)) { \
            ::NYT::NLog::NDetail::LogEventImpl( \
                logger, \
                __FILE__, \
                __LINE__, \
                __FUNCTION__, \
                level, \
                ::NYT::NLog::NDetail::FormatLogMessage(__VA_ARGS__)); \
        } \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class... TArgs>
inline Stroka FormatLogMessage(const char* format, const TArgs&... args)
{
    return Format(format, args...);
}

template <class... TArgs>
inline Stroka FormatLogMessage(const TError& error, const char* format, const TArgs&... args)
{
    TStringBuilder builder;
    Format(&builder, format, args...);
    builder.AppendChar('\n');
    builder.AppendString(ToString(error));
    return builder.Flush();
}

template <class T>
inline Stroka FormatLogMessage(const T& obj)
{
    return ToString(obj);
}

template <class TLogger>
void LogEventImpl(
    TLogger& logger,
    const char* fileName,
    int line,
    const char* function,
    ELogLevel level,
    const Stroka& message)
{
    TLogEvent event;
    event.Category = logger.GetCategory();
    event.Level = level;
    event.Message = message;
    event.FileName = fileName;
    event.Line = line;
    event.ThreadId = NConcurrency::GetCurrentThreadId();
    event.FiberId = NConcurrency::GetCurrentFiberId();
    event.TraceId = NTracing::GetCurrentTraceContext().GetTraceId();
    event.Function = function;
    logger.Write(std::move(event));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
