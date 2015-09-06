#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/format.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/thread.h>

#include <core/tracing/trace_context.h>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

struct TLogEvent
{
    static const int InvalidLine = -1;

    const char* Category = nullptr;
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

class TLogger
{
public:
    explicit TLogger(const char* category = nullptr);
    TLogger(const TLogger& other) = default;

    const char* GetCategory() const;
    bool IsEnabled(ELogLevel level) const;
    void Write(TLogEvent&& event) const;

    void AddRawTag(const Stroka& tag);
    template <class... TArgs>
    void AddTag(const char* format, const TArgs&... args);

private:
    const char* Category_;
    Stroka Context_;
    int Version_ = -1;
    mutable TLogManager* LogManager_ = nullptr;
    ELogLevel MinLevel_;

    TLogManager* GetLogManager() const;
    void Update();
    static Stroka GetMessageWithContext(const Stroka& originalMessage, const Stroka& context);

};

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_TRACE_LOGGING
#define LOG_TRACE(...)                      LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Trace, __VA_ARGS__)
#define LOG_TRACE_IF(condition, ...)        if (condition) LOG_TRACE(__VA_ARGS__)
#define LOG_TRACE_UNLESS(condition, ...)    if (!condition) LOG_TRACE(__VA_ARGS__)
#else
#define LOG_UNUSED(...)                     if (true) { } else { LOG_DEBUG(__VA_ARGS__); }
#define LOG_TRACE(...)                      LOG_UNUSED(__VA_ARGS__)
#define LOG_TRACE_IF(condition, ...)        LOG_UNUSED(__VA_ARGS__)
#define LOG_TRACE_UNLESS(condition, ...)    LOG_UNUSED(__VA_ARGS__)
#endif

#define LOG_DEBUG(...)                      LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Debug, __VA_ARGS__)
#define LOG_DEBUG_IF(condition, ...)        if (condition) LOG_DEBUG(__VA_ARGS__)
#define LOG_DEBUG_UNLESS(condition, ...)    if (!condition) LOG_DEBUG(__VA_ARGS__)

#define LOG_INFO(...)                       LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Info, __VA_ARGS__)
#define LOG_INFO_IF(condition, ...)         if (condition) LOG_INFO(__VA_ARGS__)
#define LOG_INFO_UNLESS(condition, ...)     if (!condition) LOG_INFO(__VA_ARGS__)

#define LOG_WARNING(...)                    LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Warning, __VA_ARGS__)
#define LOG_WARNING_IF(condition, ...)      if (condition) LOG_WARNING(__VA_ARGS__)
#define LOG_WARNING_UNLESS(condition, ...)  if (!condition) LOG_WARNING(__VA_ARGS__)

#define LOG_ERROR(...)                      LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Error, __VA_ARGS__)
#define LOG_ERROR_IF(condition, ...)        if (condition) LOG_ERROR(__VA_ARGS__)
#define LOG_ERROR_UNLESS(condition, ...)    if (!condition) LOG_ERROR(__VA_ARGS__)

#define LOG_FATAL(...)                      LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Fatal, __VA_ARGS__)
#define LOG_FATAL_IF(condition, ...)        if (UNLIKELY(condition)) LOG_FATAL(__VA_ARGS__)
#define LOG_FATAL_UNLESS(condition, ...)    if (!LIKELY(condition)) LOG_FATAL(__VA_ARGS__)

#define LOG_EVENT(logger, level, ...) \
    do { \
        if (logger.IsEnabled(level)) { \
            ::NYT::NLogging::NDetail::LogEventImpl( \
                logger, \
                __FILE__, \
                __LINE__, \
                __FUNCTION__, \
                level, \
                ::NYT::NLogging::NDetail::FormatLogMessage(__VA_ARGS__)); \
        } \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT

#define LOG_INL_H_
#include "log-inl.h"
#undef LOG_INL_H_

