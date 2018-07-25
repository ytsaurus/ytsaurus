#pragma once

#include "public.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/format.h>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/profiling/public.h>

#include <util/system/thread.h>

#include <atomic>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

struct TLoggingCategory
{
    const char* Name;
    ELogLevel MinLevel;
    int CurrentVersion;
    std::atomic<int>* ActualVersion;
};

////////////////////////////////////////////////////////////////////////////////

struct TLoggingPosition
{
    bool Enabled;
    int CurrentVersion;
};

////////////////////////////////////////////////////////////////////////////////

struct TLogEvent
{
    const TLoggingCategory* Category = nullptr;
    ELogLevel Level = ELogLevel::Minimum;
    ELogEventFormat Format = ELogEventFormat::PlainText;
    TString Message;
    NYson::TYsonString StructuredMessage;
    NProfiling::TCpuInstant Instant = 0;
    NConcurrency::TThreadId ThreadId = NConcurrency::InvalidThreadId;
    NConcurrency::TFiberId FiberId = NConcurrency::InvalidFiberId;
    NTracing::TTraceId TraceId = NTracing::InvalidTraceId;
};

////////////////////////////////////////////////////////////////////////////////

class TLogger
{
public:
    TLogger();
    explicit TLogger(const char* categoryName);
    TLogger(const TLogger& other) = default;

    operator bool() const;

    const TLoggingCategory* GetCategory() const;
    bool IsLevelEnabled(ELogLevel level) const;

    bool IsPositionUpToDate(const TLoggingPosition& position) const;
    void UpdatePosition(TLoggingPosition* position, const TString& message) const;

    void Write(TLogEvent&& event) const;

    TLogger& AddRawTag(const TString& tag);
    template <class... TArgs>
    TLogger& AddTag(const char* format, const TArgs&... args);
    const TString& GetContext() const;

private:
    TLogManager* LogManager_;
    const TLoggingCategory* Category_;

    TString Context_;

    static TString GetMessageWithContext(const TString& originalMessage, const TString& context);
};

////////////////////////////////////////////////////////////////////////////////

//! Typically serves as a virtual base for classes that need a member logger.
class TLoggerOwner
{
protected:
    TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_TRACE_LOGGING
#define LOG_TRACE(...)                      LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Trace, __VA_ARGS__)
#define LOG_TRACE_IF(condition, ...)        if (condition)    LOG_TRACE(__VA_ARGS__)
#define LOG_TRACE_UNLESS(condition, ...)    if (!(condition)) LOG_TRACE(__VA_ARGS__)
#else
#define LOG_UNUSED(...)                     if (true) { } else { LOG_DEBUG(__VA_ARGS__); }
#define LOG_TRACE(...)                      LOG_UNUSED(__VA_ARGS__)
#define LOG_TRACE_IF(condition, ...)        LOG_UNUSED(__VA_ARGS__)
#define LOG_TRACE_UNLESS(condition, ...)    LOG_UNUSED(__VA_ARGS__)
#endif

#define LOG_DEBUG(...)                      LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Debug, __VA_ARGS__)
#define LOG_DEBUG_IF(condition, ...)        if (condition)    LOG_DEBUG(__VA_ARGS__)
#define LOG_DEBUG_UNLESS(condition, ...)    if (!(condition)) LOG_DEBUG(__VA_ARGS__)

#define LOG_INFO(...)                       LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Info, __VA_ARGS__)
#define LOG_INFO_IF(condition, ...)         if (condition)    LOG_INFO(__VA_ARGS__)
#define LOG_INFO_UNLESS(condition, ...)     if (!(condition)) LOG_INFO(__VA_ARGS__)

#define LOG_WARNING(...)                    LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Warning, __VA_ARGS__)
#define LOG_WARNING_IF(condition, ...)      if (condition)    LOG_WARNING(__VA_ARGS__)
#define LOG_WARNING_UNLESS(condition, ...)  if (!(condition)) LOG_WARNING(__VA_ARGS__)

#define LOG_ERROR(...)                      LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Error, __VA_ARGS__)
#define LOG_ERROR_IF(condition, ...)        if (condition)    LOG_ERROR(__VA_ARGS__)
#define LOG_ERROR_UNLESS(condition, ...)    if (!(condition)) LOG_ERROR(__VA_ARGS__)

#define LOG_FATAL(...) \
    do { \
        LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Fatal, __VA_ARGS__); \
        BUILTIN_UNREACHABLE(); \
    } while(false)
#define LOG_FATAL_IF(condition, ...)        if (Y_UNLIKELY(condition)) LOG_FATAL(__VA_ARGS__)
#define LOG_FATAL_UNLESS(condition, ...)    if (!Y_LIKELY(condition)) LOG_FATAL(__VA_ARGS__)

#define LOG_EVENT(logger, level, ...) \
    do { \
        if (!logger.IsLevelEnabled(level)) { \
            break; \
        } \
        \
        static ::NYT::NLogging::TLoggingPosition position__##__LINE__; \
        bool positionUpToDate__##__LINE__ = logger.IsPositionUpToDate(position__##__LINE__); \
        if (positionUpToDate__##__LINE__ && !position__##__LINE__.Enabled) { \
            break; \
        } \
        \
        auto message__##__LINE__ = ::NYT::NLogging::NDetail::FormatLogMessage(__VA_ARGS__); \
        if (!positionUpToDate__##__LINE__) { \
            logger.UpdatePosition(&position__##__LINE__, message__##__LINE__); \
        } \
        \
        if (position__##__LINE__.Enabled) { \
            ::NYT::NLogging::NDetail::LogEventImpl( \
                logger, \
                level, \
                std::move(message__##__LINE__)); \
        } \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT

#define LOG_INL_H_
#include "log-inl.h"
#undef LOG_INL_H_

