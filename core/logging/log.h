#pragma once

#include "public.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/format.h>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/profiling/public.h>

#include <util/system/thread.h>

#include <atomic>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct TLoggingCategory
{
    const char* Name;
    std::atomic<ELogLevel> MinLevel;
    std::atomic<int> CurrentVersion;
    std::atomic<int>* ActualVersion;
};

////////////////////////////////////////////////////////////////////////////////

struct TLoggingPosition
{
    std::atomic<bool> Enabled;
    std::atomic<int> CurrentVersion;
};

////////////////////////////////////////////////////////////////////////////////

struct TLogEvent
{
    const TLoggingCategory* Category = nullptr;
    ELogLevel Level = ELogLevel::Minimum;
    ELogMessageFormat MessageFormat = ELogMessageFormat::PlainText;
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

    explicit operator bool() const;

    const TLoggingCategory* GetCategory() const;
    bool IsLevelEnabled(ELogLevel level) const;

    bool IsPositionUpToDate(const TLoggingPosition& position) const;
    void UpdatePosition(TLoggingPosition* position, const TString& message) const;

    void Write(TLogEvent&& event) const;

    TLogger& AddRawTag(const TString& tag);
    template <class... TArgs>
    TLogger& AddTag(const char* format, TArgs&&... args);
    const TString& GetContext() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    TLogManager* LogManager_;
    const TLoggingCategory* Category_;

    TString Context_;
};

////////////////////////////////////////////////////////////////////////////////

//! Typically serves as a virtual base for classes that need a member logger.
class TLoggerOwner
{
protected:
    TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

void LogStructuredEvent(const TLogger& logger,
    NYson::TYsonString message,
    ELogLevel level);

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_TRACE_LOGGING
#define YT_LOG_TRACE(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Trace, __VA_ARGS__)
#define YT_LOG_TRACE_IF(condition, ...)        if (condition)    YT_LOG_TRACE(__VA_ARGS__)
#define YT_LOG_TRACE_UNLESS(condition, ...)    if (!(condition)) YT_LOG_TRACE(__VA_ARGS__)
#else
#define YT_LOG_UNUSED(...)                     if (true) { } else { YT_LOG_DEBUG(__VA_ARGS__); }
#define YT_LOG_TRACE(...)                      YT_LOG_UNUSED(__VA_ARGS__)
#define YT_LOG_TRACE_IF(condition, ...)        YT_LOG_UNUSED(__VA_ARGS__)
#define YT_LOG_TRACE_UNLESS(condition, ...)    YT_LOG_UNUSED(__VA_ARGS__)
#endif

#define YT_LOG_DEBUG(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Debug, __VA_ARGS__)
#define YT_LOG_DEBUG_IF(condition, ...)        if (condition)    YT_LOG_DEBUG(__VA_ARGS__)
#define YT_LOG_DEBUG_UNLESS(condition, ...)    if (!(condition)) YT_LOG_DEBUG(__VA_ARGS__)

#define YT_LOG_INFO(...)                       YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Info, __VA_ARGS__)
#define YT_LOG_INFO_IF(condition, ...)         if (condition)    YT_LOG_INFO(__VA_ARGS__)
#define YT_LOG_INFO_UNLESS(condition, ...)     if (!(condition)) YT_LOG_INFO(__VA_ARGS__)

#define YT_LOG_WARNING(...)                    YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Warning, __VA_ARGS__)
#define YT_LOG_WARNING_IF(condition, ...)      if (condition)    YT_LOG_WARNING(__VA_ARGS__)
#define YT_LOG_WARNING_UNLESS(condition, ...)  if (!(condition)) YT_LOG_WARNING(__VA_ARGS__)

#define YT_LOG_ERROR(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Error, __VA_ARGS__)
#define YT_LOG_ERROR_IF(condition, ...)        if (condition)    YT_LOG_ERROR(__VA_ARGS__)
#define YT_LOG_ERROR_UNLESS(condition, ...)    if (!(condition)) YT_LOG_ERROR(__VA_ARGS__)

#define YT_LOG_FATAL(...) \
    do { \
        YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Fatal, __VA_ARGS__); \
        BUILTIN_UNREACHABLE(); \
    } while(false)
#define YT_LOG_FATAL_IF(condition, ...)        if (Y_UNLIKELY(condition)) YT_LOG_FATAL(__VA_ARGS__)
#define YT_LOG_FATAL_UNLESS(condition, ...)    if (!Y_LIKELY(condition)) YT_LOG_FATAL(__VA_ARGS__)

#define YT_LOG_EVENT(logger, level, ...) \
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
        auto message__##__LINE__ = ::NYT::NLogging::NDetail::BuildLogMessage(logger.GetContext(), __VA_ARGS__); \
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

} // namespace NYT::NLogging

#define LOG_INL_H_
#include "log-inl.h"
#undef LOG_INL_H_
